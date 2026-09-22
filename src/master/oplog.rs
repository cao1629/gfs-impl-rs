use std::collections::VecDeque;
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex as StdMutex};
use std::thread::JoinHandle;

use parking_lot::Mutex;
use prost::Message;
use tokio::sync::watch;
use tracing::{error, warn};

use crate::common::config::Config;
use crate::common::framing::{decode_records, encode_record};
use crate::master::checkpoint;
use crate::master::master_state::MasterState;
use crate::state::LogRecord;

const SEGMENT_PREFIX: &str = "oplog.";

pub trait LogSink: Send {
    fn open_segment(&mut self, number: u64) -> bool;
    fn write(&mut self, bytes: &[u8]) -> bool;
    fn sync(&mut self) -> bool;
    fn size(&self) -> u64;
}

pub struct LocalFileSink {
    dir: PathBuf,
    file: Option<File>,
    size: u64,
}

impl LocalFileSink {
    pub fn new(dir: impl Into<PathBuf>) -> LocalFileSink {
        LocalFileSink { dir: dir.into(), file: None, size: 0 }
    }
}

impl LogSink for LocalFileSink {
    fn open_segment(&mut self, number: u64) -> bool {
        if let Some(file) = self.file.take() {
            let _ = file.sync_all();
        }
        let path = segment_path(&self.dir, number);
        match OpenOptions::new().append(true).create(true).open(&path) {
            Ok(file) => {
                self.size = file.metadata().map(|m| m.len()).unwrap_or(0);
                self.file = Some(file);
                true
            }
            Err(e) => {
                error!("cannot open log segment {}: {e}", path.display());
                false
            }
        }
    }

    fn write(&mut self, bytes: &[u8]) -> bool {
        let Some(file) = self.file.as_mut() else { return false };
        if file.write_all(bytes).is_err() {
            return false;
        }
        self.size += bytes.len() as u64;
        true
    }

    fn sync(&mut self) -> bool {
        self.file.as_ref().is_some_and(|file| file.sync_all().is_ok())
    }

    fn size(&self) -> u64 {
        self.size
    }
}

impl Drop for LocalFileSink {
    fn drop(&mut self) {
        if let Some(file) = self.file.take() {
            let _ = file.sync_all();
        }
    }
}

#[derive(Debug, Default)]
pub struct ReplayedSegment {
    pub number: u64,
    pub records: Vec<LogRecord>,
    pub torn_tail: bool,
    pub valid_bytes: usize,
}

struct Queue {
    pending: VecDeque<(u64, Vec<u8>)>,
    next_seq: u64,
    stopping: bool,
}

pub struct OpLog {
    config: Config,
    state: Arc<Mutex<MasterState>>,
    sinks: StdMutex<Vec<Box<dyn LogSink>>>,
    checkpoint_dir: Mutex<Option<PathBuf>>,
    queue: StdMutex<Queue>,
    pending_cv: Condvar,
    flushed: watch::Sender<u64>,
    segment: AtomicU64,
    flusher: Mutex<Option<JoinHandle<()>>>,
    checkpoint_writer: Mutex<Option<JoinHandle<()>>>,
}

impl OpLog {
    pub fn new(config: Config, state: Arc<Mutex<MasterState>>) -> Arc<OpLog> {
        let (flushed, _) = watch::channel(0);
        Arc::new(OpLog {
            config,
            state,
            sinks: StdMutex::new(Vec::new()),
            checkpoint_dir: Mutex::new(None),
            queue: StdMutex::new(Queue { pending: VecDeque::new(), next_seq: 1, stopping: false }),
            pending_cv: Condvar::new(),
            flushed,
            segment: AtomicU64::new(0),
            flusher: Mutex::new(None),
            checkpoint_writer: Mutex::new(None),
        })
    }

    pub fn add_sink(&self, sink: Box<dyn LogSink>) {
        self.sinks.lock().unwrap().push(sink);
    }

    pub fn enable_checkpoints(&self, dir: impl Into<PathBuf>) {
        *self.checkpoint_dir.lock() = Some(dir.into());
    }

    pub fn open(self: &Arc<Self>, segment: u64) {
        self.segment.store(segment, Ordering::SeqCst);
        for sink in self.sinks.lock().unwrap().iter_mut() {
            sink.open_segment(segment);
        }
        let this = self.clone();
        *self.flusher.lock() = Some(std::thread::spawn(move || this.run()));
    }

    pub fn stop(&self) {
        {
            let mut queue = self.queue.lock().unwrap();
            if queue.stopping {
                return;
            }
            queue.stopping = true;
        }
        self.pending_cv.notify_all();
        if let Some(flusher) = self.flusher.lock().take() {
            let _ = flusher.join();
        }
        if let Some(writer) = self.checkpoint_writer.lock().take() {
            let _ = writer.join();
        }
        for sink in self.sinks.lock().unwrap().iter_mut() {
            sink.sync();
        }
        self.flushed.send_modify(|flushed| *flushed = u64::MAX);
    }

    pub fn append(&self, record: &LogRecord) -> u64 {
        let payload = record.encode_to_vec();
        let seq = {
            let mut queue = self.queue.lock().unwrap();
            let seq = queue.next_seq;
            queue.next_seq += 1;
            queue.pending.push_back((seq, encode_record(&payload)));
            seq
        };
        self.pending_cv.notify_one();
        seq
    }

    pub async fn wait_flushed(&self, seq: u64) {
        let mut flushed = self.flushed.subscribe();
        loop {
            if *flushed.borrow_and_update() >= seq {
                return;
            }
            if flushed.changed().await.is_err() {
                return;
            }
        }
    }

    pub fn segment(&self) -> u64 {
        self.segment.load(Ordering::SeqCst)
    }

    fn run(&self) {
        loop {
            let (batch, last_seq) = {
                let mut queue = self.queue.lock().unwrap();
                while !queue.stopping && queue.pending.is_empty() {
                    queue = self.pending_cv.wait(queue).unwrap();
                }
                if queue.pending.is_empty() && queue.stopping {
                    return;
                }
                let batch_size = self.config.log_flush_batch_size as usize;
                if queue.pending.len() < batch_size {
                    let (guard, _) = self
                        .pending_cv
                        .wait_timeout_while(queue, self.config.log_flush_max_delay, |q| !q.stopping && q.pending.len() < batch_size)
                        .unwrap();
                    queue = guard;
                }
                Self::drain(&mut queue)
            };
            self.write_batch(batch, last_seq);
            let needs_rotate = self.checkpoint_dir.lock().is_some()
                && self.sinks.lock().unwrap().first().is_some_and(|sink| sink.size() >= self.config.checkpoint_log_threshold);
            if needs_rotate {
                self.rotate();
            }
        }
    }

    fn drain(queue: &mut Queue) -> (Vec<Vec<u8>>, u64) {
        let mut batch = Vec::with_capacity(queue.pending.len());
        let mut last_seq = 0;
        while let Some((seq, bytes)) = queue.pending.pop_front() {
            batch.push(bytes);
            last_seq = seq;
        }
        (batch, last_seq)
    }

    fn write_batch(&self, batch: Vec<Vec<u8>>, last_seq: u64) {
        if batch.is_empty() {
            return;
        }
        let joined: Vec<u8> = batch.concat();
        for sink in self.sinks.lock().unwrap().iter_mut() {
            if !sink.write(&joined) || !sink.sync() {
                error!("log sink write failed");
            }
        }
        self.flushed.send_modify(|flushed| *flushed = (*flushed).max(last_seq));
    }

    fn rotate(&self) {
        let (checkpoint, new_segment) = {
            let state = self.state.lock();
            let (batch, last_seq) = {
                let mut queue = self.queue.lock().unwrap();
                Self::drain(&mut queue)
            };
            self.write_batch(batch, last_seq);
            let checkpoint = state.to_checkpoint();
            let new_segment = self.segment.load(Ordering::SeqCst) + 1;
            for sink in self.sinks.lock().unwrap().iter_mut() {
                sink.open_segment(new_segment);
            }
            self.segment.store(new_segment, Ordering::SeqCst);
            (checkpoint, new_segment)
        };
        if let Some(previous) = self.checkpoint_writer.lock().take() {
            let _ = previous.join();
        }
        let Some(dir) = self.checkpoint_dir.lock().clone() else { return };
        *self.checkpoint_writer.lock() = Some(std::thread::spawn(move || {
            if checkpoint::write(&dir, new_segment, &checkpoint) {
                checkpoint::prune(&dir, new_segment, 2);
            }
        }));
    }
}

pub fn segment_path(dir: &Path, number: u64) -> PathBuf {
    dir.join(checkpoint::numbered(SEGMENT_PREFIX, number))
}

pub fn list_segments(dir: &Path) -> Vec<u64> {
    let mut numbers: Vec<u64> = fs::read_dir(dir)
        .map(|entries| {
            entries.flatten().filter_map(|entry| checkpoint::parse_numbered(&entry.file_name().to_string_lossy(), SEGMENT_PREFIX)).collect()
        })
        .unwrap_or_default();
    numbers.sort_unstable();
    numbers
}

pub fn read_segment(dir: &Path, number: u64) -> ReplayedSegment {
    let bytes = fs::read(segment_path(dir, number)).unwrap_or_default();
    let decoded = decode_records(&bytes);
    let mut out = ReplayedSegment { number, torn_tail: decoded.torn_tail, valid_bytes: decoded.consumed, records: Vec::new() };
    for payload in decoded.payloads {
        match LogRecord::decode(payload.as_slice()) {
            Ok(record) => out.records.push(record),
            Err(_) => warn!("skipping an unparsable record in segment {number}"),
        }
    }
    out
}

pub fn truncate_segment(dir: &Path, number: u64, bytes: usize) {
    let path = segment_path(dir, number);
    match OpenOptions::new().write(true).open(&path) {
        Ok(file) => {
            if file.set_len(bytes as u64).is_err() {
                warn!("could not truncate {}", path.display());
            }
        }
        Err(_) => warn!("could not open {} for truncation", path.display()),
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;
    use crate::state::CreateRecord;
    use crate::state::log_record::Body;

    fn create_record(path: &str) -> LogRecord {
        LogRecord { body: Some(Body::Create(CreateRecord { path: path.to_string() })) }
    }

    #[tokio::test]
    async fn appends_flushes_and_replays() {
        let dir = tempfile::tempdir().unwrap();
        let config = Config { log_flush_max_delay: Duration::from_millis(2), ..Config::default() };
        let state = Arc::new(Mutex::new(MasterState::default()));
        {
            let log = OpLog::new(config, state);
            log.add_sink(Box::new(LocalFileSink::new(dir.path())));
            log.open(1);
            let mut seq = 0;
            for i in 0..5 {
                seq = log.append(&create_record(&format!("/f{i}")));
            }
            log.wait_flushed(seq).await;
            log.stop();
        }
        let segment = read_segment(dir.path(), 1);
        assert_eq!(segment.records.len(), 5);
        assert!(matches!(&segment.records[4].body, Some(Body::Create(r)) if r.path == "/f4"));
        assert!(!segment.torn_tail);

        let mut junk = fs::read(segment_path(dir.path(), 1)).unwrap();
        junk.extend_from_slice(b"\x09\x00\x00\x00junk");
        fs::write(segment_path(dir.path(), 1), &junk).unwrap();
        let torn = read_segment(dir.path(), 1);
        assert_eq!(torn.records.len(), 5);
        assert!(torn.torn_tail);
        truncate_segment(dir.path(), 1, torn.valid_bytes);
        assert!(!read_segment(dir.path(), 1).torn_tail);
        assert_eq!(list_segments(dir.path()), vec![1]);
    }

    #[tokio::test]
    async fn rotates_into_checkpoints_that_recovery_loads() {
        let dir = tempfile::tempdir().unwrap();
        let config = Config { log_flush_max_delay: Duration::from_millis(2), checkpoint_log_threshold: 200, ..Config::default() };
        let state = Arc::new(Mutex::new(MasterState::default()));
        {
            let log = OpLog::new(config, state.clone());
            log.add_sink(Box::new(LocalFileSink::new(dir.path())));
            log.enable_checkpoints(dir.path());
            log.open(1);
            for i in 0..40 {
                let seq = {
                    let mut state = state.lock();
                    let path = format!("/file{i}");
                    state.apply_create(&path);
                    log.append(&create_record(&path))
                };
                log.wait_flushed(seq).await;
            }
            assert!(log.segment() > 1);
            log.stop();
        }
        let checkpoints = checkpoint::list(dir.path());
        assert!(!checkpoints.is_empty());
        assert!(checkpoints.len() <= 2);
        fs::write(dir.path().join("checkpoint.000999.tmp"), b"half written").unwrap();
        let (number, loaded) = checkpoint::load_latest(dir.path()).unwrap();
        assert_eq!(number, *checkpoints.last().unwrap());
        let mut recovered = MasterState::default();
        recovered.load(&loaded);
        for segment in list_segments(dir.path()) {
            assert!(segment >= number);
            for record in read_segment(dir.path(), segment).records {
                recovered.apply(&record);
            }
        }
        assert_eq!(recovered.files.len(), 40);
        assert!(recovered.files.exists("/file39"));
    }
}

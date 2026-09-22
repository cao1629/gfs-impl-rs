use std::collections::{HashMap, HashSet};
use std::fs::{self, File, OpenOptions};
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use parking_lot::Mutex;
use tokio::sync::Mutex as AsyncMutex;
use tracing::{error, info, warn};

use crate::common::ids::{handle_to_hex, hex_to_handle, random_hex_id};
use crate::rpc::ResultCode;

const META_HEADER_SIZE: u64 = 8;
const COPY_BUFFER: usize = 1 << 20;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ChunkListing {
    pub handle: u64,
    pub version: u64,
    pub length: u64,
}

struct EntryState {
    chunk: File,
    meta: File,
    version: u64,
    length: u64,
    checksums: Vec<u32>,
}

pub struct ChunkEntry {
    mutation: Arc<AsyncMutex<()>>,
    state: Mutex<EntryState>,
}

pub struct ChunkStore {
    data_dir: PathBuf,
    chunk_size: u64,
    block_size: u64,
    entries: Mutex<HashMap<u64, Arc<ChunkEntry>>>,
    corrupt: Mutex<HashSet<u64>>,
}

fn blocks_needed(length: u64, block_size: u64) -> usize {
    length.div_ceil(block_size) as usize
}

fn write_version(meta: &File, version: u64) -> bool {
    meta.write_all_at(&version.to_le_bytes(), 0).is_ok() && meta.sync_all().is_ok()
}

fn encode_checksums(checksums: &[u32]) -> Vec<u8> {
    checksums.iter().flat_map(|c| c.to_le_bytes()).collect()
}

impl ChunkStore {
    pub fn new(data_dir: impl Into<PathBuf>, chunk_size: u64, checksum_block_size: u64) -> ChunkStore {
        let data_dir = data_dir.into();
        let _ = fs::create_dir_all(&data_dir);
        ChunkStore {
            data_dir,
            chunk_size,
            block_size: checksum_block_size,
            entries: Mutex::new(HashMap::new()),
            corrupt: Mutex::new(HashSet::new()),
        }
    }

    pub fn chunk_size(&self) -> u64 {
        self.chunk_size
    }

    pub fn checksum_block_size(&self) -> u64 {
        self.block_size
    }

    pub fn chunk_path(&self, handle: u64) -> PathBuf {
        self.data_dir.join(format!("{}.chunk", handle_to_hex(handle)))
    }

    pub fn meta_path(&self, handle: u64) -> PathBuf {
        self.data_dir.join(format!("{}.meta", handle_to_hex(handle)))
    }

    fn find(&self, handle: u64) -> Option<Arc<ChunkEntry>> {
        self.entries.lock().get(&handle).cloned()
    }

    fn open_entry(&self, handle: u64, create_new: bool, version: u64) -> Option<ChunkEntry> {
        let mut options = OpenOptions::new();
        options.read(true).write(true);
        if create_new {
            options.create_new(true);
        }
        let chunk = options.open(self.chunk_path(handle)).ok()?;
        let meta = match options.open(self.meta_path(handle)) {
            Ok(meta) => meta,
            Err(_) => {
                if create_new {
                    let _ = fs::remove_file(self.chunk_path(handle));
                }
                return None;
            }
        };
        if create_new {
            if !write_version(&meta, version) {
                return None;
            }
            return Some(ChunkEntry {
                mutation: Arc::new(AsyncMutex::new(())),
                state: Mutex::new(EntryState { chunk, meta, version, length: 0, checksums: Vec::new() }),
            });
        }
        let meta_size = meta.metadata().ok()?.len();
        if meta_size < META_HEADER_SIZE {
            return None;
        }
        let mut header = [0u8; 8];
        meta.read_exact_at(&mut header, 0).ok()?;
        let version = u64::from_le_bytes(header);
        let length = chunk.metadata().ok()?.len();
        let stored = ((meta_size - META_HEADER_SIZE) / 4) as usize;
        let loaded = stored.min(blocks_needed(length, self.block_size));
        let mut raw = vec![0u8; loaded * 4];
        if loaded > 0 {
            meta.read_exact_at(&mut raw, META_HEADER_SIZE).ok()?;
        }
        let checksums = raw.chunks_exact(4).map(|c| u32::from_le_bytes(c.try_into().unwrap())).collect();
        Some(ChunkEntry {
            mutation: Arc::new(AsyncMutex::new(())),
            state: Mutex::new(EntryState { chunk, meta, version, length, checksums }),
        })
    }

    pub fn scan(&self) {
        let mut chunks = Vec::new();
        let mut metas = Vec::new();
        if let Ok(items) = fs::read_dir(&self.data_dir) {
            for item in items.flatten() {
                if !item.file_type().map(|t| t.is_file()).unwrap_or(false) {
                    continue;
                }
                let name = item.file_name().to_string_lossy().to_string();
                let Some((stem, ext)) = name.rsplit_once('.') else { continue };
                let Some(handle) = hex_to_handle(stem) else { continue };
                match ext {
                    "chunk" => chunks.push(handle),
                    "meta" => metas.push(handle),
                    _ => {}
                }
            }
        }
        chunks.sort_unstable();
        metas.sort_unstable();
        for &handle in &metas {
            if chunks.binary_search(&handle).is_err() {
                warn!("removing orphan meta for chunk {}", handle_to_hex(handle));
                let _ = fs::remove_file(self.meta_path(handle));
            }
        }
        let mut entries = self.entries.lock();
        for &handle in &chunks {
            if metas.binary_search(&handle).is_err() {
                warn!("chunk {} has no meta file, ignoring it", handle_to_hex(handle));
                continue;
            }
            let Some(entry) = self.open_entry(handle, false, 0) else {
                warn!("chunk {} could not be opened, ignoring it", handle_to_hex(handle));
                continue;
            };
            {
                let state = entry.state.lock();
                if state.checksums.len() < blocks_needed(state.length, self.block_size) {
                    warn!("chunk {} is missing checksums for its tail, those blocks read as corrupt", handle_to_hex(handle));
                }
            }
            entries.insert(handle, Arc::new(entry));
        }
        info!("scanned {} chunks in {}", entries.len(), self.data_dir.display());
    }

    pub fn create(&self, handle: u64, version: u64) -> ResultCode {
        let mut entries = self.entries.lock();
        if entries.contains_key(&handle) {
            return ResultCode::Failed;
        }
        let Some(entry) = self.open_entry(handle, true, version) else { return ResultCode::Failed };
        entries.insert(handle, Arc::new(entry));
        ResultCode::Ok
    }

    pub fn create_copy(&self, handle: u64, version: u64, copy_from: u64) -> ResultCode {
        let Some(source) = self.find(copy_from) else { return ResultCode::NoSuchChunk };
        let mut entries = self.entries.lock();
        if entries.contains_key(&handle) {
            return ResultCode::Failed;
        }
        let Some(entry) = self.open_entry(handle, true, version) else { return ResultCode::Failed };
        {
            let source_state = source.state.lock();
            let mut state = entry.state.lock();
            let mut buf = vec![0u8; COPY_BUFFER];
            let mut done = 0u64;
            while done < source_state.length {
                let step = (buf.len() as u64).min(source_state.length - done) as usize;
                if source_state.chunk.read_exact_at(&mut buf[..step], done).is_err()
                    || state.chunk.write_all_at(&buf[..step], done).is_err()
                {
                    return ResultCode::Failed;
                }
                done += step as u64;
            }
            state.length = source_state.length;
            state.checksums = source_state.checksums.clone();
            let raw = encode_checksums(&state.checksums);
            if !raw.is_empty() && state.meta.write_all_at(&raw, META_HEADER_SIZE).is_err() {
                return ResultCode::Failed;
            }
            if state.chunk.sync_all().is_err() || state.meta.sync_all().is_err() {
                return ResultCode::Failed;
            }
        }
        entries.insert(handle, Arc::new(entry));
        ResultCode::Ok
    }

    fn recompute_blocks(&self, state: &mut EntryState, first_block: u64, last_block: u64) -> bool {
        let needed = blocks_needed(state.length, self.block_size);
        if state.checksums.len() < needed {
            state.checksums.resize(needed, 0);
        }
        if needed == 0 {
            return true;
        }
        let last_block = last_block.min(needed as u64 - 1);
        if first_block > last_block {
            return true;
        }
        let start = first_block * self.block_size;
        let end = ((last_block + 1) * self.block_size).min(state.length);
        let mut buf = vec![0u8; (end - start) as usize];
        if state.chunk.read_exact_at(&mut buf, start).is_err() {
            return false;
        }
        let mut raw = Vec::with_capacity(((last_block - first_block + 1) * 4) as usize);
        for block in first_block..=last_block {
            let block_start = (block * self.block_size - start) as usize;
            let block_end = (block_start + self.block_size as usize).min(buf.len());
            let crc = crc32fast::hash(&buf[block_start..block_end]);
            state.checksums[block as usize] = crc;
            raw.extend_from_slice(&crc.to_le_bytes());
        }
        state.meta.write_all_at(&raw, META_HEADER_SIZE + 4 * first_block).is_ok()
    }

    pub fn read(&self, handle: u64, offset: u64, length: u64) -> Result<Vec<u8>, ResultCode> {
        let Some(entry) = self.find(handle) else { return Err(ResultCode::NoSuchChunk) };
        let state = entry.state.lock();
        if offset > state.length {
            return Err(ResultCode::OutOfRange);
        }
        let end = offset.saturating_add(length).min(state.length);
        if end == offset {
            return Ok(Vec::new());
        }
        let first = offset / self.block_size;
        let last = (end - 1) / self.block_size;
        if last as usize >= state.checksums.len() {
            self.mark_corrupt(handle);
            return Err(ResultCode::ChecksumMismatch);
        }
        let start = first * self.block_size;
        let stop = ((last + 1) * self.block_size).min(state.length);
        let mut buf = vec![0u8; (stop - start) as usize];
        if state.chunk.read_exact_at(&mut buf, start).is_err() {
            return Err(ResultCode::Failed);
        }
        for block in first..=last {
            let block_start = (block * self.block_size - start) as usize;
            let block_end = (block_start + self.block_size as usize).min(buf.len());
            if crc32fast::hash(&buf[block_start..block_end]) != state.checksums[block as usize] {
                self.mark_corrupt(handle);
                return Err(ResultCode::ChecksumMismatch);
            }
        }
        Ok(buf[(offset - start) as usize..(end - start) as usize].to_vec())
    }

    pub fn write(&self, handle: u64, offset: u64, data: &[u8]) -> ResultCode {
        let Some(entry) = self.find(handle) else { return ResultCode::NoSuchChunk };
        if offset.saturating_add(data.len() as u64) > self.chunk_size {
            return ResultCode::OutOfRange;
        }
        if data.is_empty() {
            return ResultCode::Ok;
        }
        let mut state = entry.state.lock();
        if state.chunk.write_all_at(data, offset).is_err() {
            return ResultCode::Failed;
        }
        let old_length = state.length;
        state.length = old_length.max(offset + data.len() as u64);
        let first = offset.min(old_length) / self.block_size;
        let last = (state.length - 1) / self.block_size;
        if !self.recompute_blocks(&mut state, first, last) {
            return ResultCode::Failed;
        }
        if state.chunk.sync_all().is_err() || state.meta.sync_all().is_err() {
            return ResultCode::Failed;
        }
        ResultCode::Ok
    }

    pub fn pad(&self, handle: u64, from_offset: u64) -> ResultCode {
        let Some(entry) = self.find(handle) else { return ResultCode::NoSuchChunk };
        if from_offset > self.chunk_size {
            return ResultCode::OutOfRange;
        }
        let mut state = entry.state.lock();
        let old_length = state.length;
        if from_offset >= old_length {
            if state.chunk.set_len(self.chunk_size).is_err() {
                return ResultCode::Failed;
            }
        } else {
            let zeros = vec![0u8; COPY_BUFFER];
            let mut pos = from_offset;
            while pos < self.chunk_size {
                let step = (zeros.len() as u64).min(self.chunk_size - pos) as usize;
                if state.chunk.write_all_at(&zeros[..step], pos).is_err() {
                    return ResultCode::Failed;
                }
                pos += step as u64;
            }
        }
        state.length = self.chunk_size;
        let first = from_offset.min(old_length) / self.block_size;
        let last = (self.chunk_size - 1) / self.block_size;
        if !self.recompute_blocks(&mut state, first, last) {
            return ResultCode::Failed;
        }
        if state.chunk.sync_all().is_err() || state.meta.sync_all().is_err() {
            return ResultCode::Failed;
        }
        ResultCode::Ok
    }

    pub fn length(&self, handle: u64) -> Result<u64, ResultCode> {
        let Some(entry) = self.find(handle) else { return Err(ResultCode::NoSuchChunk) };
        let length = entry.state.lock().length;
        Ok(length)
    }

    pub fn version(&self, handle: u64) -> Option<u64> {
        let entry = self.find(handle)?;
        let version = entry.state.lock().version;
        Some(version)
    }

    pub fn set_version(&self, handle: u64, version: u64) -> ResultCode {
        let Some(entry) = self.find(handle) else { return ResultCode::NoSuchChunk };
        let mut state = entry.state.lock();
        if state.version == version {
            return ResultCode::Ok;
        }
        if !write_version(&state.meta, version) {
            return ResultCode::Failed;
        }
        state.version = version;
        ResultCode::Ok
    }

    pub fn contains(&self, handle: u64) -> bool {
        self.entries.lock().contains_key(&handle)
    }

    pub fn remove(&self, handle: u64) -> bool {
        let removed = self.entries.lock().remove(&handle);
        if removed.is_none() {
            return false;
        }
        let _ = fs::remove_file(self.chunk_path(handle));
        let _ = fs::remove_file(self.meta_path(handle));
        self.corrupt.lock().remove(&handle);
        true
    }

    pub fn list(&self) -> Vec<ChunkListing> {
        let snapshot: Vec<(u64, Arc<ChunkEntry>)> = self.entries.lock().iter().map(|(h, e)| (*h, e.clone())).collect();
        let mut out: Vec<ChunkListing> = snapshot
            .into_iter()
            .map(|(handle, entry)| {
                let state = entry.state.lock();
                ChunkListing { handle, version: state.version, length: state.length }
            })
            .collect();
        out.sort_by_key(|listing| listing.handle);
        out
    }

    pub fn mutation_lock(&self, handle: u64) -> Option<Arc<AsyncMutex<()>>> {
        self.find(handle).map(|entry| entry.mutation.clone())
    }

    fn mark_corrupt(&self, handle: u64) {
        if self.corrupt.lock().insert(handle) {
            error!("checksum mismatch on chunk {}", handle_to_hex(handle));
        }
    }

    pub fn corrupt_handles(&self) -> Vec<u64> {
        let mut out: Vec<u64> = self.corrupt.lock().iter().copied().collect();
        out.sort_unstable();
        out
    }

    pub fn clear_corrupt(&self, handles: &[u64]) {
        let mut corrupt = self.corrupt.lock();
        for handle in handles {
            corrupt.remove(handle);
        }
    }
}

pub fn load_or_create_chunkserver_id(data_dir: &Path) -> String {
    let _ = fs::create_dir_all(data_dir);
    let path = data_dir.join("chunkserver_id");
    if let Ok(text) = fs::read_to_string(&path) {
        let id = text.lines().next().unwrap_or("").trim().to_string();
        if !id.is_empty() {
            return id;
        }
    }
    let id = random_hex_id(16);
    let _ = fs::write(&path, format!("{id}\n"));
    id
}

#[cfg(test)]
mod tests {
    use super::*;

    const CHUNK: u64 = 1 << 20;
    const BLOCK: u64 = 64 << 10;

    fn pattern(n: usize, base: u8) -> Vec<u8> {
        (0..n).map(|i| base + (i % 23) as u8).collect()
    }

    fn flip_byte(path: &Path, offset: u64) {
        let file = OpenOptions::new().read(true).write(true).open(path).unwrap();
        let mut byte = [0u8; 1];
        file.read_exact_at(&mut byte, offset).unwrap();
        byte[0] ^= 0x5a;
        file.write_all_at(&byte, offset).unwrap();
    }

    fn meta_entry(meta: &[u8], index: usize) -> u32 {
        u32::from_le_bytes(meta[8 + 4 * index..12 + 4 * index].try_into().unwrap())
    }

    fn meta_version(meta: &[u8]) -> u64 {
        u64::from_le_bytes(meta[..8].try_into().unwrap())
    }

    #[test]
    fn create_write_read_pad_length() {
        let dir = tempfile::tempdir().unwrap();
        let store = ChunkStore::new(dir.path(), CHUNK, BLOCK);
        assert_eq!(store.create(7, 3), ResultCode::Ok);
        assert_eq!(store.create(7, 3), ResultCode::Failed);
        assert!(store.contains(7));
        assert_eq!(store.version(7), Some(3));
        let data = pattern(100000, b'a');
        assert_eq!(store.write(7, 0, &data), ResultCode::Ok);
        assert_eq!(store.length(7), Ok(100000));
        assert_eq!(store.read(7, 0, 100000).unwrap(), data);
        assert_eq!(store.read(7, 99990, 1000).unwrap(), data[99990..]);
        assert!(store.read(7, 100000, 10).unwrap().is_empty());
        assert_eq!(store.read(7, 100001, 10), Err(ResultCode::OutOfRange));
        assert_eq!(store.write(7, CHUNK - 5, b"0123456789"), ResultCode::OutOfRange);
        assert_eq!(store.read(99, 0, 1), Err(ResultCode::NoSuchChunk));

        assert_eq!(store.pad(7, 100000), ResultCode::Ok);
        assert_eq!(store.length(7), Ok(CHUNK));
        let mut expected = data[99998..].to_vec();
        expected.extend_from_slice(&[0, 0, 0, 0]);
        assert_eq!(store.read(7, 99998, 6).unwrap(), expected);
        assert_eq!(store.read(7, CHUNK - 3, 100).unwrap(), vec![0u8; 3]);

        let listing = store.list();
        assert_eq!(listing, vec![ChunkListing { handle: 7, version: 3, length: CHUNK }]);

        assert_eq!(store.set_version(7, 9), ResultCode::Ok);
        assert_eq!(store.version(7), Some(9));
        assert!(store.remove(7));
        assert!(!store.contains(7));
        assert!(!store.chunk_path(7).exists());
        assert!(!store.meta_path(7).exists());
    }

    #[test]
    fn meta_layout_matches_the_fixed_format() {
        let dir = tempfile::tempdir().unwrap();
        let store = ChunkStore::new(dir.path(), CHUNK, BLOCK);
        assert_eq!(store.create(1, 5), ResultCode::Ok);
        let data = pattern((BLOCK * 2 + 100) as usize, b'q');
        assert_eq!(store.write(1, 0, &data), ResultCode::Ok);
        let meta = fs::read(store.meta_path(1)).unwrap();
        assert_eq!(meta.len(), 8 + 4 * 3);
        assert_eq!(meta_version(&meta), 5);
        assert_eq!(meta_entry(&meta, 0), crc32fast::hash(&data[..BLOCK as usize]));
        assert_eq!(meta_entry(&meta, 1), crc32fast::hash(&data[BLOCK as usize..2 * BLOCK as usize]));
        assert_eq!(meta_entry(&meta, 2), crc32fast::hash(&data[2 * BLOCK as usize..]));

        assert_eq!(store.set_version(1, 6), ResultCode::Ok);
        let meta = fs::read(store.meta_path(1)).unwrap();
        assert_eq!(meta_version(&meta), 6);
        assert_eq!(meta.len(), 8 + 4 * 3);
    }

    #[test]
    fn overlapping_write_recomputes_only_touched_blocks() {
        let dir = tempfile::tempdir().unwrap();
        let store = ChunkStore::new(dir.path(), CHUNK, BLOCK);
        assert_eq!(store.create(2, 1), ResultCode::Ok);
        let mut data = pattern((BLOCK * 4) as usize, b'a');
        assert_eq!(store.write(2, 0, &data), ResultCode::Ok);
        let patch = pattern(1000, b'z');
        let at = (BLOCK * 2 - 500) as usize;
        assert_eq!(store.write(2, at as u64, &patch), ResultCode::Ok);
        data[at..at + 1000].copy_from_slice(&patch);
        let meta = fs::read(store.meta_path(2)).unwrap();
        for b in 0..4usize {
            assert_eq!(meta_entry(&meta, b), crc32fast::hash(&data[b * BLOCK as usize..(b + 1) * BLOCK as usize]), "block {b}");
        }
        assert_eq!(store.read(2, 0, data.len() as u64).unwrap(), data);
    }

    #[test]
    fn write_beyond_length_zero_fills_the_gap() {
        let dir = tempfile::tempdir().unwrap();
        let store = ChunkStore::new(dir.path(), CHUNK, BLOCK);
        assert_eq!(store.create(3, 1), ResultCode::Ok);
        assert_eq!(store.write(3, 0, b"head"), ResultCode::Ok);
        assert_eq!(store.write(3, BLOCK + 10, b"tail"), ResultCode::Ok);
        let back = store.read(3, 0, BLOCK + 14).unwrap();
        assert_eq!(&back[..4], b"head");
        assert_eq!(back[4..(BLOCK + 10) as usize], vec![0u8; (BLOCK + 6) as usize]);
        assert_eq!(&back[(BLOCK + 10) as usize..], b"tail");
    }

    #[test]
    fn corruption_of_the_chunk_file_is_detected_on_read() {
        let dir = tempfile::tempdir().unwrap();
        let store = ChunkStore::new(dir.path(), CHUNK, BLOCK);
        assert_eq!(store.create(4, 1), ResultCode::Ok);
        let data = pattern((BLOCK * 3) as usize, b'm');
        assert_eq!(store.write(4, 0, &data), ResultCode::Ok);
        flip_byte(&store.chunk_path(4), BLOCK + 17);
        assert!(store.read(4, 0, BLOCK).is_ok());
        assert!(store.read(4, BLOCK * 2, BLOCK).is_ok());
        assert_eq!(store.read(4, BLOCK - 1, 2), Err(ResultCode::ChecksumMismatch));
        assert_eq!(store.corrupt_handles(), vec![4]);
        store.clear_corrupt(&[4]);
        assert!(store.corrupt_handles().is_empty());
    }

    #[test]
    fn copy_duplicates_data_and_checksums() {
        let dir = tempfile::tempdir().unwrap();
        let store = ChunkStore::new(dir.path(), CHUNK, BLOCK);
        assert_eq!(store.create(5, 2), ResultCode::Ok);
        let data = pattern((BLOCK + 333) as usize, b'c');
        assert_eq!(store.write(5, 0, &data), ResultCode::Ok);
        assert_eq!(store.create_copy(6, 3, 42), ResultCode::NoSuchChunk);
        assert_eq!(store.create_copy(6, 3, 5), ResultCode::Ok);
        assert_eq!(store.version(6), Some(3));
        assert_eq!(store.read(6, 0, data.len() as u64).unwrap(), data);
        assert_eq!(fs::read(store.meta_path(6)).unwrap()[8..], fs::read(store.meta_path(5)).unwrap()[8..]);
        assert_eq!(store.write(6, 0, b"changed"), ResultCode::Ok);
        assert_eq!(store.read(5, 0, 7).unwrap(), data[..7]);
    }

    #[test]
    fn scan_reloads_and_repairs() {
        let dir = tempfile::tempdir().unwrap();
        let data = pattern((BLOCK * 2 + 5) as usize, b'r');
        {
            let store = ChunkStore::new(dir.path(), CHUNK, BLOCK);
            for (handle, version) in [(10, 4), (11, 1), (12, 1)] {
                assert_eq!(store.create(handle, version), ResultCode::Ok);
                assert_eq!(store.write(handle, 0, &data), ResultCode::Ok);
            }
        }
        fs::remove_file(dir.path().join(format!("{}.meta", handle_to_hex(11)))).unwrap();
        fs::remove_file(dir.path().join(format!("{}.chunk", handle_to_hex(12)))).unwrap();
        OpenOptions::new().write(true).open(dir.path().join(format!("{}.meta", handle_to_hex(10)))).unwrap().set_len(8 + 4 * 2).unwrap();
        fs::write(dir.path().join("notachunk.txt"), b"ignored").unwrap();

        let store = ChunkStore::new(dir.path(), CHUNK, BLOCK);
        store.scan();
        assert!(store.contains(10));
        assert!(!store.contains(11));
        assert!(!store.contains(12));
        assert!(!dir.path().join(format!("{}.meta", handle_to_hex(12))).exists());
        assert!(dir.path().join(format!("{}.chunk", handle_to_hex(11))).exists());
        assert_eq!(store.version(10), Some(4));
        assert_eq!(store.length(10), Ok(data.len() as u64));
        assert_eq!(store.read(10, 0, BLOCK * 2).unwrap(), data[..(BLOCK * 2) as usize]);
        assert_eq!(store.read(10, BLOCK * 2, 5), Err(ResultCode::ChecksumMismatch));
        assert_eq!(store.write(10, BLOCK * 2, b"fixed"), ResultCode::Ok);
        assert_eq!(store.read(10, BLOCK * 2, 5).unwrap(), b"fixed");
    }

    #[test]
    fn chunkserver_id_is_stable() {
        let dir = tempfile::tempdir().unwrap();
        let first = load_or_create_chunkserver_id(dir.path());
        assert_eq!(first.len(), 32);
        assert_eq!(load_or_create_chunkserver_id(dir.path()), first);
    }
}

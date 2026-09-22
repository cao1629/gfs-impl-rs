use std::fs::{self, File};
use std::io::Write;
use std::path::{Path, PathBuf};

use prost::Message;
use tracing::{error, info, warn};

use crate::common::framing::{decode_records, encode_record};
use crate::master::oplog;
use crate::state::Checkpoint;

const PREFIX: &str = "checkpoint.";

pub fn numbered(prefix: &str, number: u64) -> String {
    format!("{prefix}{number:06}")
}

pub fn parse_numbered(name: &str, prefix: &str) -> Option<u64> {
    let rest = name.strip_prefix(prefix)?;
    if rest.len() != 6 || !rest.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    rest.parse().ok()
}

pub fn path(dir: &Path, number: u64) -> PathBuf {
    dir.join(numbered(PREFIX, number))
}

fn write_whole_file(path: &Path, bytes: &[u8]) -> bool {
    let Ok(mut file) = File::create(path) else { return false };
    file.write_all(bytes).is_ok() && file.sync_all().is_ok()
}

pub fn write(dir: &Path, number: u64, checkpoint: &Checkpoint) -> bool {
    let payload = checkpoint.encode_to_vec();
    let final_path = path(dir, number);
    let mut tmp = final_path.clone().into_os_string();
    tmp.push(".tmp");
    let tmp_path = PathBuf::from(tmp);
    if !write_whole_file(&tmp_path, &encode_record(&payload)) {
        error!("failed to write {}", tmp_path.display());
        return false;
    }
    if let Err(e) = fs::rename(&tmp_path, &final_path) {
        error!("failed to rename {}: {e}", tmp_path.display());
        return false;
    }
    if let Ok(directory) = File::open(dir) {
        let _ = directory.sync_all();
    }
    info!("wrote {} with {} files and {} chunks", final_path.display(), checkpoint.files.len(), checkpoint.chunks.len());
    true
}

pub fn list(dir: &Path) -> Vec<u64> {
    let mut numbers: Vec<u64> = fs::read_dir(dir)
        .map(|entries| entries.flatten().filter_map(|entry| parse_numbered(&entry.file_name().to_string_lossy(), PREFIX)).collect())
        .unwrap_or_default();
    numbers.sort_unstable();
    numbers
}

pub fn load_latest(dir: &Path) -> Option<(u64, Checkpoint)> {
    for number in list(dir).into_iter().rev() {
        let bytes = fs::read(path(dir, number)).unwrap_or_default();
        let decoded = decode_records(&bytes);
        if decoded.payloads.len() != 1 || decoded.torn_tail {
            warn!("ignoring damaged checkpoint {number}");
            continue;
        }
        match Checkpoint::decode(decoded.payloads[0].as_slice()) {
            Ok(checkpoint) => return Some((number, checkpoint)),
            Err(_) => warn!("ignoring unparsable checkpoint {number}"),
        }
    }
    None
}

pub fn prune(dir: &Path, newest: u64, keep: usize) {
    for segment in oplog::list_segments(dir) {
        if segment < newest {
            let _ = fs::remove_file(oplog::segment_path(dir, segment));
        }
    }
    let numbers = list(dir);
    if numbers.len() > keep {
        for number in &numbers[..numbers.len() - keep] {
            let _ = fs::remove_file(path(dir, *number));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::FileEntry;

    #[test]
    fn ignores_damaged_files() {
        let dir = tempfile::tempdir().unwrap();
        let good = Checkpoint {
            next_chunk_handle: 77,
            files: vec![FileEntry { path: "/keep".to_string(), chunk_handles: vec![] }],
            chunks: vec![],
        };
        assert!(write(dir.path(), 3, &good));
        fs::write(path(dir.path(), 4), b"not a checkpoint").unwrap();
        fs::write(dir.path().join("checkpoint.000999.tmp"), b"half written").unwrap();
        let (number, loaded) = load_latest(dir.path()).unwrap();
        assert_eq!(number, 3);
        assert_eq!(loaded.next_chunk_handle, 77);
        assert_eq!(loaded.files[0].path, "/keep");
        assert_eq!(list(dir.path()), vec![3, 4]);
    }
}

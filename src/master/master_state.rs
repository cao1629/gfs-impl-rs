use tracing::warn;

use crate::common::paths::child_prefix;
use crate::master::chunk_table::ChunkTable;
use crate::master::namespace::{FileMeta, Namespace};
use crate::state::log_record::Body;
use crate::state::{Checkpoint, ChunkEntry, FileEntry, LogRecord};

#[derive(Debug)]
pub struct MasterState {
    pub files: Namespace,
    pub chunks: ChunkTable,
    pub next_handle: u64,
}

impl Default for MasterState {
    fn default() -> Self {
        MasterState { files: Namespace::default(), chunks: ChunkTable::default(), next_handle: 1 }
    }
}

impl MasterState {
    pub fn apply_create(&mut self, path: &str) {
        self.files.insert(path, FileMeta::default());
    }

    pub fn apply_rename(&mut self, source: &str, target: &str) {
        if !self.files.exists(source) && !self.files.is_directory(source) {
            return;
        }
        self.files.rename_subtree(source, target);
    }

    pub fn apply_remove(&mut self, path: &str) {
        let Some(meta) = self.files.find(path).cloned() else { return };
        for handle in meta.chunks {
            if let Some(chunk) = self.chunks.find_mut(handle)
                && chunk.refcount > 0
            {
                chunk.refcount -= 1;
            }
        }
        self.files.erase(path);
    }

    pub fn apply_alloc_handle(&mut self, handle: u64) {
        self.next_handle = self.next_handle.max(handle + 1);
    }

    pub fn apply_add_chunk(&mut self, path: &str, index: u64, handle: u64) {
        let Some(meta) = self.files.find(path) else { return };
        let count = meta.chunks.len() as u64;
        if index < count {
            if meta.chunks[index as usize] == handle {
                return;
            }
            self.apply_replace_chunk(path, index, handle);
            return;
        }
        if index != count {
            return;
        }
        if let Some(meta) = self.files.find_mut(path) {
            meta.chunks.push(handle);
        }
        let chunk = self.chunks.create(handle, 1);
        chunk.pending = false;
        chunk.refcount += 1;
        self.apply_alloc_handle(handle);
    }

    pub fn apply_replace_chunk(&mut self, path: &str, index: u64, handle: u64) {
        let old = match self.files.find(path) {
            Some(meta) if (index as usize) < meta.chunks.len() => meta.chunks[index as usize],
            _ => return,
        };
        if old == handle {
            return;
        }
        if let Some(previous) = self.chunks.find_mut(old)
            && previous.refcount > 0
        {
            previous.refcount -= 1;
        }
        if let Some(meta) = self.files.find_mut(path) {
            meta.chunks[index as usize] = handle;
        }
        let chunk = self.chunks.create(handle, 1);
        chunk.pending = false;
        chunk.refcount += 1;
        self.apply_alloc_handle(handle);
    }

    pub fn apply_bump_version(&mut self, handle: u64, version: u64) {
        if let Some(chunk) = self.chunks.find_mut(handle) {
            chunk.version = chunk.version.max(version);
        }
    }

    pub fn apply_snapshot(&mut self, source: &str, target: &str) {
        let source_prefix = child_prefix(source);
        let target_prefix = child_prefix(target);
        for (path, meta) in self.files.subtree(source, true) {
            let copied = if path == source { target.to_string() } else { format!("{}{}", target_prefix, &path[source_prefix.len()..]) };
            if self.files.exists(&copied) {
                continue;
            }
            for handle in &meta.chunks {
                if let Some(chunk) = self.chunks.find_mut(*handle) {
                    chunk.refcount += 1;
                }
            }
            self.files.insert(&copied, meta);
        }
    }

    pub fn apply_drop_chunk(&mut self, handle: u64) {
        self.chunks.erase(handle);
    }

    pub fn apply(&mut self, record: &LogRecord) {
        match &record.body {
            Some(Body::Create(r)) => self.apply_create(&r.path),
            Some(Body::Rename(r)) => self.apply_rename(&r.source, &r.target),
            Some(Body::Remove(r)) => self.apply_remove(&r.path),
            Some(Body::AllocHandle(r)) => self.apply_alloc_handle(r.handle),
            Some(Body::AddChunk(r)) => self.apply_add_chunk(&r.path, r.index, r.handle),
            Some(Body::ReplaceChunk(r)) => self.apply_replace_chunk(&r.path, r.index, r.handle),
            Some(Body::BumpVersion(r)) => self.apply_bump_version(r.handle, r.version),
            Some(Body::Snapshot(r)) => self.apply_snapshot(&r.source, &r.target),
            Some(Body::DropChunk(r)) => self.apply_drop_chunk(r.handle),
            None => warn!("log record without a body"),
        }
    }

    pub fn to_checkpoint(&self) -> Checkpoint {
        let mut checkpoint = Checkpoint { next_chunk_handle: self.next_handle, ..Default::default() };
        for (path, meta) in self.files.iter() {
            checkpoint.files.push(FileEntry { path: path.clone(), chunk_handles: meta.chunks.clone() });
        }
        for (handle, meta) in self.chunks.iter() {
            if meta.pending {
                continue;
            }
            checkpoint.chunks.push(ChunkEntry { handle: *handle, version: meta.version });
        }
        checkpoint
    }

    pub fn load(&mut self, checkpoint: &Checkpoint) {
        self.files.clear();
        self.chunks.clear();
        self.next_handle = checkpoint.next_chunk_handle.max(1);
        for entry in &checkpoint.files {
            self.files.insert(&entry.path, FileMeta { chunks: entry.chunk_handles.clone() });
        }
        for entry in &checkpoint.chunks {
            self.chunks.create(entry.handle, entry.version);
            self.apply_alloc_handle(entry.handle);
        }
    }

    pub fn recompute_refcounts(&mut self) {
        for (_, meta) in self.chunks.iter_mut() {
            meta.refcount = 0;
        }
        let handles: Vec<u64> = self.files.iter().flat_map(|(_, meta)| meta.chunks.iter().copied()).collect();
        for handle in handles {
            self.chunks.create(handle, 1).refcount += 1;
            self.apply_alloc_handle(handle);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn apply_is_idempotent_and_refcounts_follow_snapshots() {
        let mut state = MasterState::default();
        state.apply_create("/a");
        state.apply_create("/a");
        state.apply_add_chunk("/a", 0, 10);
        state.apply_add_chunk("/a", 0, 10);
        state.apply_add_chunk("/a", 1, 11);
        assert_eq!(state.files.find("/a").unwrap().chunks, vec![10, 11]);
        assert_eq!(state.next_handle, 12);
        state.apply_snapshot("/a", "/b");
        state.apply_snapshot("/a", "/b");
        assert_eq!(state.chunks.find(10).unwrap().refcount, 2);
        state.apply_replace_chunk("/b", 0, 20);
        assert_eq!(state.chunks.find(10).unwrap().refcount, 1);
        assert_eq!(state.chunks.find(20).unwrap().refcount, 1);
        state.apply_bump_version(10, 5);
        state.apply_bump_version(10, 3);
        assert_eq!(state.chunks.find(10).unwrap().version, 5);
        state.apply_remove("/a");
        state.apply_remove("/a");
        assert_eq!(state.chunks.find(10).unwrap().refcount, 0);
        assert_eq!(state.chunks.find(11).unwrap().refcount, 1);
        state.recompute_refcounts();
        assert_eq!(state.chunks.find(11).unwrap().refcount, 1);
        assert_eq!(state.chunks.find(20).unwrap().refcount, 1);
        state.apply_drop_chunk(10);
        assert!(state.chunks.find(10).is_none());
    }

    #[test]
    fn checkpoint_round_trips_without_pending_chunks() {
        let mut state = MasterState::default();
        state.apply_create("/f");
        state.apply_add_chunk("/f", 0, 3);
        state.chunks.create(9, 1).pending = true;
        state.apply_alloc_handle(9);
        let checkpoint = state.to_checkpoint();
        assert_eq!(checkpoint.next_chunk_handle, 10);
        assert_eq!(checkpoint.files.len(), 1);
        assert_eq!(checkpoint.chunks.len(), 1);
        let mut loaded = MasterState::default();
        loaded.load(&checkpoint);
        loaded.recompute_refcounts();
        assert_eq!(loaded.files.find("/f").unwrap().chunks, vec![3]);
        assert_eq!(loaded.chunks.find(3).unwrap().refcount, 1);
        assert_eq!(loaded.next_handle, 10);
    }
}

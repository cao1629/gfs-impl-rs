use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::time::Instant;

#[derive(Clone, Debug)]
pub struct Lease {
    pub primary: String,
    pub expiry: Instant,
    pub revoked: bool,
}

#[derive(Clone, Debug)]
pub struct ChunkMeta {
    pub version: u64,
    pub refcount: u32,
    pub pending: bool,
    pub granting: bool,
    pub locations: BTreeMap<String, Instant>,
    pub lease: Option<Lease>,
}

impl ChunkMeta {
    fn with_version(version: u64) -> ChunkMeta {
        ChunkMeta { version, refcount: 0, pending: false, granting: false, locations: BTreeMap::new(), lease: None }
    }
}

#[derive(Debug, Default)]
pub struct ChunkTable {
    chunks: HashMap<u64, ChunkMeta>,
    held_by: HashMap<String, BTreeSet<u64>>,
}

impl ChunkTable {
    pub fn find(&self, handle: u64) -> Option<&ChunkMeta> {
        self.chunks.get(&handle)
    }

    pub fn find_mut(&mut self, handle: u64) -> Option<&mut ChunkMeta> {
        self.chunks.get_mut(&handle)
    }

    pub fn create(&mut self, handle: u64, version: u64) -> &mut ChunkMeta {
        self.chunks.entry(handle).or_insert_with(|| ChunkMeta::with_version(version))
    }

    pub fn erase(&mut self, handle: u64) {
        let Some(meta) = self.chunks.remove(&handle) else { return };
        for server in meta.locations.keys() {
            if let Some(held) = self.held_by.get_mut(server) {
                held.remove(&handle);
                if held.is_empty() {
                    self.held_by.remove(server);
                }
            }
        }
    }

    pub fn add_location(&mut self, handle: u64, chunkserver: &str, when: Instant) -> bool {
        let Some(meta) = self.chunks.get_mut(&handle) else { return false };
        if meta.locations.contains_key(chunkserver) {
            return false;
        }
        meta.locations.insert(chunkserver.to_string(), when);
        self.held_by.entry(chunkserver.to_string()).or_default().insert(handle);
        true
    }

    pub fn remove_location(&mut self, handle: u64, chunkserver: &str) -> bool {
        let Some(meta) = self.chunks.get_mut(&handle) else { return false };
        if meta.locations.remove(chunkserver).is_none() {
            return false;
        }
        if let Some(held) = self.held_by.get_mut(chunkserver) {
            held.remove(&handle);
            if held.is_empty() {
                self.held_by.remove(chunkserver);
            }
        }
        true
    }

    pub fn held_by(&self, chunkserver: &str) -> BTreeSet<u64> {
        self.held_by.get(chunkserver).cloned().unwrap_or_default()
    }

    pub fn held_count(&self, chunkserver: &str) -> usize {
        self.held_by.get(chunkserver).map_or(0, |held| held.len())
    }

    pub fn iter(&self) -> impl Iterator<Item = (&u64, &ChunkMeta)> {
        self.chunks.iter()
    }

    pub fn iter_mut(&mut self) -> impl Iterator<Item = (&u64, &mut ChunkMeta)> {
        self.chunks.iter_mut()
    }

    pub fn len(&self) -> usize {
        self.chunks.len()
    }

    pub fn is_empty(&self) -> bool {
        self.chunks.is_empty()
    }

    pub fn clear(&mut self) {
        self.chunks.clear();
        self.held_by.clear();
    }
}

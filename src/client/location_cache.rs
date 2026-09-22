use std::collections::BTreeMap;
use std::time::{Duration, Instant};

use parking_lot::Mutex;

use crate::common::clock::now;
use crate::rpc::Replica;

#[derive(Clone, Debug, Default)]
pub struct CachedChunk {
    pub handle: u64,
    pub version: u64,
    pub replicas: Vec<Replica>,
}

#[derive(Clone, Debug, Default)]
pub struct CachedLease {
    pub handle: u64,
    pub version: u64,
    pub primary: Replica,
    pub secondaries: Vec<Replica>,
}

pub struct ExpiringCache<T> {
    ttl: Duration,
    entries: Mutex<BTreeMap<(String, u64), (Instant, T)>>,
}

impl<T: Clone> ExpiringCache<T> {
    pub fn new(ttl: Duration) -> ExpiringCache<T> {
        ExpiringCache { ttl, entries: Mutex::new(BTreeMap::new()) }
    }

    pub fn get(&self, path: &str, index: u64) -> Option<T> {
        let mut entries = self.entries.lock();
        let key = (path.to_string(), index);
        let (expiry, entry) = entries.get(&key)?;
        if *expiry <= now() {
            entries.remove(&key);
            return None;
        }
        Some(entry.clone())
    }

    pub fn put(&self, path: &str, index: u64, entry: T) {
        self.entries.lock().insert((path.to_string(), index), (now() + self.ttl, entry));
    }

    pub fn invalidate(&self, path: &str, index: u64) {
        self.entries.lock().remove(&(path.to_string(), index));
    }

    pub fn invalidate_file(&self, path: &str) {
        self.entries.lock().retain(|(cached, _), _| cached != path);
    }
}

pub type LocationCache = ExpiringCache<CachedChunk>;
pub type LeaseCache = ExpiringCache<CachedLease>;

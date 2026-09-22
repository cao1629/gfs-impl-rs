use std::collections::HashMap;
use std::sync::Arc;

use parking_lot::Mutex;
use tokio::sync::{OwnedRwLockReadGuard, OwnedRwLockWriteGuard, RwLock};

use crate::common::paths::ancestors_of;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LockMode {
    Read,
    Write,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LockRequest {
    pub path: String,
    pub mode: LockMode,
}

impl LockRequest {
    pub fn new(path: impl Into<String>, mode: LockMode) -> LockRequest {
        LockRequest { path: path.into(), mode }
    }
}

struct Entry {
    lock: Arc<RwLock<()>>,
    refs: usize,
}

#[derive(Default)]
pub struct LockTable {
    entries: Mutex<HashMap<String, Entry>>,
}

struct EntryRef {
    table: Arc<LockTable>,
    path: String,
}

impl Drop for EntryRef {
    fn drop(&mut self) {
        self.table.release_entry(&self.path);
    }
}

#[allow(dead_code)]
enum Guard {
    Read(OwnedRwLockReadGuard<()>),
    Write(OwnedRwLockWriteGuard<()>),
}

struct HeldLock {
    guard: Guard,
    entry: EntryRef,
}

pub struct LockSet {
    held: Vec<HeldLock>,
}

impl LockSet {
    pub fn release(&mut self) {
        while let Some(held) = self.held.pop() {
            drop(held.guard);
            drop(held.entry);
        }
    }

    pub fn is_empty(&self) -> bool {
        self.held.is_empty()
    }

    pub fn paths(&self) -> Vec<String> {
        self.held.iter().map(|h| h.entry.path.clone()).collect()
    }
}

impl Drop for LockSet {
    fn drop(&mut self) {
        self.release();
    }
}

fn depth_of(path: &str) -> usize {
    if path == "/" { 0 } else { path.matches('/').count() }
}

impl LockTable {
    pub fn new() -> Arc<LockTable> {
        Arc::new(LockTable::default())
    }

    pub fn normalize(mut requests: Vec<LockRequest>) -> Vec<LockRequest> {
        requests.sort_by(|a, b| depth_of(&a.path).cmp(&depth_of(&b.path)).then_with(|| a.path.cmp(&b.path)));
        let mut out: Vec<LockRequest> = Vec::with_capacity(requests.len());
        for request in requests {
            if let Some(last) = out.last_mut()
                && last.path == request.path
            {
                if request.mode == LockMode::Write {
                    last.mode = LockMode::Write;
                }
                continue;
            }
            out.push(request);
        }
        out
    }

    pub fn for_path(path: &str, leaf: LockMode) -> Vec<LockRequest> {
        let mut out: Vec<LockRequest> = ancestors_of(path).into_iter().map(|a| LockRequest::new(a, LockMode::Read)).collect();
        out.push(LockRequest::new(path, leaf));
        out
    }

    pub fn for_paths(first: &str, first_mode: LockMode, second: &str, second_mode: LockMode) -> Vec<LockRequest> {
        let mut out = Self::for_path(first, first_mode);
        out.extend(Self::for_path(second, second_mode));
        out
    }

    pub async fn acquire(self: &Arc<Self>, requests: Vec<LockRequest>) -> LockSet {
        let ordered = Self::normalize(requests);
        let mut held = Vec::with_capacity(ordered.len());
        for request in ordered {
            let lock = {
                let mut entries = self.entries.lock();
                let entry = entries.entry(request.path.clone()).or_insert_with(|| Entry { lock: Arc::new(RwLock::new(())), refs: 0 });
                entry.refs += 1;
                entry.lock.clone()
            };
            let entry = EntryRef { table: self.clone(), path: request.path };
            let guard = match request.mode {
                LockMode::Read => Guard::Read(lock.read_owned().await),
                LockMode::Write => Guard::Write(lock.write_owned().await),
            };
            held.push(HeldLock { guard, entry });
        }
        LockSet { held }
    }

    fn release_entry(&self, path: &str) {
        let mut entries = self.entries.lock();
        if let Some(entry) = entries.get_mut(path) {
            entry.refs -= 1;
            if entry.refs == 0 {
                entries.remove(path);
            }
        }
    }

    pub fn active_entries(&self) -> usize {
        self.entries.lock().len()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::time::Duration;

    use tokio::time::sleep;

    use super::*;

    #[test]
    fn normalizes_into_global_order_and_dedupes() {
        let ordered = LockTable::normalize(vec![
            LockRequest::new("/home/user", LockMode::Read),
            LockRequest::new("/", LockMode::Read),
            LockRequest::new("/home", LockMode::Read),
            LockRequest::new("/home/user", LockMode::Write),
            LockRequest::new("/b", LockMode::Read),
            LockRequest::new("/a", LockMode::Write),
        ]);
        let paths: Vec<&str> = ordered.iter().map(|r| r.path.as_str()).collect();
        assert_eq!(paths, vec!["/", "/a", "/b", "/home", "/home/user"]);
        assert_eq!(ordered[4].mode, LockMode::Write);
        let for_path = LockTable::for_path("/x/y/z", LockMode::Write);
        assert_eq!(for_path.len(), 4);
        assert_eq!(for_path[0].path, "/");
        assert_eq!(for_path[3].mode, LockMode::Write);
    }

    #[tokio::test]
    async fn readers_share_writers_exclude_and_entries_are_reclaimed() {
        let table = LockTable::new();
        let mut first = table.acquire(LockTable::for_path("/d/f", LockMode::Read)).await;
        let mut second = table.acquire(LockTable::for_path("/d/g", LockMode::Read)).await;
        let writer_done = Arc::new(AtomicBool::new(false));
        let writer = {
            let table = table.clone();
            let done = writer_done.clone();
            tokio::spawn(async move {
                let _w = table.acquire(LockTable::for_path("/d", LockMode::Write)).await;
                done.store(true, Ordering::SeqCst);
            })
        };
        sleep(Duration::from_millis(50)).await;
        assert!(!writer_done.load(Ordering::SeqCst));
        first.release();
        sleep(Duration::from_millis(50)).await;
        assert!(!writer_done.load(Ordering::SeqCst));
        second.release();
        writer.await.unwrap();
        assert!(writer_done.load(Ordering::SeqCst));
        assert_eq!(table.active_entries(), 0);
    }

    #[tokio::test]
    async fn lock_set_can_be_released_from_another_task() {
        let table = LockTable::new();
        let held = {
            let table = table.clone();
            tokio::spawn(async move { table.acquire(LockTable::for_path("/p", LockMode::Write)).await }).await.unwrap()
        };
        let got = Arc::new(AtomicBool::new(false));
        let waiter = {
            let table = table.clone();
            let got = got.clone();
            tokio::spawn(async move {
                let _r = table.acquire(LockTable::for_path("/p", LockMode::Read)).await;
                got.store(true, Ordering::SeqCst);
            })
        };
        sleep(Duration::from_millis(50)).await;
        assert!(!got.load(Ordering::SeqCst));
        tokio::spawn(async move { drop(held) }).await.unwrap();
        waiter.await.unwrap();
        assert!(got.load(Ordering::SeqCst));
        assert_eq!(table.active_entries(), 0);
    }
}

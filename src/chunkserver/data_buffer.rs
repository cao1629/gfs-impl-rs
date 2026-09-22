use std::collections::{BTreeMap, HashMap};

use parking_lot::Mutex;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct BufferKey {
    pub client_id: String,
    pub sequence: u64,
}

impl BufferKey {
    pub fn new(client_id: impl Into<String>, sequence: u64) -> BufferKey {
        BufferKey { client_id: client_id.into(), sequence }
    }
}

struct Inner {
    capacity: u64,
    used: u64,
    next_stamp: u64,
    entries: HashMap<BufferKey, (u64, Vec<u8>)>,
    order: BTreeMap<u64, BufferKey>,
}

pub struct DataBuffer {
    inner: Mutex<Inner>,
}

impl DataBuffer {
    pub fn new(capacity: u64) -> DataBuffer {
        DataBuffer { inner: Mutex::new(Inner { capacity, used: 0, next_stamp: 0, entries: HashMap::new(), order: BTreeMap::new() }) }
    }

    pub fn put(&self, key: BufferKey, data: Vec<u8>) {
        let mut inner = self.inner.lock();
        if let Some((stamp, old)) = inner.entries.remove(&key) {
            inner.used -= old.len() as u64;
            inner.order.remove(&stamp);
        }
        while let Some(oldest) = inner.order.keys().next().copied() {
            if inner.used + data.len() as u64 <= inner.capacity {
                break;
            }
            if let Some(oldest_key) = inner.order.remove(&oldest)
                && let Some((_, old)) = inner.entries.remove(&oldest_key)
            {
                inner.used -= old.len() as u64;
            }
        }
        let stamp = inner.next_stamp;
        inner.next_stamp += 1;
        inner.used += data.len() as u64;
        inner.order.insert(stamp, key.clone());
        inner.entries.insert(key, (stamp, data));
    }

    pub fn take(&self, key: &BufferKey) -> Option<Vec<u8>> {
        let mut inner = self.inner.lock();
        let (stamp, data) = inner.entries.remove(key)?;
        inner.order.remove(&stamp);
        inner.used -= data.len() as u64;
        Some(data)
    }

    pub fn size_of(&self, key: &BufferKey) -> Option<u64> {
        self.inner.lock().entries.get(key).map(|(_, data)| data.len() as u64)
    }

    pub fn bytes_in_use(&self) -> u64 {
        self.inner.lock().used
    }

    pub fn entry_count(&self) -> usize {
        self.inner.lock().entries.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn take_removes_the_entry() {
        let buffer = DataBuffer::new(1000);
        buffer.put(BufferKey::new("c1", 1), b"hello".to_vec());
        assert_eq!(buffer.entry_count(), 1);
        assert_eq!(buffer.bytes_in_use(), 5);
        assert_eq!(buffer.take(&BufferKey::new("c1", 1)), Some(b"hello".to_vec()));
        assert_eq!(buffer.take(&BufferKey::new("c1", 1)), None);
        assert_eq!(buffer.bytes_in_use(), 0);
        assert_eq!(buffer.take(&BufferKey::new("c2", 1)), None);
    }

    #[test]
    fn evicts_oldest_when_over_capacity() {
        let buffer = DataBuffer::new(10);
        buffer.put(BufferKey::new("c", 1), b"aaaa".to_vec());
        buffer.put(BufferKey::new("c", 2), b"bbbb".to_vec());
        buffer.put(BufferKey::new("c", 3), b"cccc".to_vec());
        assert_eq!(buffer.entry_count(), 2);
        assert_eq!(buffer.take(&BufferKey::new("c", 1)), None);
        assert_eq!(buffer.take(&BufferKey::new("c", 2)), Some(b"bbbb".to_vec()));
        assert_eq!(buffer.take(&BufferKey::new("c", 3)), Some(b"cccc".to_vec()));
    }

    #[test]
    fn replaces_an_existing_key() {
        let buffer = DataBuffer::new(100);
        buffer.put(BufferKey::new("c", 1), b"one".to_vec());
        buffer.put(BufferKey::new("c", 1), b"uno".to_vec());
        assert_eq!(buffer.entry_count(), 1);
        assert_eq!(buffer.bytes_in_use(), 3);
        assert_eq!(buffer.take(&BufferKey::new("c", 1)), Some(b"uno".to_vec()));
    }

    #[test]
    fn oversized_entry_still_lands_alone() {
        let buffer = DataBuffer::new(4);
        buffer.put(BufferKey::new("c", 1), b"ab".to_vec());
        buffer.put(BufferKey::new("c", 2), b"abcdefgh".to_vec());
        assert_eq!(buffer.entry_count(), 1);
        assert_eq!(buffer.take(&BufferKey::new("c", 2)), Some(b"abcdefgh".to_vec()));
    }
}

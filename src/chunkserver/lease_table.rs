use std::collections::HashMap;
use std::time::{Duration, Instant};

use parking_lot::Mutex;

use crate::common::clock::now;
use crate::rpc::Replica;

#[derive(Clone, Debug)]
pub struct LeaseInfo {
    pub expiry: Instant,
    pub secondaries: Vec<Replica>,
}

#[derive(Clone, Debug)]
pub enum LeaseCheck {
    Primary(LeaseInfo),
    Expired,
    NotHeld,
}

struct Slot {
    held: bool,
    expiry: Instant,
    secondaries: Vec<Replica>,
    next_serial: u64,
}

impl Slot {
    fn empty() -> Slot {
        Slot { held: false, expiry: now(), secondaries: Vec::new(), next_serial: 1 }
    }
}

pub struct LeaseTable {
    skew_margin: Duration,
    slots: Mutex<HashMap<u64, Slot>>,
}

impl LeaseTable {
    pub fn new(skew_margin: Duration) -> LeaseTable {
        LeaseTable { skew_margin, slots: Mutex::new(HashMap::new()) }
    }

    fn local_expiry(&self, lease: Duration) -> Instant {
        now() + lease.saturating_sub(self.skew_margin)
    }

    pub fn grant(&self, handle: u64, lease: Duration, secondaries: Vec<Replica>) {
        let mut slots = self.slots.lock();
        let slot = slots.entry(handle).or_insert_with(Slot::empty);
        slot.held = true;
        slot.expiry = self.local_expiry(lease);
        slot.secondaries = secondaries;
    }

    pub fn extend(&self, handle: u64, lease: Duration) -> bool {
        let mut slots = self.slots.lock();
        match slots.get_mut(&handle) {
            Some(slot) if slot.held => {
                slot.expiry = self.local_expiry(lease);
                true
            }
            _ => false,
        }
    }

    pub fn revoke(&self, handle: u64) {
        if let Some(slot) = self.slots.lock().get_mut(&handle) {
            slot.held = false;
            slot.secondaries.clear();
        }
    }

    pub fn check(&self, handle: u64) -> LeaseCheck {
        let slots = self.slots.lock();
        match slots.get(&handle) {
            Some(slot) if slot.held => {
                if now() > slot.expiry {
                    LeaseCheck::Expired
                } else {
                    LeaseCheck::Primary(LeaseInfo { expiry: slot.expiry, secondaries: slot.secondaries.clone() })
                }
            }
            _ => LeaseCheck::NotHeld,
        }
    }

    pub fn next_serial(&self, handle: u64) -> u64 {
        let mut slots = self.slots.lock();
        let slot = slots.entry(handle).or_insert_with(Slot::empty);
        let serial = slot.next_serial;
        slot.next_serial += 1;
        serial
    }

    pub fn held_handles(&self) -> Vec<u64> {
        let t = now();
        let mut out: Vec<u64> = self.slots.lock().iter().filter(|(_, slot)| slot.held && t <= slot.expiry).map(|(h, _)| *h).collect();
        out.sort_unstable();
        out
    }
}

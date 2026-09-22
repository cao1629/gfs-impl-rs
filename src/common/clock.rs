use std::time::{Instant, SystemTime, UNIX_EPOCH};

pub fn now() -> Instant {
    Instant::now()
}

pub fn unix_seconds() -> i64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_secs() as i64).unwrap_or(0)
}

#![allow(dead_code)]

use std::future::Future;
use std::net::TcpListener;
use std::time::{Duration, Instant};

use tokio::time::sleep;

pub fn pattern(size: usize, seed: u32) -> Vec<u8> {
    let mut out = vec![0u8; size];
    let mut x = seed.wrapping_mul(2654435761).wrapping_add(12345);
    for byte in out.iter_mut() {
        x ^= x << 13;
        x ^= x >> 17;
        x ^= x << 5;
        *byte = b'a' + (x % 26) as u8;
    }
    out
}

pub fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port()
}

pub async fn wait_until<F, Fut>(mut probe: F, limit: Duration) -> bool
where
    F: FnMut() -> Fut,
    Fut: Future<Output = bool>,
{
    let deadline = Instant::now() + limit;
    loop {
        if probe().await {
            return true;
        }
        if Instant::now() >= deadline {
            return probe().await;
        }
        sleep(Duration::from_millis(20)).await;
    }
}

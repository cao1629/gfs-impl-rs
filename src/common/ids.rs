use rand::RngCore;

pub fn random_hex_id(bytes: usize) -> String {
    let mut buf = vec![0u8; bytes];
    rand::rng().fill_bytes(&mut buf);
    buf.iter().map(|b| format!("{b:02x}")).collect()
}

pub fn handle_to_hex(handle: u64) -> String {
    format!("{handle:016x}")
}

pub fn hex_to_handle(text: &str) -> Option<u64> {
    if text.len() != 16 || !text.bytes().all(|b| b.is_ascii_hexdigit()) {
        return None;
    }
    u64::from_str_radix(text, 16).ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn handles_round_trip_through_hex() {
        assert_eq!(handle_to_hex(0x0a1b2c3d4e5f6071), "0a1b2c3d4e5f6071");
        assert_eq!(hex_to_handle("0a1b2c3d4e5f6071"), Some(0x0a1b2c3d4e5f6071));
        assert_eq!(hex_to_handle("0a1b"), None);
        assert_eq!(hex_to_handle("+a1b2c3d4e5f6071"), None);
        assert_eq!(random_hex_id(16).len(), 32);
        assert_ne!(random_hex_id(16), random_hex_id(16));
    }
}

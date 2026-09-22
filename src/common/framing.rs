pub const FRAME_HEADER_SIZE: usize = 8;

pub fn encode_record(payload: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(FRAME_HEADER_SIZE + payload.len());
    out.extend_from_slice(&(payload.len() as u32).to_le_bytes());
    out.extend_from_slice(&crc32fast::hash(payload).to_le_bytes());
    out.extend_from_slice(payload);
    out
}

#[derive(Debug, Default)]
pub struct DecodedRecords {
    pub payloads: Vec<Vec<u8>>,
    pub consumed: usize,
    pub torn_tail: bool,
}

pub fn decode_records(bytes: &[u8]) -> DecodedRecords {
    let mut result = DecodedRecords::default();
    let mut pos = 0;
    while pos + FRAME_HEADER_SIZE <= bytes.len() {
        let len = u32::from_le_bytes(bytes[pos..pos + 4].try_into().unwrap()) as usize;
        let crc = u32::from_le_bytes(bytes[pos + 4..pos + 8].try_into().unwrap());
        let start = pos + FRAME_HEADER_SIZE;
        if start + len > bytes.len() {
            result.torn_tail = true;
            break;
        }
        let payload = &bytes[start..start + len];
        if crc32fast::hash(payload) != crc {
            result.torn_tail = true;
            break;
        }
        result.payloads.push(payload.to_vec());
        pos = start + len;
    }
    if pos < bytes.len() {
        result.torn_tail = true;
    }
    result.consumed = pos;
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trips_several_records() {
        let mut bytes = encode_record(b"alpha");
        bytes.extend(encode_record(b""));
        bytes.extend(encode_record(&vec![b'x'; 70000]));
        let decoded = decode_records(&bytes);
        assert_eq!(decoded.payloads.len(), 3);
        assert_eq!(decoded.payloads[0], b"alpha");
        assert!(decoded.payloads[1].is_empty());
        assert_eq!(decoded.payloads[2].len(), 70000);
        assert!(!decoded.torn_tail);
        assert_eq!(decoded.consumed, bytes.len());
    }

    #[test]
    fn stops_at_torn_tail() {
        let mut bytes = encode_record(b"first");
        bytes.extend(encode_record(b"second"));
        bytes.truncate(bytes.len() - 2);
        let decoded = decode_records(&bytes);
        assert_eq!(decoded.payloads.len(), 1);
        assert_eq!(decoded.payloads[0], b"first");
        assert!(decoded.torn_tail);
        assert_eq!(decoded.consumed, encode_record(b"first").len());
    }

    #[test]
    fn rejects_corrupted_payload() {
        let mut bytes = encode_record(b"first");
        bytes.extend(encode_record(b"second"));
        let last = bytes.len() - 1;
        bytes[last] ^= 0x01;
        let decoded = decode_records(&bytes);
        assert_eq!(decoded.payloads.len(), 1);
        assert!(decoded.torn_tail);
    }
}

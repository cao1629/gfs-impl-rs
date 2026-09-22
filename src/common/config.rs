use std::process::exit;
use std::time::Duration;

#[derive(Clone, Debug)]
pub struct Config {
    pub chunk_size: u64,
    pub max_record_append_size: u64,
    pub replication_goal: u32,
    pub min_replicas_for_write: u32,
    pub checksum_block_size: u64,

    pub lease_duration: Duration,
    pub lease_clock_skew_margin: Duration,
    pub heartbeat_interval: Duration,
    pub chunkserver_dead_timeout: Duration,
    pub deleted_file_retention: Duration,
    pub client_location_cache_ttl: Duration,
    pub gc_interval: Duration,

    pub mutation_retry_inner: u32,
    pub mutation_retry_outer: u32,
    pub retry_backoff_base: Duration,
    pub outer_retry_delay: Duration,

    pub log_flush_batch_size: u32,
    pub log_flush_max_delay: Duration,
    pub checkpoint_log_threshold: u64,

    pub rpc_deadline: Duration,
    pub client_rpc_deadline: Duration,
    pub master_worker_threads: u32,
    pub push_frame_size: u64,
    pub data_buffer_capacity: u64,
    pub rack: String,

    pub listen: String,
    pub advertise: String,
    pub master_address: String,
    pub data_dir: String,
}

impl Default for Config {
    fn default() -> Self {
        Config {
            chunk_size: 64 << 20,
            max_record_append_size: 0,
            replication_goal: 3,
            min_replicas_for_write: 1,
            checksum_block_size: 64 << 10,
            lease_duration: Duration::from_secs(60),
            lease_clock_skew_margin: Duration::from_secs(5),
            heartbeat_interval: Duration::from_secs(3),
            chunkserver_dead_timeout: Duration::from_secs(15),
            deleted_file_retention: Duration::from_secs(3 * 24 * 3600),
            client_location_cache_ttl: Duration::from_secs(60),
            gc_interval: Duration::from_secs(60),
            mutation_retry_inner: 3,
            mutation_retry_outer: 2,
            retry_backoff_base: Duration::from_millis(200),
            outer_retry_delay: Duration::ZERO,
            log_flush_batch_size: 32,
            log_flush_max_delay: Duration::from_millis(10),
            checkpoint_log_threshold: 64 << 20,
            rpc_deadline: Duration::from_secs(2),
            client_rpc_deadline: Duration::from_secs(10),
            master_worker_threads: 16,
            push_frame_size: 256 << 10,
            data_buffer_capacity: 64 << 20,
            rack: String::new(),
            listen: "127.0.0.1:7000".to_string(),
            advertise: String::new(),
            master_address: "127.0.0.1:7000".to_string(),
            data_dir: String::new(),
        }
    }
}

const KEYS: &[&str] = &[
    "advertise",
    "checkpoint_log_threshold",
    "checksum_block_size",
    "chunk_size",
    "chunkserver_dead_timeout",
    "client_location_cache_ttl",
    "client_rpc_deadline",
    "data_buffer_capacity",
    "data_dir",
    "deleted_file_retention",
    "gc_interval",
    "heartbeat_interval",
    "lease_clock_skew_margin",
    "lease_duration",
    "listen",
    "log_flush_batch_size",
    "log_flush_max_delay",
    "master_address",
    "master_worker_threads",
    "max_record_append_size",
    "min_replicas_for_write",
    "mutation_retry_inner",
    "mutation_retry_outer",
    "outer_retry_delay",
    "push_frame_size",
    "rack",
    "replication_goal",
    "retry_backoff_base",
    "rpc_deadline",
];

fn set_size(field: &mut u64, value: &str) -> bool {
    match parse_size(value) {
        Some(n) => {
            *field = n;
            true
        }
        None => false,
    }
}

fn set_count(field: &mut u32, value: &str) -> bool {
    match parse_size(value) {
        Some(n) if n <= u32::MAX as u64 => {
            *field = n as u32;
            true
        }
        _ => false,
    }
}

fn set_duration(field: &mut Duration, value: &str) -> bool {
    match parse_duration(value) {
        Some(d) => {
            *field = d;
            true
        }
        None => false,
    }
}

impl Config {
    pub fn effective_max_record_append_size(&self) -> u64 {
        if self.max_record_append_size == 0 { self.chunk_size / 4 } else { self.max_record_append_size }
    }

    pub fn effective_outer_retry_delay(&self) -> Duration {
        if self.outer_retry_delay.is_zero() { self.chunkserver_dead_timeout } else { self.outer_retry_delay }
    }

    pub fn effective_advertise(&self) -> String {
        if self.advertise.is_empty() { self.listen.clone() } else { self.advertise.clone() }
    }

    pub fn set(&mut self, key: &str, value: &str) -> bool {
        match key {
            "chunk_size" => set_size(&mut self.chunk_size, value),
            "max_record_append_size" => set_size(&mut self.max_record_append_size, value),
            "replication_goal" => set_count(&mut self.replication_goal, value),
            "min_replicas_for_write" => set_count(&mut self.min_replicas_for_write, value),
            "checksum_block_size" => set_size(&mut self.checksum_block_size, value),
            "lease_duration" => set_duration(&mut self.lease_duration, value),
            "lease_clock_skew_margin" => set_duration(&mut self.lease_clock_skew_margin, value),
            "heartbeat_interval" => set_duration(&mut self.heartbeat_interval, value),
            "chunkserver_dead_timeout" => set_duration(&mut self.chunkserver_dead_timeout, value),
            "deleted_file_retention" => set_duration(&mut self.deleted_file_retention, value),
            "client_location_cache_ttl" => set_duration(&mut self.client_location_cache_ttl, value),
            "gc_interval" => set_duration(&mut self.gc_interval, value),
            "mutation_retry_inner" => set_count(&mut self.mutation_retry_inner, value),
            "mutation_retry_outer" => set_count(&mut self.mutation_retry_outer, value),
            "retry_backoff_base" => set_duration(&mut self.retry_backoff_base, value),
            "outer_retry_delay" => set_duration(&mut self.outer_retry_delay, value),
            "log_flush_batch_size" => set_count(&mut self.log_flush_batch_size, value),
            "log_flush_max_delay" => set_duration(&mut self.log_flush_max_delay, value),
            "checkpoint_log_threshold" => set_size(&mut self.checkpoint_log_threshold, value),
            "rpc_deadline" => set_duration(&mut self.rpc_deadline, value),
            "client_rpc_deadline" => set_duration(&mut self.client_rpc_deadline, value),
            "master_worker_threads" => set_count(&mut self.master_worker_threads, value),
            "push_frame_size" => set_size(&mut self.push_frame_size, value),
            "data_buffer_capacity" => set_size(&mut self.data_buffer_capacity, value),
            "rack" => {
                self.rack = value.to_string();
                true
            }
            "listen" => {
                self.listen = value.to_string();
                true
            }
            "advertise" => {
                self.advertise = value.to_string();
                true
            }
            "master_address" => {
                self.master_address = value.to_string();
                true
            }
            "data_dir" => {
                self.data_dir = value.to_string();
                true
            }
            _ => false,
        }
    }

    pub fn usage() -> String {
        let mut text = String::from("options (--key=value or --key value):\n");
        for key in KEYS {
            text.push_str("  --");
            text.push_str(key);
            text.push('\n');
        }
        text
    }

    pub fn apply_flags(&mut self, args: impl IntoIterator<Item = String>) -> Result<Vec<String>, String> {
        let mut positional = Vec::new();
        let mut iter = args.into_iter();
        while let Some(arg) = iter.next() {
            let Some(flag) = arg.strip_prefix("--") else {
                positional.push(arg);
                continue;
            };
            let (key, value) = match flag.split_once('=') {
                Some((key, value)) => (key.to_string(), value.to_string()),
                None => match iter.next() {
                    Some(value) => (flag.to_string(), value),
                    None => return Err(format!("missing value for --{flag}")),
                },
            };
            if !self.set(&key, &value) {
                return Err(format!("bad option --{key}={value}"));
            }
        }
        Ok(positional)
    }

    pub fn from_args(args: impl IntoIterator<Item = String>) -> Config {
        let mut config = Config::default();
        match config.apply_flags(args) {
            Ok(positional) if positional.is_empty() => config,
            Ok(positional) => {
                eprintln!("unexpected argument: {}\n{}", positional[0], Config::usage());
                exit(2)
            }
            Err(message) => {
                eprintln!("{message}\n{}", Config::usage());
                exit(2)
            }
        }
    }
}

fn split_number(text: &str) -> Option<(f64, &str)> {
    let bytes = text.as_bytes();
    let mut end = 0;
    if end < bytes.len() && (bytes[end] == b'+' || bytes[end] == b'-') {
        end += 1;
    }
    let mut digits = 0;
    while end < bytes.len() && bytes[end].is_ascii_digit() {
        end += 1;
        digits += 1;
    }
    if end < bytes.len() && bytes[end] == b'.' {
        end += 1;
        while end < bytes.len() && bytes[end].is_ascii_digit() {
            end += 1;
            digits += 1;
        }
    }
    if digits == 0 {
        return None;
    }
    let value: f64 = text[..end].parse().ok()?;
    Some((value, &text[end..]))
}

pub fn parse_duration(text: &str) -> Option<Duration> {
    let (value, unit) = split_number(text)?;
    let factor = match unit {
        "" | "ms" => 1.0,
        "s" => 1000.0,
        "m" => 60_000.0,
        "h" => 3_600_000.0,
        "d" => 86_400_000.0,
        _ => return None,
    };
    let millis = value * factor;
    if millis < 0.0 {
        return None;
    }
    Some(Duration::from_millis(millis as u64))
}

pub fn parse_size(text: &str) -> Option<u64> {
    let (value, unit) = split_number(text)?;
    let factor = match unit {
        "" => 1.0,
        "K" | "KB" | "k" => 1024.0,
        "M" | "MB" | "m" => 1024.0 * 1024.0,
        "G" | "GB" | "g" => 1024.0 * 1024.0 * 1024.0,
        _ => return None,
    };
    let bytes = value * factor;
    if bytes < 0.0 {
        return None;
    }
    Some(bytes as u64)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_units() {
        assert_eq!(parse_duration("250"), Some(Duration::from_millis(250)));
        assert_eq!(parse_duration("1.5s"), Some(Duration::from_millis(1500)));
        assert_eq!(parse_duration("3d"), Some(Duration::from_millis(3 * 86400 * 1000)));
        assert_eq!(parse_duration("x"), None);
        assert_eq!(parse_size("1M"), Some(1 << 20));
        assert_eq!(parse_size("64K"), Some(64 << 10));
        assert_eq!(parse_size("1K5"), None);
    }

    #[test]
    fn sets_by_key() {
        let mut config = Config::default();
        assert!(config.set("chunk_size", "1M"));
        assert!(config.set("lease_duration", "3s"));
        assert!(config.set("rack", "r1"));
        assert_eq!(config.chunk_size, 1 << 20);
        assert_eq!(config.lease_duration, Duration::from_secs(3));
        assert_eq!(config.rack, "r1");
        assert!(!config.set("no_such_key", "1"));
        assert_eq!(config.effective_max_record_append_size(), (1 << 20) / 4);
        assert_eq!(config.effective_outer_retry_delay(), config.chunkserver_dead_timeout);
    }

    #[test]
    fn splits_flags_from_positional_arguments() {
        let mut config = Config::default();
        let args = ["--chunk_size=2M", "create", "--rack", "r2", "/a"].map(String::from);
        let positional = config.apply_flags(args).unwrap();
        assert_eq!(positional, vec!["create".to_string(), "/a".to_string()]);
        assert_eq!(config.chunk_size, 2 << 20);
        assert_eq!(config.rack, "r2");
        assert!(config.apply_flags(["--rack".to_string()]).is_err());
        assert!(config.apply_flags(["--bogus=1".to_string()]).is_err());
    }
}

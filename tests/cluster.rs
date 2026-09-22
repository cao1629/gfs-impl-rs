mod support;

use std::collections::BTreeMap;
use std::fs::{self, File};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::Duration;

use gfs::client::{Client, DirEntry, ErrorCode};
use gfs::common::config::{Config, parse_duration};
use gfs::common::net::lazy_channel;
use gfs::rpc::chunkserver_client::ChunkserverClient;
use gfs::rpc::master_client::MasterClient;
use gfs::rpc::*;
use tokio::time::{sleep, timeout};
use tonic::transport::Channel;

use support::{free_port, pattern, wait_until};

struct Process {
    child: Option<Child>,
    address: String,
    data_dir: PathBuf,
    rack: String,
    log_path: PathBuf,
}

struct LocalCluster {
    root: PathBuf,
    flags: BTreeMap<String, String>,
    master: Process,
    chunkservers: Vec<Process>,
}

fn default_flags() -> BTreeMap<String, String> {
    [
        ("chunk_size", "1M"),
        ("lease_duration", "3s"),
        ("lease_clock_skew_margin", "200ms"),
        ("heartbeat_interval", "200ms"),
        ("chunkserver_dead_timeout", "1s"),
        ("deleted_file_retention", "2s"),
        ("gc_interval", "500ms"),
        ("rpc_deadline", "1s"),
        ("client_rpc_deadline", "5s"),
        ("retry_backoff_base", "50ms"),
        ("log_flush_max_delay", "5ms"),
        ("client_location_cache_ttl", "2s"),
    ]
    .into_iter()
    .map(|(k, v)| (k.to_string(), v.to_string()))
    .collect()
}

fn spawn(binary: &str, args: &[String], log_path: &Path) -> Child {
    let log = File::options().create(true).append(true).open(log_path).unwrap();
    let err = log.try_clone().unwrap();
    Command::new(binary).args(args).stdout(Stdio::from(log)).stderr(Stdio::from(err)).spawn().expect("spawn a cluster process")
}

async fn wait_for_master(address: &str) {
    let client = MasterClient::new(lazy_channel(address));
    let ready = wait_until(
        || async {
            timeout(Duration::from_millis(300), client.clone().get_cluster_info(GetClusterInfoRequest {})).await.is_ok_and(|r| r.is_ok())
        },
        Duration::from_secs(15),
    )
    .await;
    assert!(ready, "timed out waiting for master at {address}");
}

async fn wait_for_chunkserver(address: &str) {
    let client = ChunkserverClient::new(lazy_channel(address));
    let ready = wait_until(
        || async {
            timeout(Duration::from_millis(300), client.clone().get_chunk_length(GetChunkLengthRequest { handle: 0 }))
                .await
                .is_ok_and(|r| r.is_ok())
        },
        Duration::from_secs(15),
    )
    .await;
    assert!(ready, "timed out waiting for chunkserver at {address}");
}

impl LocalCluster {
    async fn start(chunkservers: usize, overrides: &[(&str, &str)]) -> LocalCluster {
        let mut flags = default_flags();
        for (k, v) in overrides {
            flags.insert(k.to_string(), v.to_string());
        }
        let root = std::env::temp_dir().join(format!("gfs-test-{}-{}", std::process::id(), free_port()));
        fs::create_dir_all(&root).unwrap();
        let master = Process {
            child: None,
            address: format!("127.0.0.1:{}", free_port()),
            data_dir: root.join("master"),
            rack: String::new(),
            log_path: root.join("master.log"),
        };
        fs::create_dir_all(&master.data_dir).unwrap();
        let mut cluster = LocalCluster { root, flags, master, chunkservers: Vec::new() };
        cluster.spawn_master();
        wait_for_master(&cluster.master.address).await;
        for i in 0..chunkservers {
            let process = Process {
                child: None,
                address: format!("127.0.0.1:{}", free_port()),
                data_dir: cluster.root.join(format!("cs{i}")),
                rack: format!("rack{}", i % 2),
                log_path: cluster.root.join(format!("cs{i}.log")),
            };
            fs::create_dir_all(&process.data_dir).unwrap();
            cluster.chunkservers.push(process);
            cluster.spawn_chunkserver(i);
        }
        for i in 0..chunkservers {
            wait_for_chunkserver(&cluster.chunkservers[i].address).await;
        }
        sleep(cluster.heartbeat_interval() * 3).await;
        cluster
    }

    fn heartbeat_interval(&self) -> Duration {
        parse_duration(&self.flags["heartbeat_interval"]).unwrap()
    }

    fn common_args(&self) -> Vec<String> {
        self.flags.iter().map(|(k, v)| format!("--{k}={v}")).collect()
    }

    fn spawn_master(&mut self) {
        let mut args = self.common_args();
        args.push(format!("--listen={}", self.master.address));
        args.push(format!("--data_dir={}", self.master.data_dir.display()));
        self.master.child = Some(spawn(env!("CARGO_BIN_EXE_gfs_master"), &args, &self.master.log_path));
    }

    fn spawn_chunkserver(&mut self, i: usize) {
        let mut args = self.common_args();
        let process = &self.chunkservers[i];
        args.push(format!("--listen={}", process.address));
        args.push(format!("--master_address={}", self.master.address));
        args.push(format!("--data_dir={}", process.data_dir.display()));
        args.push(format!("--rack={}", process.rack));
        let child = spawn(env!("CARGO_BIN_EXE_gfs_chunkserver"), &args, &process.log_path);
        self.chunkservers[i].child = Some(child);
    }

    fn kill(process: &mut Process) {
        if let Some(mut child) = process.child.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }

    fn kill_chunkserver(&mut self, i: usize) {
        Self::kill(&mut self.chunkservers[i]);
    }

    async fn restart_chunkserver(&mut self, i: usize) {
        Self::kill(&mut self.chunkservers[i]);
        self.spawn_chunkserver(i);
        wait_for_chunkserver(&self.chunkservers[i].address).await;
    }

    fn kill_master(&mut self) {
        Self::kill(&mut self.master);
    }

    async fn restart_master(&mut self) {
        Self::kill(&mut self.master);
        self.spawn_master();
        wait_for_master(&self.master.address).await;
        sleep(self.heartbeat_interval() * 3).await;
    }

    fn client_config(&self) -> Config {
        let mut config = Config::default();
        for (k, v) in &self.flags {
            assert!(config.set(k, v), "bad flag {k}={v}");
        }
        config.master_address = self.master.address.clone();
        config
    }

    fn client(&self) -> Client {
        Client::new(self.client_config())
    }

    fn master_client(&self) -> MasterClient<Channel> {
        MasterClient::new(lazy_channel(&self.master.address))
    }

    fn chunkserver_address(&self, i: usize) -> &str {
        &self.chunkservers[i].address
    }

    fn master_data_dir(&self) -> &Path {
        &self.master.data_dir
    }

    fn chunk_files_on_disk(&self) -> usize {
        self.chunkservers
            .iter()
            .map(|p| {
                fs::read_dir(&p.data_dir)
                    .map(|d| d.flatten().filter(|e| e.path().extension().is_some_and(|x| x == "chunk")).count())
                    .unwrap_or(0)
            })
            .sum()
    }

    async fn replicas_of(&self, path: &str, index: u64) -> Vec<Replica> {
        let request = FindLocationRequest { path: path.to_string(), first_index: index, count: 1 };
        match self.master_client().find_location(request).await {
            Ok(resp) => resp.into_inner().chunks.first().map(|c| c.replicas.clone()).unwrap_or_default(),
            Err(_) => Vec::new(),
        }
    }
}

impl Drop for LocalCluster {
    fn drop(&mut self) {
        for process in &mut self.chunkservers {
            Self::kill(process);
        }
        Self::kill(&mut self.master);
        if std::env::var_os("GFS_KEEP_TEST_DIRS").is_none() {
            let _ = fs::remove_dir_all(&self.root);
        } else {
            eprintln!("test cluster kept at {}", self.root.display());
        }
    }
}

fn names(entries: &[DirEntry]) -> Vec<String> {
    let mut out: Vec<String> = entries.iter().map(|e| e.name.clone()).collect();
    out.sort();
    out
}

fn has_replica_at(replicas: &[Replica], address: &str) -> bool {
    replicas.iter().any(|r| r.address == address)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn create_list_delete_and_undelete() {
    let cluster = LocalCluster::start(3, &[]).await;
    let client = cluster.client();
    client.create("/a/b").await.unwrap();
    client.create("/a/c").await.unwrap();
    client.create("/a/sub/d").await.unwrap();
    assert_eq!(client.create("/a/b").await.unwrap_err().code, ErrorCode::AlreadyExists);
    assert_eq!(client.create("/a/b/under-a-file").await.unwrap_err().code, ErrorCode::InvalidArgument);

    assert_eq!(names(&client.list("/a", false).await.unwrap()), vec!["b", "c", "sub"]);
    assert_eq!(names(&client.list("/", false).await.unwrap()), vec!["a"]);

    client.remove("/a/b").await.unwrap();
    assert_eq!(client.open("/a/b").await.unwrap_err().code, ErrorCode::NotFound);
    assert_eq!(names(&client.list("/a", false).await.unwrap()), vec!["c", "sub"]);
    let entries = client.list("/a", true).await.unwrap();
    assert_eq!(entries.len(), 3);
    let hidden = entries.iter().map(|e| e.name.clone()).find(|n| n.starts_with(".deleted.")).expect("a hidden entry");
    client.rename(&format!("/a/{hidden}"), "/a/b").await.unwrap();
    client.open("/a/b").await.unwrap();
    assert_eq!(client.remove("/a").await.unwrap_err().code, ErrorCode::NotFound);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn write_across_chunk_boundary_reads_back() {
    let cluster = LocalCluster::start(3, &[]).await;
    let client = cluster.client();
    let chunk: u64 = 1 << 20;
    let data = pattern((chunk * 2 + chunk / 2) as usize, 7);
    client.create("/big").await.unwrap();
    client.write("/big", 0, &data).await.unwrap();

    assert_eq!(client.read("/big", 0, data.len() as u64 + 1000).await.unwrap(), data);
    let at = (chunk - 10) as usize;
    assert_eq!(client.read("/big", at as u64, 20).await.unwrap(), data[at..at + 20]);
    assert_eq!(client.length("/big").await.unwrap(), data.len() as u64);
    assert_eq!(client.open("/big").await.unwrap().chunk_count, 3);

    client.write("/big", chunk + 100, b"hello").await.unwrap();
    let at = (chunk + 98) as usize;
    let mut expected = data[at..at + 2].to_vec();
    expected.extend_from_slice(b"hello");
    expected.extend_from_slice(&data[at + 7..at + 9]);
    assert_eq!(client.read("/big", at as u64, 9).await.unwrap(), expected);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_record_appends_stay_intact() {
    let cluster = LocalCluster::start(3, &[]).await;
    let record_size = 5000usize;
    let writers = 4u32;
    let per_writer = 60u32;
    cluster.client().create("/log").await.unwrap();
    let mut tasks = Vec::new();
    for w in 0..writers {
        let config = cluster.client_config();
        tasks.push(tokio::spawn(async move {
            let client = Client::new(config);
            let mut offsets = Vec::new();
            let mut failures = 0;
            for i in 0..per_writer {
                let seed = w * 100000 + i;
                match client.record_append("/log", &pattern(record_size, seed)).await {
                    Ok(offset) => offsets.push((offset, seed)),
                    Err(_) => failures += 1,
                }
            }
            (offsets, failures)
        }));
    }
    let mut all = Vec::new();
    let mut failures = 0;
    for task in tasks {
        let (offsets, failed) = task.await.unwrap();
        all.extend(offsets);
        failures += failed;
    }
    assert_eq!(failures, 0);

    let reader = cluster.client();
    for (offset, seed) in &all {
        assert_eq!(reader.read("/log", *offset, record_size as u64).await.unwrap(), pattern(record_size, *seed), "offset {offset}");
    }
    assert_eq!(all.len(), (writers * per_writer) as usize);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn record_append_pads_at_chunk_boundary() {
    let cluster = LocalCluster::start(3, &[]).await;
    let client = cluster.client();
    client.create("/pad").await.unwrap();
    let record = 250 * 1024;
    let mut offsets = Vec::new();
    for i in 0..5u32 {
        offsets.push(client.record_append("/pad", &pattern(record, i)).await.unwrap());
    }
    assert_eq!(offsets[3], 3 * record as u64);
    assert_eq!(offsets[4], 1 << 20);
    assert_eq!(client.read("/pad", offsets[4], record as u64).await.unwrap(), pattern(record, 4));
    assert_eq!(client.record_append("/pad", &pattern(300 * 1024, 9)).await.unwrap_err().code, ErrorCode::InvalidArgument);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn chunkserver_death_then_writes_recover() {
    let mut cluster = LocalCluster::start(3, &[]).await;
    let client = cluster.client();
    client.create("/f").await.unwrap();
    client.write("/f", 0, &pattern(100, 1)).await.unwrap();
    let before = cluster.replicas_of("/f", 0).await;
    assert_eq!(before.len(), 3);
    assert!(has_replica_at(&before, cluster.chunkserver_address(0)));

    cluster.kill_chunkserver(0);
    client.write("/f", 100, &pattern(100, 2)).await.unwrap();
    client.write("/f", 0, &pattern(50, 3)).await.unwrap();

    let mut expected = pattern(50, 3);
    expected.extend_from_slice(&pattern(100, 1)[50..]);
    expected.extend_from_slice(&pattern(100, 2));
    assert_eq!(client.read("/f", 0, 200).await.unwrap(), expected);

    sleep(Duration::from_millis(1500)).await;
    let after = cluster.replicas_of("/f", 0).await;
    assert_eq!(after.len(), 2);
    assert!(!has_replica_at(&after, cluster.chunkserver_address(0)));

    cluster.restart_chunkserver(0).await;
    sleep(Duration::from_millis(1000)).await;
    let restarted = cluster.replicas_of("/f", 0).await;
    assert_eq!(restarted.len(), 2);
    assert!(!has_replica_at(&restarted, cluster.chunkserver_address(0)));

    assert_eq!(client.read("/f", 0, 200).await.unwrap(), expected);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn master_restart_keeps_metadata() {
    let mut cluster = LocalCluster::start(3, &[]).await;
    let client = cluster.client();
    let data = pattern((1 << 20) + 500, 11);
    client.create("/keep/a").await.unwrap();
    client.create("/keep/b").await.unwrap();
    client.write("/keep/a", 0, &data).await.unwrap();
    client.rename("/keep/b", "/keep/c").await.unwrap();

    cluster.kill_master();
    cluster.restart_master().await;

    let fresh = cluster.client();
    assert_eq!(names(&fresh.list("/keep", false).await.unwrap()), vec!["a", "c"]);
    assert_eq!(fresh.read("/keep/a", 0, data.len() as u64).await.unwrap(), data);
    fresh.write("/keep/a", 10, b"after-restart").await.unwrap();
    assert_eq!(fresh.read("/keep/a", 10, 13).await.unwrap(), b"after-restart");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn checkpoint_is_written_and_loaded() {
    let mut cluster = LocalCluster::start(3, &[("checkpoint_log_threshold", "1K")]).await;
    let client = cluster.client();
    for i in 0..80 {
        client.create(&format!("/many/file{i}")).await.unwrap();
    }
    client.write("/many/file3", 0, b"payload").await.unwrap();
    sleep(Duration::from_millis(500)).await;
    let checkpoint_seen = fs::read_dir(cluster.master_data_dir())
        .unwrap()
        .flatten()
        .any(|e| e.file_name().to_string_lossy().starts_with("checkpoint.") && e.path().extension().is_none_or(|x| x != "tmp"));
    assert!(checkpoint_seen);

    cluster.kill_master();
    cluster.restart_master().await;
    let fresh = cluster.client();
    assert_eq!(fresh.list("/many", false).await.unwrap().len(), 80);
    assert_eq!(fresh.read("/many/file3", 0, 7).await.unwrap(), b"payload");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn snapshot_isolates_writes() {
    let cluster = LocalCluster::start(3, &[]).await;
    let client = cluster.client();
    client.create("/src").await.unwrap();
    client.write("/src", 0, b"AAAA").await.unwrap();
    client.snapshot("/src", "/snap").await.unwrap();

    client.write("/src", 0, b"BBBB").await.unwrap();
    assert_eq!(client.read("/snap", 0, 4).await.unwrap(), b"AAAA");
    assert_eq!(client.read("/src", 0, 4).await.unwrap(), b"BBBB");

    client.write("/snap", 0, b"CCCC").await.unwrap();
    assert_eq!(client.read("/src", 0, 4).await.unwrap(), b"BBBB");
    assert_eq!(client.read("/snap", 0, 4).await.unwrap(), b"CCCC");

    client.create("/d/x").await.unwrap();
    client.create("/d/y/z").await.unwrap();
    client.write("/d/x", 0, b"xx").await.unwrap();
    client.snapshot("/d", "/e").await.unwrap();
    assert_eq!(names(&client.list("/e", false).await.unwrap()), vec!["x", "y"]);
    assert_eq!(client.read("/e/x", 0, 2).await.unwrap(), b"xx");
    assert_eq!(client.snapshot("/d", "/e").await.unwrap_err().code, ErrorCode::AlreadyExists);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn delete_then_garbage_collection_removes_chunk_files() {
    let cluster = LocalCluster::start(3, &[]).await;
    let client = cluster.client();
    client.create("/gone").await.unwrap();
    client.write("/gone", 0, &pattern(4096, 5)).await.unwrap();
    assert_eq!(cluster.chunk_files_on_disk(), 3);
    client.remove("/gone").await.unwrap();
    sleep(Duration::from_millis(4500)).await;
    assert_eq!(cluster.chunk_files_on_disk(), 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rename_file_and_directory() {
    let cluster = LocalCluster::start(3, &[]).await;
    let client = cluster.client();
    client.create("/a/b").await.unwrap();
    client.write("/a/b", 0, b"data").await.unwrap();
    client.rename("/a/b", "/a/c").await.unwrap();
    client.rename("/a", "/z").await.unwrap();
    assert_eq!(client.read("/z/c", 0, 4).await.unwrap(), b"data");
    assert_eq!(client.open("/a/b").await.unwrap_err().code, ErrorCode::NotFound);
    assert_eq!(client.rename("/z/c", "/z/c").await.unwrap_err().code, ErrorCode::AlreadyExists);
}

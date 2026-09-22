mod support;

use std::fs::OpenOptions;
use std::os::unix::fs::FileExt;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use gfs::chunkserver::chunk_store::{ChunkStore, load_or_create_chunkserver_id};
use gfs::chunkserver::chunkserver::{Chunkserver, message_limit};
use gfs::chunkserver::service::ChunkserverService;
use gfs::common::config::Config;
use gfs::common::net::{bind, lazy_channel};
use gfs::rpc::chunkserver_client::ChunkserverClient;
use gfs::rpc::push_data_request::Payload;
use gfs::rpc::*;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio::time::sleep;
use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::{Channel, Server};

const CHUNK: u64 = 1 << 20;

fn test_config() -> Config {
    Config {
        chunk_size: CHUNK,
        checksum_block_size: 64 << 10,
        lease_duration: Duration::from_millis(1500),
        lease_clock_skew_margin: Duration::from_millis(100),
        heartbeat_interval: Duration::from_secs(60),
        rpc_deadline: Duration::from_millis(2000),
        client_rpc_deadline: Duration::from_millis(5000),
        data_buffer_capacity: 8 << 20,
        master_address: "127.0.0.1:1".to_string(),
        ..Config::default()
    }
}

fn pattern(n: usize, base: u8) -> Vec<u8> {
    (0..n).map(|i| base + (i % 19) as u8).collect()
}

struct TestServer {
    _dir: tempfile::TempDir,
    id: String,
    address: String,
    store: Arc<ChunkStore>,
    chunkserver: Arc<Chunkserver>,
    server: JoinHandle<()>,
    client: ChunkserverClient<Channel>,
}

impl TestServer {
    async fn start(rack: &str) -> TestServer {
        let dir = tempfile::tempdir().unwrap();
        let mut config = test_config();
        config.data_dir = dir.path().to_str().unwrap().to_string();
        config.rack = rack.to_string();
        let store = Arc::new(ChunkStore::new(dir.path(), config.chunk_size, config.checksum_block_size));
        store.scan();
        let id = load_or_create_chunkserver_id(dir.path());
        let chunkserver = Chunkserver::new(config.clone(), id.clone(), store.clone());
        let (incoming, addr) = bind("127.0.0.1:0").await.unwrap();
        let address = addr.to_string();
        chunkserver.set_advertise_address(address.clone());
        let service = ChunkserverService::server(chunkserver.clone());
        let server = tokio::spawn(async move {
            let _ = Server::builder().add_service(service).serve_with_incoming(incoming).await;
        });
        let limit = message_limit(config.chunk_size);
        let client = ChunkserverClient::new(lazy_channel(&address)).max_decoding_message_size(limit).max_encoding_message_size(limit);
        TestServer { _dir: dir, id, address, store, chunkserver, server, client }
    }

    fn replica(&self) -> Replica {
        Replica { chunkserver_id: self.id.clone(), address: self.address.clone(), rack: String::new() }
    }
}

impl Drop for TestServer {
    fn drop(&mut self) {
        self.server.abort();
        self.chunkserver.stop();
    }
}

struct Fixture {
    servers: Vec<TestServer>,
}

impl Fixture {
    async fn new() -> Fixture {
        let mut servers = Vec::new();
        for i in 0..3 {
            servers.push(TestServer::start(&format!("rack{}", i % 2)).await);
        }
        let fixture = Fixture { servers };
        for i in 0..3 {
            assert_eq!(fixture.create_chunk(i, 1, 1, 0).await, ResultCode::Ok);
        }
        fixture.grant(1, 2, 1500).await;
        fixture
    }

    async fn create_chunk(&self, server: usize, handle: u64, version: u64, copy_from: u64) -> ResultCode {
        self.servers[server]
            .client
            .clone()
            .create_chunk(CreateChunkRequest { handle, version, copy_from })
            .await
            .unwrap()
            .into_inner()
            .code()
    }

    async fn grant(&self, handle: u64, version: u64, lease_ms: u64) {
        let req = GrantLeaseRequest { handle, version, lease_ms, secondaries: vec![self.servers[1].replica(), self.servers[2].replica()] };
        let resp = self.servers[0].client.clone().grant_lease(req).await.unwrap().into_inner();
        assert_eq!(resp.code(), ResultCode::Ok);
        for i in 1..3 {
            let resp = self.servers[i].client.clone().update_version(UpdateVersionRequest { handle, version }).await.unwrap().into_inner();
            assert_eq!(resp.code(), ResultCode::Ok);
        }
    }

    async fn push_chain(&self, data: &[u8], sequence: u64, chain: &[usize]) -> PushDataResponse {
        let forward_to: Vec<Replica> = chain[1..].iter().map(|i| self.servers[*i].replica()).collect();
        self.push_to(&self.servers[chain[0]], data, sequence, forward_to).await
    }

    async fn push_to(&self, first: &TestServer, data: &[u8], sequence: u64, forward_to: Vec<Replica>) -> PushDataResponse {
        let (tx, rx) = mpsc::channel(8);
        let mut client = first.client.clone();
        let call = tokio::spawn(async move { client.push_data(ReceiverStream::new(rx)).await });
        let header = PushHeader { client_id: "client-a".to_string(), sequence, total_length: data.len() as u64, forward_to };
        tx.send(PushDataRequest { payload: Some(Payload::Header(header)) }).await.unwrap();
        for frame in data.chunks(30000) {
            tx.send(PushDataRequest { payload: Some(Payload::Data(frame.to_vec())) }).await.unwrap();
        }
        drop(tx);
        call.await.unwrap().unwrap().into_inner()
    }

    async fn push(&self, data: &[u8], sequence: u64) -> PushDataResponse {
        self.push_chain(data, sequence, &[0, 1, 2]).await
    }

    async fn write(&self, server: usize, handle: u64, version: u64, offset: u64, sequence: u64) -> WriteResponse {
        let req = WriteRequest { handle, version, offset, client_id: "client-a".to_string(), sequence };
        self.servers[server].client.clone().write(req).await.unwrap().into_inner()
    }

    async fn append(&self, handle: u64, version: u64, sequence: u64) -> RecordAppendResponse {
        let req = RecordAppendRequest { handle, version, client_id: "client-a".to_string(), sequence };
        self.servers[0].client.clone().record_append(req).await.unwrap().into_inner()
    }

    async fn read(&self, server: usize, handle: u64, version: u64, offset: u64, length: u64) -> ReadResponse {
        let req = ReadRequest { handle, version, offset, length };
        self.servers[server].client.clone().read(req).await.unwrap().into_inner()
    }

    async fn length(&self, server: usize, handle: u64) -> u64 {
        let resp = self.servers[server].client.clone().get_chunk_length(GetChunkLengthRequest { handle }).await.unwrap().into_inner();
        assert_eq!(resp.code(), ResultCode::Ok);
        resp.length
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn push_along_chain_then_write_reaches_every_replica() {
    let f = Fixture::new().await;
    let data = pattern(100 * 1024, b'a');
    assert_eq!(f.push(&data, 1).await.code(), ResultCode::Ok);
    let written = f.write(0, 1, 2, 0, 1).await;
    assert_eq!(written.code(), ResultCode::Ok, "failed at {}", written.failed_at);
    for i in 0..3 {
        let got = f.read(i, 1, 2, 0, data.len() as u64).await;
        assert_eq!(got.code(), ResultCode::Ok, "replica {i}");
        assert_eq!(got.data, data, "replica {i}");
        assert_eq!(f.length(i, 1).await, data.len() as u64);
    }
    let older = f.read(1, 1, 1, 10, 5).await;
    assert_eq!(older.code(), ResultCode::Ok);
    assert_eq!(older.data, data[10..15]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn record_append_assigns_offsets_and_pads_at_the_boundary() {
    let f = Fixture::new().await;
    let first = pattern(100 * 1024, b'b');
    assert_eq!(f.push(&first, 10).await.code(), ResultCode::Ok);
    let a1 = f.append(1, 2, 10).await;
    assert_eq!(a1.code(), ResultCode::Ok, "failed at {}", a1.failed_at);
    assert_eq!(a1.offset, 0);

    let second = pattern(200 * 1024, b'c');
    assert_eq!(f.push(&second, 11).await.code(), ResultCode::Ok);
    let a2 = f.append(1, 2, 11).await;
    assert_eq!(a2.code(), ResultCode::Ok, "failed at {}", a2.failed_at);
    assert_eq!(a2.offset, first.len() as u64);

    let big = pattern(900 * 1024, b'd');
    assert_eq!(f.push(&big, 12).await.code(), ResultCode::Ok);
    let a3 = f.append(1, 2, 12).await;
    assert_eq!(a3.code(), ResultCode::RetryNextChunk);
    for i in 0..3 {
        assert_eq!(f.length(i, 1).await, CHUNK, "replica {i}");
        let got = f.read(i, 1, 2, first.len() as u64, second.len() as u64).await;
        assert_eq!(got.code(), ResultCode::Ok);
        assert_eq!(got.data, second);
        let tail = f.read(i, 1, 2, CHUNK - 8, 8).await;
        assert_eq!(tail.code(), ResultCode::Ok);
        assert_eq!(tail.data, vec![0u8; 8]);
    }
    assert_eq!(f.push(b"tiny", 13).await.code(), ResultCode::Ok);
    assert_eq!(f.append(1, 2, 13).await.code(), ResultCode::RetryNextChunk);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rejects_missing_data_wrong_version_and_non_primary() {
    let f = Fixture::new().await;
    assert_eq!(f.write(0, 1, 2, 0, 999).await.code(), ResultCode::DataMissing);
    assert_eq!(f.push(b"data", 20).await.code(), ResultCode::Ok);
    assert_eq!(f.write(0, 1, 1, 0, 20).await.code(), ResultCode::StaleVersion);
    assert_eq!(f.write(1, 1, 2, 0, 20).await.code(), ResultCode::NotPrimary);
    assert_eq!(f.write(0, 42, 2, 0, 20).await.code(), ResultCode::NoSuchChunk);
    assert_eq!(f.write(0, 1, 2, CHUNK - 2, 20).await.code(), ResultCode::OutOfRange);
    assert_eq!(f.write(0, 1, 2, 0, 20).await.code(), ResultCode::Ok);
    assert_eq!(f.read(2, 1, 5, 0, 4).await.code(), ResultCode::StaleVersion);
    assert_eq!(f.read(2, 7, 2, 0, 4).await.code(), ResultCode::NoSuchChunk);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn secondary_with_newer_version_fences_the_old_primary() {
    let f = Fixture::new().await;
    let resp = f.servers[1].client.clone().update_version(UpdateVersionRequest { handle: 1, version: 3 }).await.unwrap().into_inner();
    assert_eq!(resp.code(), ResultCode::Ok);
    assert_eq!(f.push(b"fenced", 30).await.code(), ResultCode::Ok);
    let written = f.write(0, 1, 2, 0, 30).await;
    assert_eq!(written.code(), ResultCode::Failed);
    assert_eq!(written.failed_at, f.servers[1].id);
    assert_eq!(f.read(0, 1, 2, 0, 6).await.data, b"fenced");
    let untouched = f.read(1, 1, 3, 0, 6).await;
    assert_eq!(untouched.code(), ResultCode::Ok);
    assert!(untouched.data.is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn lease_expiry_revoke_and_regrant() {
    let f = Fixture::new().await;
    assert_eq!(f.push(b"one", 40).await.code(), ResultCode::Ok);
    assert_eq!(f.write(0, 1, 2, 0, 40).await.code(), ResultCode::Ok);
    sleep(Duration::from_millis(1600)).await;
    assert_eq!(f.push(b"two", 41).await.code(), ResultCode::Ok);
    assert_eq!(f.write(0, 1, 2, 0, 41).await.code(), ResultCode::LeaseExpired);

    f.grant(1, 3, 1500).await;
    assert_eq!(f.push(b"three", 42).await.code(), ResultCode::Ok);
    assert_eq!(f.write(0, 1, 3, 0, 42).await.code(), ResultCode::Ok);
    let resp = f.servers[0].client.clone().revoke_lease(RevokeLeaseRequest { handle: 1 }).await.unwrap().into_inner();
    assert_eq!(resp.code(), ResultCode::Ok);
    assert_eq!(f.push(b"four", 43).await.code(), ResultCode::Ok);
    assert_eq!(f.write(0, 1, 3, 0, 43).await.code(), ResultCode::NotPrimary);
    assert_eq!(f.read(2, 1, 3, 0, 5).await.data, b"three");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn corrupt_replica_reports_checksum_mismatch() {
    let f = Fixture::new().await;
    let data = pattern(70000, b'k');
    assert_eq!(f.push(&data, 50).await.code(), ResultCode::Ok);
    assert_eq!(f.write(0, 1, 2, 0, 50).await.code(), ResultCode::Ok);
    let path = f.servers[1].store.chunk_path(1);
    let file = OpenOptions::new().write(true).open(Path::new(&path)).unwrap();
    file.write_all_at(b"X", 100).unwrap();
    drop(file);
    assert_eq!(f.read(1, 1, 2, 0, 200).await.code(), ResultCode::ChecksumMismatch);
    assert_eq!(f.read(1, 1, 2, 65536, 100).await.code(), ResultCode::Ok);
    assert_eq!(f.read(0, 1, 2, 0, 200).await.code(), ResultCode::Ok);
    assert_eq!(f.servers[1].store.corrupt_handles(), vec![1]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn push_chain_breaks_at_a_dead_hop() {
    let f = Fixture::new().await;
    let dead = Replica { chunkserver_id: "dead-server".to_string(), address: "127.0.0.1:1".to_string(), rack: String::new() };
    let resp = f.push_to(&f.servers[0], b"data", 60, vec![f.servers[1].replica(), dead]).await;
    assert_eq!(resp.code(), ResultCode::Failed);
    assert_eq!(resp.failed_at, "dead-server");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn create_chunk_is_idempotent_and_copies_locally() {
    let f = Fixture::new().await;
    assert_eq!(f.create_chunk(0, 1, 2, 0).await, ResultCode::Ok);
    assert_eq!(f.create_chunk(0, 1, 9, 0).await, ResultCode::Failed);
    assert_eq!(f.push(b"copied", 70).await.code(), ResultCode::Ok);
    assert_eq!(f.write(0, 1, 2, 0, 70).await.code(), ResultCode::Ok);
    assert_eq!(f.create_chunk(0, 2, 1, 1).await, ResultCode::Ok);
    assert_eq!(f.read(0, 2, 1, 0, 6).await.data, b"copied");
    assert_eq!(f.create_chunk(0, 3, 1, 77).await, ResultCode::NoSuchChunk);
}

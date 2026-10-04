mod support;

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicI32, AtomicU64, Ordering};
use std::time::Duration;

use gfs::client::{Client, ErrorCode};
use gfs::common::config::Config;
use gfs::common::net::{bind, lazy_channel};
use gfs::common::paths::{ancestors_of, child_prefix, is_hidden_path, is_valid_path};
use gfs::rpc::chunkserver_client::ChunkserverClient;
use gfs::rpc::chunkserver_server::{Chunkserver, ChunkserverServer};
use gfs::rpc::master_server::{Master, MasterServer};
use gfs::rpc::push_data_request::Payload;
use gfs::rpc::*;
use parking_lot::Mutex;
use tokio::task::JoinHandle;
use tonic::transport::Server;
use tonic::{Request, Response, Status, Streaming};

use support::pattern;

const FAKE_CHUNK_SIZE: u64 = 64 * 1024;
const FAKE_MAX_APPEND: u64 = 16 * 1024;

fn ok<T>(value: T) -> Result<Response<T>, Status> {
    Ok(Response::new(value))
}

#[derive(Clone, Default)]
struct FakeChunk {
    version: u64,
    data: Vec<u8>,
}

struct ChunkserverInner {
    id: String,
    address: String,
    chunks: Mutex<BTreeMap<u64, FakeChunk>>,
    buffer: Mutex<BTreeMap<(String, u64), Vec<u8>>>,
    peers: Mutex<Vec<String>>,
    serial: AtomicU64,
    stale_reads: AtomicI32,
    drop_data: AtomicI32,
    pushes: AtomicI32,
}

impl ChunkserverInner {
    fn replica(&self) -> Replica {
        Replica { chunkserver_id: self.id.clone(), address: self.address.clone(), rack: String::new() }
    }

    fn create_chunk(&self, handle: u64, version: u64) {
        self.chunks.lock().insert(handle, FakeChunk { version, data: Vec::new() });
    }

    fn chunk_data(&self, handle: u64) -> Vec<u8> {
        self.chunks.lock().entry(handle).or_default().data.clone()
    }

    fn take_buffer(&self, client: &str, sequence: u64) -> Option<Vec<u8>> {
        let data = self.buffer.lock().remove(&(client.to_string(), sequence))?;
        if self.drop_data.load(Ordering::SeqCst) > 0 {
            self.drop_data.fetch_sub(1, Ordering::SeqCst);
            return None;
        }
        Some(data)
    }

    fn apply(&self, handle: u64, version: u64, kind: MutationKind, offset: u64, data: &[u8]) -> ResultCode {
        let mut chunks = self.chunks.lock();
        let Some(chunk) = chunks.get_mut(&handle) else { return ResultCode::NoSuchChunk };
        if chunk.version != version {
            return ResultCode::StaleVersion;
        }
        if kind == MutationKind::Pad {
            chunk.data.resize(FAKE_CHUNK_SIZE as usize, 0);
            return ResultCode::Ok;
        }
        let end = offset as usize + data.len();
        if end as u64 > FAKE_CHUNK_SIZE {
            return ResultCode::OutOfRange;
        }
        if chunk.data.len() < end {
            chunk.data.resize(end, 0);
        }
        chunk.data[offset as usize..end].copy_from_slice(data);
        ResultCode::Ok
    }

    async fn forward(&self, handle: u64, version: u64, kind: MutationKind, offset: u64, client: &str, sequence: u64) -> ResultCode {
        let peers = self.peers.lock().clone();
        for peer in peers {
            let mut stub = ChunkserverClient::new(lazy_channel(&peer));
            let req = ApplyMutationRequest {
                handle,
                version,
                serial: self.serial.fetch_add(1, Ordering::SeqCst) + 1,
                kind: kind as i32,
                offset,
                client_id: client.to_string(),
                sequence,
            };
            match stub.apply_mutation(req).await {
                Ok(resp) if resp.get_ref().code() == ResultCode::Ok => {}
                _ => return ResultCode::Failed,
            }
        }
        ResultCode::Ok
    }
}

#[derive(Clone)]
struct FakeChunkserverService(Arc<ChunkserverInner>);

#[tonic::async_trait]
impl Chunkserver for FakeChunkserverService {
    async fn push_data(&self, request: Request<Streaming<PushDataRequest>>) -> Result<Response<PushDataResponse>, Status> {
        let mut stream = request.into_inner();
        let header = match stream.message().await {
            Ok(Some(PushDataRequest { payload: Some(Payload::Header(header)) })) => header,
            _ => return ok(PushDataResponse { code: ResultCode::Failed as i32, failed_at: String::new() }),
        };
        let mut data = Vec::new();
        while let Ok(Some(message)) = stream.message().await {
            if let Some(Payload::Data(bytes)) = message.payload {
                data.extend_from_slice(&bytes);
            }
        }
        self.0.pushes.fetch_add(1, Ordering::SeqCst);
        self.0.buffer.lock().insert((header.client_id.clone(), header.sequence), data.clone());
        if let Some(next) = header.forward_to.first() {
            let mut stub = ChunkserverClient::new(lazy_channel(&next.address));
            let forwarded = PushHeader { forward_to: header.forward_to[1..].to_vec(), ..header.clone() };
            let messages =
                vec![PushDataRequest { payload: Some(Payload::Header(forwarded)) }, PushDataRequest { payload: Some(Payload::Data(data)) }];
            match stub.push_data(tokio_stream::iter(messages)).await {
                Err(_) => return ok(PushDataResponse { code: ResultCode::Failed as i32, failed_at: next.chunkserver_id.clone() }),
                Ok(resp) if resp.get_ref().code() != ResultCode::Ok => return ok(resp.into_inner()),
                Ok(_) => {}
            }
        }
        ok(PushDataResponse { code: ResultCode::Ok as i32, failed_at: String::new() })
    }

    async fn read(&self, request: Request<ReadRequest>) -> Result<Response<ReadResponse>, Status> {
        let req = request.into_inner();
        if self.0.stale_reads.load(Ordering::SeqCst) > 0 {
            self.0.stale_reads.fetch_sub(1, Ordering::SeqCst);
            return ok(ReadResponse { code: ResultCode::StaleVersion as i32, data: Vec::new() });
        }
        let chunks = self.0.chunks.lock();
        let Some(chunk) = chunks.get(&req.handle) else {
            return ok(ReadResponse { code: ResultCode::NoSuchChunk as i32, data: Vec::new() });
        };
        if chunk.version < req.version {
            return ok(ReadResponse { code: ResultCode::StaleVersion as i32, data: Vec::new() });
        }
        let data = if (req.offset as usize) < chunk.data.len() {
            let end = (req.offset + req.length).min(chunk.data.len() as u64) as usize;
            chunk.data[req.offset as usize..end].to_vec()
        } else {
            Vec::new()
        };
        ok(ReadResponse { code: ResultCode::Ok as i32, data })
    }

    async fn write(&self, request: Request<WriteRequest>) -> Result<Response<WriteResponse>, Status> {
        let req = request.into_inner();
        let Some(data) = self.0.take_buffer(&req.client_id, req.sequence) else {
            return ok(WriteResponse { code: ResultCode::DataMissing as i32, failed_at: String::new() });
        };
        let code = self.0.apply(req.handle, req.version, MutationKind::Write, req.offset, &data);
        if code != ResultCode::Ok {
            return ok(WriteResponse { code: code as i32, failed_at: String::new() });
        }
        let code = self.0.forward(req.handle, req.version, MutationKind::Write, req.offset, &req.client_id, req.sequence).await;
        ok(WriteResponse { code: code as i32, failed_at: String::new() })
    }

    async fn record_append(&self, request: Request<RecordAppendRequest>) -> Result<Response<RecordAppendResponse>, Status> {
        let req = request.into_inner();
        let Some(data) = self.0.take_buffer(&req.client_id, req.sequence) else {
            return ok(RecordAppendResponse { code: ResultCode::DataMissing as i32, offset: 0, failed_at: String::new() });
        };
        let (offset, pad) = {
            let chunks = self.0.chunks.lock();
            let Some(chunk) = chunks.get(&req.handle) else {
                return ok(RecordAppendResponse { code: ResultCode::NoSuchChunk as i32, offset: 0, failed_at: String::new() });
            };
            if chunk.version != req.version {
                return ok(RecordAppendResponse { code: ResultCode::StaleVersion as i32, offset: 0, failed_at: String::new() });
            }
            let offset = chunk.data.len() as u64;
            (offset, offset + data.len() as u64 > FAKE_CHUNK_SIZE)
        };
        let kind = if pad { MutationKind::Pad } else { MutationKind::Write };
        self.0.apply(req.handle, req.version, kind, offset, &data);
        let code = self.0.forward(req.handle, req.version, kind, offset, &req.client_id, req.sequence).await;
        if code != ResultCode::Ok {
            return ok(RecordAppendResponse { code: code as i32, offset: 0, failed_at: String::new() });
        }
        let code = if pad { ResultCode::RetryNextChunk } else { ResultCode::Ok };
        ok(RecordAppendResponse { code: code as i32, offset, failed_at: String::new() })
    }

    async fn get_chunk_length(&self, request: Request<GetChunkLengthRequest>) -> Result<Response<GetChunkLengthResponse>, Status> {
        let req = request.into_inner();
        let chunks = self.0.chunks.lock();
        match chunks.get(&req.handle) {
            Some(chunk) => ok(GetChunkLengthResponse { code: ResultCode::Ok as i32, length: chunk.data.len() as u64 }),
            None => ok(GetChunkLengthResponse { code: ResultCode::NoSuchChunk as i32, length: 0 }),
        }
    }

    async fn apply_mutation(&self, request: Request<ApplyMutationRequest>) -> Result<Response<ApplyMutationResponse>, Status> {
        let req = request.into_inner();
        let data = if req.kind() == MutationKind::Write {
            match self.0.take_buffer(&req.client_id, req.sequence) {
                Some(data) => data,
                None => return ok(ApplyMutationResponse { code: ResultCode::DataMissing as i32 }),
            }
        } else {
            Vec::new()
        };
        let code = self.0.apply(req.handle, req.version, req.kind(), req.offset, &data);
        ok(ApplyMutationResponse { code: code as i32 })
    }

    async fn create_chunk(&self, request: Request<CreateChunkRequest>) -> Result<Response<CreateChunkResponse>, Status> {
        let req = request.into_inner();
        self.0.create_chunk(req.handle, req.version);
        ok(CreateChunkResponse { code: ResultCode::Ok as i32 })
    }

    async fn grant_lease(&self, _: Request<GrantLeaseRequest>) -> Result<Response<GrantLeaseResponse>, Status> {
        ok(GrantLeaseResponse { code: ResultCode::Ok as i32 })
    }

    async fn revoke_lease(&self, _: Request<RevokeLeaseRequest>) -> Result<Response<RevokeLeaseResponse>, Status> {
        ok(RevokeLeaseResponse { code: ResultCode::Ok as i32 })
    }

    async fn update_version(&self, _: Request<UpdateVersionRequest>) -> Result<Response<UpdateVersionResponse>, Status> {
        ok(UpdateVersionResponse { code: ResultCode::Ok as i32 })
    }
}

struct MasterInner {
    servers: Vec<Arc<ChunkserverInner>>,
    files: Mutex<BTreeMap<String, Vec<u64>>>,
    versions: Mutex<BTreeMap<u64, u64>>,
    next_handle: AtomicU64,
    lease_requests: AtomicI32,
    snapshot_delay_ms: AtomicU64,
}

impl MasterInner {
    fn replicas(&self) -> Vec<Replica> {
        self.servers.iter().map(|s| s.replica()).collect()
    }

    fn exists_locked(files: &BTreeMap<String, Vec<u64>>, path: &str) -> bool {
        if files.contains_key(path) {
            return true;
        }
        let prefix = child_prefix(path);
        files.range(prefix.clone()..).next().is_some_and(|(k, _)| k.starts_with(&prefix))
    }

    fn move_locked(&self, source: &str, target: &str, erase_source: bool) -> Result<(), Status> {
        let mut files = self.files.lock();
        if !is_valid_path(source) || !is_valid_path(target) {
            return Err(Status::invalid_argument("bad path"));
        }
        if Self::exists_locked(&files, target) {
            return Err(Status::already_exists("target exists"));
        }
        let mut moved = Vec::new();
        if let Some(handles) = files.get(source) {
            moved.push((target.to_string(), handles.clone()));
        } else {
            let prefix = child_prefix(source);
            for (path, handles) in files.range(prefix.clone()..).take_while(|(k, _)| k.starts_with(&prefix)) {
                moved.push((format!("{}{}", child_prefix(target), &path[prefix.len()..]), handles.clone()));
            }
            if moved.is_empty() {
                return Err(Status::not_found("no such file or directory"));
            }
        }
        if erase_source {
            files.remove(source);
            let prefix = child_prefix(source);
            files.retain(|path, _| !path.starts_with(&prefix));
        }
        for (path, handles) in moved {
            files.insert(path, handles);
        }
        Ok(())
    }

    fn chunk_info(&self, index: u64, handle: u64) -> ChunkInfo {
        ChunkInfo { index, handle, version: *self.versions.lock().get(&handle).unwrap_or(&1), replicas: self.replicas() }
    }
}

#[derive(Clone)]
struct FakeMasterService(Arc<MasterInner>);

#[tonic::async_trait]
impl Master for FakeMasterService {
    async fn get_cluster_info(&self, _: Request<GetClusterInfoRequest>) -> Result<Response<GetClusterInfoResponse>, Status> {
        ok(GetClusterInfoResponse { chunk_size: FAKE_CHUNK_SIZE, max_record_append_size: FAKE_MAX_APPEND })
    }

    async fn create(&self, request: Request<CreateRequest>) -> Result<Response<CreateResponse>, Status> {
        let req = request.into_inner();
        let mut files = self.0.files.lock();
        if !is_valid_path(&req.path) || req.path == "/" {
            return Err(Status::invalid_argument("bad path"));
        }
        if files.contains_key(&req.path) {
            return Err(Status::already_exists("exists"));
        }
        if ancestors_of(&req.path).iter().any(|a| files.contains_key(a)) {
            return Err(Status::invalid_argument("ancestor is a file"));
        }
        files.insert(req.path, Vec::new());
        ok(CreateResponse {})
    }

    async fn open(&self, request: Request<OpenRequest>) -> Result<Response<OpenResponse>, Status> {
        let req = request.into_inner();
        let files = self.0.files.lock();
        match files.get(&req.path) {
            Some(handles) => ok(OpenResponse { chunk_count: handles.len() as u64 }),
            None => Err(Status::not_found("no such file")),
        }
    }

    async fn delete(&self, request: Request<DeleteRequest>) -> Result<Response<DeleteResponse>, Status> {
        let req = request.into_inner();
        if self.0.files.lock().remove(&req.path).is_none() {
            return Err(Status::not_found("no such file"));
        }
        ok(DeleteResponse {})
    }

    async fn rename(&self, request: Request<RenameRequest>) -> Result<Response<RenameResponse>, Status> {
        let req = request.into_inner();
        self.0.move_locked(&req.source, &req.target, true)?;
        ok(RenameResponse {})
    }

    async fn snapshot(&self, request: Request<SnapshotRequest>) -> Result<Response<SnapshotResponse>, Status> {
        let req = request.into_inner();
        tokio::time::sleep(Duration::from_millis(self.0.snapshot_delay_ms.load(Ordering::SeqCst))).await;
        self.0.move_locked(&req.source, &req.target, false)?;
        ok(SnapshotResponse {})
    }

    async fn find_matching_files(&self, request: Request<FindMatchingFilesRequest>) -> Result<Response<FindMatchingFilesResponse>, Status> {
        let req = request.into_inner();
        let files = self.0.files.lock();
        let prefix = child_prefix(&req.directory);
        let mut seen: BTreeMap<String, bool> = BTreeMap::new();
        for (path, _) in files.range(prefix.clone()..).take_while(|(k, _)| k.starts_with(&prefix)) {
            let rest = &path[prefix.len()..];
            let (name, dir) = match rest.find('/') {
                Some(slash) => (&rest[..slash], true),
                None => (rest, false),
            };
            if !dir && !req.include_hidden && is_hidden_path(path) {
                continue;
            }
            let entry = seen.entry(name.to_string()).or_insert(false);
            *entry = *entry || dir;
        }
        let entries = seen.into_iter().map(|(name, is_directory)| DirEntry { name, is_directory }).collect();
        ok(FindMatchingFilesResponse { entries })
    }

    async fn find_location(&self, request: Request<FindLocationRequest>) -> Result<Response<FindLocationResponse>, Status> {
        let req = request.into_inner();
        let files = self.0.files.lock();
        let Some(handles) = files.get(&req.path) else { return Err(Status::not_found("no such file")) };
        let mut chunks = Vec::new();
        let mut index = req.first_index;
        while (index as usize) < handles.len() && index < req.first_index + req.count as u64 {
            chunks.push(self.0.chunk_info(index, handles[index as usize]));
            index += 1;
        }
        ok(FindLocationResponse { chunks })
    }

    async fn find_lease_holder(&self, request: Request<FindLeaseHolderRequest>) -> Result<Response<FindLeaseHolderResponse>, Status> {
        let req = request.into_inner();
        let files = self.0.files.lock();
        let Some(handles) = files.get(&req.path) else { return Err(Status::not_found("no such file")) };
        let Some(&handle) = handles.get(req.index as usize) else { return Err(Status::not_found("no such chunk index")) };
        self.0.lease_requests.fetch_add(1, Ordering::SeqCst);
        let replicas = self.0.replicas();
        ok(FindLeaseHolderResponse {
            code: ResultCode::Ok as i32,
            handle,
            version: *self.0.versions.lock().get(&handle).unwrap_or(&1),
            primary: replicas.first().cloned(),
            secondaries: replicas[1..].to_vec(),
        })
    }

    async fn add_chunk(&self, request: Request<AddChunkRequest>) -> Result<Response<AddChunkResponse>, Status> {
        let req = request.into_inner();
        let mut files = self.0.files.lock();
        let Some(handles) = files.get_mut(&req.path) else { return Err(Status::not_found("no such file")) };
        if req.index as usize > handles.len() {
            return Err(Status::invalid_argument("chunk index leaves a hole"));
        }
        if req.index as usize == handles.len() {
            let handle = self.0.next_handle.fetch_add(1, Ordering::SeqCst);
            self.0.versions.lock().insert(handle, 1);
            for server in &self.0.servers {
                server.create_chunk(handle, 1);
            }
            handles.push(handle);
        }
        let handle = handles[req.index as usize];
        ok(AddChunkResponse { code: ResultCode::Ok as i32, chunk: Some(self.0.chunk_info(req.index, handle)) })
    }

    async fn heart_beat(&self, _: Request<HeartBeatRequest>) -> Result<Response<HeartBeatResponse>, Status> {
        ok(HeartBeatResponse::default())
    }
}

struct FakeCluster {
    servers: Vec<Arc<ChunkserverInner>>,
    master: Arc<MasterInner>,
    master_address: String,
    tasks: Vec<JoinHandle<()>>,
}

impl FakeCluster {
    async fn start(count: usize) -> FakeCluster {
        let mut servers = Vec::new();
        let mut tasks = Vec::new();
        for i in 0..count {
            let (incoming, addr) = bind("127.0.0.1:0").await.unwrap();
            let inner = Arc::new(ChunkserverInner {
                id: format!("cs{i}"),
                address: addr.to_string(),
                chunks: Mutex::new(BTreeMap::new()),
                buffer: Mutex::new(BTreeMap::new()),
                peers: Mutex::new(Vec::new()),
                serial: AtomicU64::new(0),
                stale_reads: AtomicI32::new(0),
                drop_data: AtomicI32::new(0),
                pushes: AtomicI32::new(0),
            });
            let service = ChunkserverServer::new(FakeChunkserverService(inner.clone()));
            tasks.push(tokio::spawn(async move {
                let _ = Server::builder().add_service(service).serve_with_incoming(incoming).await;
            }));
            servers.push(inner);
        }
        for (i, server) in servers.iter().enumerate() {
            let peers = servers.iter().enumerate().filter(|(j, _)| *j != i).map(|(_, s)| s.address.clone()).collect();
            *server.peers.lock() = peers;
        }
        let master = Arc::new(MasterInner {
            servers: servers.clone(),
            files: Mutex::new(BTreeMap::new()),
            versions: Mutex::new(BTreeMap::new()),
            next_handle: AtomicU64::new(1),
            lease_requests: AtomicI32::new(0),
            snapshot_delay_ms: AtomicU64::new(0),
        });
        let (incoming, addr) = bind("127.0.0.1:0").await.unwrap();
        let service = MasterServer::new(FakeMasterService(master.clone()));
        tasks.push(tokio::spawn(async move {
            let _ = Server::builder().add_service(service).serve_with_incoming(incoming).await;
        }));
        FakeCluster { servers, master, master_address: addr.to_string(), tasks }
    }

    fn client_config(&self) -> Config {
        Config {
            master_address: self.master_address.clone(),
            client_rpc_deadline: Duration::from_millis(2000),
            lease_duration: Duration::from_millis(1000),
            lease_clock_skew_margin: Duration::from_millis(100),
            retry_backoff_base: Duration::from_millis(10),
            outer_retry_delay: Duration::from_millis(30),
            mutation_retry_inner: 3,
            mutation_retry_outer: 2,
            client_location_cache_ttl: Duration::from_millis(5000),
            push_frame_size: 8 * 1024,
            ..Config::default()
        }
    }

    fn client(&self) -> Client {
        Client::new(self.client_config())
    }
}

impl Drop for FakeCluster {
    fn drop(&mut self) {
        for task in &self.tasks {
            task.abort();
        }
    }
}

fn names(entries: &[gfs::client::DirEntry]) -> Vec<String> {
    entries.iter().map(|e| format!("{}{}", e.name, if e.is_directory { "/" } else { "" })).collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn constructor_does_not_contact_master() {
    let config = Config { master_address: "127.0.0.1:1".to_string(), client_rpc_deadline: Duration::from_millis(300), ..Config::default() };
    let client = Client::new(config);
    assert_eq!(client.client_id().len(), 32);
    assert_eq!(client.chunk_size().await, 0);
    assert_eq!(client.create("/x").await.unwrap_err().code, ErrorCode::Unavailable);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn write_across_chunk_boundary_reads_back() {
    let cluster = FakeCluster::start(3).await;
    let client = cluster.client();
    assert_eq!(client.chunk_size().await, FAKE_CHUNK_SIZE);
    let data = pattern((FAKE_CHUNK_SIZE * 2 + 500) as usize, 3);
    client.create("/big").await.unwrap();
    client.write("/big", 0, &data).await.unwrap();

    assert_eq!(client.open("/big").await.unwrap().chunk_count, 3);
    assert_eq!(client.read("/big", 0, data.len() as u64 + 100).await.unwrap(), data);
    let at = (FAKE_CHUNK_SIZE - 7) as usize;
    assert_eq!(client.read("/big", at as u64, 15).await.unwrap(), data[at..at + 15]);

    client.write("/big", FAKE_CHUNK_SIZE + 10, b"hello").await.unwrap();
    let at = (FAKE_CHUNK_SIZE + 8) as usize;
    let mut expected = data[at..at + 2].to_vec();
    expected.extend_from_slice(b"hello");
    expected.extend_from_slice(&data[at + 7..at + 9]);
    assert_eq!(client.read("/big", at as u64, 9).await.unwrap(), expected);

    for server in &cluster.servers {
        assert_eq!(server.chunk_data(1), data[..FAKE_CHUNK_SIZE as usize]);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn write_beyond_current_chunks_creates_them_in_order() {
    let cluster = FakeCluster::start(3).await;
    let client = cluster.client();
    client.create("/sparse").await.unwrap();
    client.write("/sparse", FAKE_CHUNK_SIZE * 2 + 100, b"tail").await.unwrap();
    assert_eq!(client.open("/sparse").await.unwrap().chunk_count, 3);
    assert_eq!(client.read("/sparse", FAKE_CHUNK_SIZE * 2 + 100, 4).await.unwrap(), b"tail");
    client.write("/sparse", 5, b"").await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn record_append_offsets_increase_and_pad_at_boundary() {
    let cluster = FakeCluster::start(3).await;
    let client = cluster.client();
    client.create("/log").await.unwrap();
    let record = 15 * 1024;
    let mut offsets = Vec::new();
    for i in 0..6u32 {
        offsets.push(client.record_append("/log", &pattern(record, i)).await.unwrap());
    }
    for (i, offset) in offsets.iter().take(4).enumerate() {
        assert_eq!(*offset, (record * i) as u64);
    }
    assert_eq!(offsets[4], FAKE_CHUNK_SIZE);
    assert_eq!(offsets[5], FAKE_CHUNK_SIZE + record as u64);
    for i in 0..6u32 {
        assert_eq!(client.read("/log", offsets[i as usize], record as u64).await.unwrap(), pattern(record, i), "record {i}");
    }
    assert_eq!(client.length("/log").await.unwrap(), FAKE_CHUNK_SIZE + 2 * record as u64);
    client.record_append("/log", b"").await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn record_append_over_limit_is_rejected() {
    let cluster = FakeCluster::start(3).await;
    let client = cluster.client();
    client.create("/log").await.unwrap();
    assert_eq!(client.record_append("/log", &pattern(FAKE_MAX_APPEND as usize + 1, 1)).await.unwrap_err().code, ErrorCode::InvalidArgument);
    assert_eq!(client.record_append("/missing", b"x").await.unwrap_err().code, ErrorCode::NotFound);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn read_past_end_is_short() {
    let cluster = FakeCluster::start(3).await;
    let client = cluster.client();
    client.create("/short").await.unwrap();
    client.write("/short", 0, &pattern(100, 9)).await.unwrap();
    assert_eq!(client.read("/short", 50, 1000).await.unwrap(), pattern(100, 9)[50..]);
    assert!(client.read("/short", 200, 10).await.unwrap().is_empty());
    assert!(client.read("/short", FAKE_CHUNK_SIZE * 5, 10).await.unwrap().is_empty());
    assert!(client.read("/short", 0, 0).await.unwrap().is_empty());
    client.create("/empty").await.unwrap();
    assert!(client.read("/empty", 0, 10).await.unwrap().is_empty());
    assert_eq!(client.length("/empty").await.unwrap(), 0);
    assert_eq!(client.length("/short").await.unwrap(), 100);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn stale_replica_falls_over_to_another() {
    let cluster = FakeCluster::start(3).await;
    let client = cluster.client();
    client.create("/f").await.unwrap();
    client.write("/f", 0, b"payload").await.unwrap();
    cluster.servers[0].stale_reads.store(1000, Ordering::SeqCst);
    cluster.servers[1].stale_reads.store(1000, Ordering::SeqCst);
    assert_eq!(client.read("/f", 0, 7).await.unwrap(), b"payload");
    cluster.servers[2].stale_reads.store(1000, Ordering::SeqCst);
    assert_eq!(client.read("/f", 0, 7).await.unwrap_err().code, ErrorCode::Unavailable);
    for server in &cluster.servers {
        server.stale_reads.store(0, Ordering::SeqCst);
    }
    assert_eq!(client.read("/f", 0, 7).await.unwrap(), b"payload");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn data_missing_triggers_repush() {
    let cluster = FakeCluster::start(3).await;
    let client = cluster.client();
    client.create("/f").await.unwrap();
    cluster.servers[0].drop_data.store(1, Ordering::SeqCst);
    client.write("/f", 0, b"first").await.unwrap();
    assert_eq!(cluster.servers[0].drop_data.load(Ordering::SeqCst), 0);
    assert!(cluster.servers[0].pushes.load(Ordering::SeqCst) >= 2);
    cluster.servers[0].drop_data.store(1, Ordering::SeqCst);
    assert_eq!(client.record_append("/f", b"second").await.unwrap(), 5);
    assert_eq!(client.read("/f", 0, 11).await.unwrap(), b"firstsecond");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn lease_holder_is_cached_across_writes() {
    let cluster = FakeCluster::start(3).await;
    let client = cluster.client();
    client.create("/f").await.unwrap();
    client.write("/f", 0, b"a").await.unwrap();
    client.write("/f", 1, b"b").await.unwrap();
    client.write("/f", 2, b"c").await.unwrap();
    assert_eq!(cluster.master.lease_requests.load(Ordering::SeqCst), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn status_mapping() {
    let cluster = FakeCluster::start(3).await;
    let client = cluster.client();
    assert_eq!(client.open("/nope").await.unwrap_err().code, ErrorCode::NotFound);
    assert_eq!(client.read("/nope", 0, 1).await.unwrap_err().code, ErrorCode::NotFound);
    assert_eq!(client.write("/nope", 0, b"x").await.unwrap_err().code, ErrorCode::NotFound);
    assert_eq!(client.remove("/nope").await.unwrap_err().code, ErrorCode::NotFound);
    assert_eq!(client.create("relative").await.unwrap_err().code, ErrorCode::InvalidArgument);
    client.create("/a").await.unwrap();
    assert_eq!(client.create("/a").await.unwrap_err().code, ErrorCode::AlreadyExists);
    assert_eq!(client.create("/a/b").await.unwrap_err().code, ErrorCode::InvalidArgument);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn snapshot_waits_out_the_masters_lease_wait() {
    let cluster = FakeCluster::start(3).await;
    let client = Client::new(Config { client_rpc_deadline: Duration::from_millis(300), ..cluster.client_config() });
    assert!(client.create("/a/b").await.is_ok());
    cluster.master.snapshot_delay_ms.store(800, Ordering::SeqCst);
    assert!(client.snapshot("/a", "/s").await.is_ok());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn list_rename_snapshot_round_trip() {
    let cluster = FakeCluster::start(3).await;
    let client = cluster.client();
    client.create("/a/b").await.unwrap();
    client.create("/a/c").await.unwrap();
    client.create("/a/d/e").await.unwrap();
    client.write("/a/b", 0, b"bee").await.unwrap();
    assert_eq!(names(&client.list("/a", false).await.unwrap()), vec!["b", "c", "d/"]);

    client.rename("/a/b", "/a/bb").await.unwrap();
    client.snapshot("/a", "/s").await.unwrap();
    assert_eq!(names(&client.list("/s", false).await.unwrap()), vec!["bb", "c", "d/"]);
    assert_eq!(client.read("/s/bb", 0, 3).await.unwrap(), b"bee");
    assert_eq!(client.snapshot("/a", "/s").await.unwrap_err().code, ErrorCode::AlreadyExists);
    assert_eq!(client.rename("/zzz", "/y").await.unwrap_err().code, ErrorCode::NotFound);

    client.remove("/a/bb").await.unwrap();
    assert_eq!(names(&client.list("/a", false).await.unwrap()), vec!["c", "d/"]);
    assert_eq!(names(&client.list("/", false).await.unwrap()), vec!["a/", "s/"]);
}

mod support;

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::{Duration, Instant};

use gfs::common::config::Config;
use gfs::common::net::{bind, lazy_channel};
use gfs::master::checkpoint;
use gfs::master::master::Master;
use gfs::master::service::MasterService;
use gfs::rpc::chunkserver_server::{Chunkserver, ChunkserverServer};
use gfs::rpc::master_client::MasterClient;
use gfs::rpc::*;
use parking_lot::Mutex;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tokio::time::{sleep, timeout};
use tonic::transport::{Channel, Server};
use tonic::{Code, Request, Response, Status, Streaming};

use support::wait_until;

fn test_config(data_dir: &str) -> Config {
    Config {
        data_dir: data_dir.to_string(),
        heartbeat_interval: Duration::from_millis(100),
        chunkserver_dead_timeout: Duration::from_millis(400),
        lease_duration: Duration::from_millis(2000),
        lease_clock_skew_margin: Duration::from_millis(50),
        gc_interval: Duration::from_millis(200),
        deleted_file_retention: Duration::from_millis(1000),
        rpc_deadline: Duration::from_millis(500),
        log_flush_max_delay: Duration::from_millis(2),
        master_worker_threads: 4,
        ..Config::default()
    }
}

fn code<T>(result: Result<T, Status>) -> Code {
    result.err().map(|status| status.code()).unwrap_or(Code::Ok)
}

#[derive(Default)]
struct FakeState {
    chunks: BTreeMap<u64, u64>,
    held_leases: BTreeSet<u64>,
    creates: Vec<CreateChunkRequest>,
    grants: Vec<GrantLeaseRequest>,
    revokes: Vec<RevokeLeaseRequest>,
    updates: Vec<UpdateVersionRequest>,
}

struct FakeInner {
    id: String,
    rack: String,
    address: String,
    state: Mutex<FakeState>,
}

#[derive(Clone)]
struct FakeService(Arc<FakeInner>);

fn ok<T>(value: T) -> Result<Response<T>, Status> {
    Ok(Response::new(value))
}

#[tonic::async_trait]
impl Chunkserver for FakeService {
    async fn push_data(&self, _: Request<Streaming<PushDataRequest>>) -> Result<Response<PushDataResponse>, Status> {
        Err(Status::unimplemented("fake"))
    }

    async fn read(&self, _: Request<ReadRequest>) -> Result<Response<ReadResponse>, Status> {
        Err(Status::unimplemented("fake"))
    }

    async fn write(&self, _: Request<WriteRequest>) -> Result<Response<WriteResponse>, Status> {
        Err(Status::unimplemented("fake"))
    }

    async fn record_append(&self, _: Request<RecordAppendRequest>) -> Result<Response<RecordAppendResponse>, Status> {
        Err(Status::unimplemented("fake"))
    }

    async fn get_chunk_length(&self, _: Request<GetChunkLengthRequest>) -> Result<Response<GetChunkLengthResponse>, Status> {
        ok(GetChunkLengthResponse { code: ResultCode::NoSuchChunk as i32, length: 0 })
    }

    async fn apply_mutation(&self, _: Request<ApplyMutationRequest>) -> Result<Response<ApplyMutationResponse>, Status> {
        Err(Status::unimplemented("fake"))
    }

    async fn create_chunk(&self, request: Request<CreateChunkRequest>) -> Result<Response<CreateChunkResponse>, Status> {
        let req = request.into_inner();
        let mut state = self.0.state.lock();
        state.chunks.insert(req.handle, req.version);
        state.creates.push(req);
        ok(CreateChunkResponse { code: ResultCode::Ok as i32 })
    }

    async fn grant_lease(&self, request: Request<GrantLeaseRequest>) -> Result<Response<GrantLeaseResponse>, Status> {
        let req = request.into_inner();
        let mut state = self.0.state.lock();
        state.chunks.insert(req.handle, req.version);
        state.held_leases.insert(req.handle);
        state.grants.push(req);
        ok(GrantLeaseResponse { code: ResultCode::Ok as i32 })
    }

    async fn revoke_lease(&self, request: Request<RevokeLeaseRequest>) -> Result<Response<RevokeLeaseResponse>, Status> {
        let req = request.into_inner();
        let mut state = self.0.state.lock();
        state.held_leases.remove(&req.handle);
        state.revokes.push(req);
        ok(RevokeLeaseResponse { code: ResultCode::Ok as i32 })
    }

    async fn update_version(&self, request: Request<UpdateVersionRequest>) -> Result<Response<UpdateVersionResponse>, Status> {
        let req = request.into_inner();
        let mut state = self.0.state.lock();
        state.chunks.insert(req.handle, req.version);
        state.updates.push(req);
        ok(UpdateVersionResponse { code: ResultCode::Ok as i32 })
    }
}

struct FakeChunkserver {
    inner: Arc<FakeInner>,
    server: Option<JoinHandle<()>>,
    heartbeat: Option<JoinHandle<()>>,
}

impl FakeChunkserver {
    async fn start(id: &str, rack: &str) -> FakeChunkserver {
        let (incoming, addr) = bind("127.0.0.1:0").await.unwrap();
        let inner = Arc::new(FakeInner {
            id: id.to_string(),
            rack: rack.to_string(),
            address: addr.to_string(),
            state: Mutex::new(FakeState::default()),
        });
        let service = ChunkserverServer::new(FakeService(inner.clone()));
        let server = tokio::spawn(async move {
            let _ = Server::builder().add_service(service).serve_with_incoming(incoming).await;
        });
        FakeChunkserver { inner, server: Some(server), heartbeat: None }
    }

    fn id(&self) -> &str {
        &self.inner.id
    }

    async fn heartbeat_once(inner: &FakeInner, client: &mut MasterClient<Channel>) -> Option<HeartBeatResponse> {
        let mut req = HeartBeatRequest {
            chunkserver_id: inner.id.clone(),
            address: inner.address.clone(),
            rack: inner.rack.clone(),
            ..Default::default()
        };
        {
            let state = inner.state.lock();
            for (handle, version) in &state.chunks {
                req.chunks.push(ChunkReport { handle: *handle, version: *version, length: 0 });
            }
            req.lease_extension_requests = state.held_leases.iter().copied().collect();
        }
        let resp = timeout(Duration::from_millis(500), client.heart_beat(req)).await.ok()?.ok()?.into_inner();
        let mut state = inner.state.lock();
        for handle in &resp.delete_handles {
            state.chunks.remove(handle);
        }
        Some(resp)
    }

    fn start_heartbeats(&mut self, master_address: &str, interval: Duration) {
        self.stop_heartbeats();
        let inner = self.inner.clone();
        let mut client = MasterClient::new(lazy_channel(master_address));
        self.heartbeat = Some(tokio::spawn(async move {
            loop {
                Self::heartbeat_once(&inner, &mut client).await;
                sleep(interval).await;
            }
        }));
    }

    fn stop_heartbeats(&mut self) {
        if let Some(task) = self.heartbeat.take() {
            task.abort();
        }
    }

    fn set_version(&self, handle: u64, version: u64) {
        self.inner.state.lock().chunks.insert(handle, version);
    }

    fn holds(&self, handle: u64) -> bool {
        self.inner.state.lock().chunks.contains_key(&handle)
    }

    fn creates(&self) -> Vec<CreateChunkRequest> {
        self.inner.state.lock().creates.clone()
    }

    fn grants(&self) -> Vec<GrantLeaseRequest> {
        self.inner.state.lock().grants.clone()
    }

    fn revokes(&self) -> Vec<RevokeLeaseRequest> {
        self.inner.state.lock().revokes.clone()
    }

    fn updates(&self) -> Vec<UpdateVersionRequest> {
        self.inner.state.lock().updates.clone()
    }
}

impl Drop for FakeChunkserver {
    fn drop(&mut self) {
        self.stop_heartbeats();
        if let Some(server) = self.server.take() {
            server.abort();
        }
    }
}

struct MasterHarness {
    config: Config,
    address: String,
    master: Option<Arc<Master>>,
    server: Option<(oneshot::Sender<()>, JoinHandle<()>)>,
    client: MasterClient<Channel>,
}

impl MasterHarness {
    async fn start(config: Config) -> MasterHarness {
        let address = format!("127.0.0.1:{}", support::free_port());
        let client = MasterClient::new(lazy_channel(&address));
        let mut harness = MasterHarness { config, address, master: None, server: None, client };
        harness.boot().await;
        harness
    }

    async fn boot(&mut self) {
        let master = Master::new(self.config.clone());
        master.start();
        let (incoming, _) = bind(&self.address).await.unwrap();
        let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
        let service = MasterService::server(master.clone());
        let task = tokio::spawn(async move {
            let _ = Server::builder()
                .add_service(service)
                .serve_with_incoming_shutdown(incoming, async {
                    let _ = shutdown_rx.await;
                })
                .await;
        });
        self.master = Some(master);
        self.server = Some((shutdown_tx, task));
        self.client = MasterClient::new(lazy_channel(&self.address));
    }

    async fn stop(&mut self) {
        if let Some((shutdown, mut task)) = self.server.take() {
            let _ = shutdown.send(());
            if timeout(Duration::from_secs(5), &mut task).await.is_err() {
                task.abort();
            }
        }
        if let Some(master) = self.master.take() {
            master.stop();
        }
    }

    async fn restart(&mut self) {
        self.stop().await;
        self.boot().await;
    }

    fn master(&self) -> &Arc<Master> {
        self.master.as_ref().unwrap()
    }

    fn address(&self) -> &str {
        &self.address
    }

    async fn create(&self, path: &str) -> Result<CreateResponse, Status> {
        self.client.clone().create(CreateRequest { path: path.to_string() }).await.map(|r| r.into_inner())
    }

    async fn open(&self, path: &str) -> Result<u64, Status> {
        self.client.clone().open(OpenRequest { path: path.to_string() }).await.map(|r| r.into_inner().chunk_count)
    }

    async fn remove(&self, path: &str) -> Result<DeleteResponse, Status> {
        self.client.clone().delete(DeleteRequest { path: path.to_string() }).await.map(|r| r.into_inner())
    }

    async fn rename(&self, source: &str, target: &str) -> Result<RenameResponse, Status> {
        self.client.clone().rename(RenameRequest { source: source.to_string(), target: target.to_string() }).await.map(|r| r.into_inner())
    }

    async fn snapshot(&self, source: &str, target: &str) -> Result<SnapshotResponse, Status> {
        let mut request = Request::new(SnapshotRequest { source: source.to_string(), target: target.to_string() });
        request.set_timeout(Duration::from_secs(10));
        self.client.clone().snapshot(request).await.map(|r| r.into_inner())
    }

    async fn list(&self, directory: &str, include_hidden: bool) -> Result<Vec<String>, Status> {
        let resp = self
            .client
            .clone()
            .find_matching_files(FindMatchingFilesRequest { directory: directory.to_string(), include_hidden })
            .await?
            .into_inner();
        Ok(resp.entries.into_iter().map(|e| format!("{}{}", e.name, if e.is_directory { "/" } else { "" })).collect())
    }

    async fn add_chunk(&self, path: &str, index: u64) -> Result<AddChunkResponse, Status> {
        self.client.clone().add_chunk(AddChunkRequest { path: path.to_string(), index }).await.map(|r| r.into_inner())
    }

    async fn added_handle(&self, path: &str, index: u64) -> u64 {
        self.add_chunk(path, index).await.unwrap().chunk.unwrap().handle
    }

    async fn find_location(&self, path: &str, index: u64) -> Result<FindLocationResponse, Status> {
        self.client
            .clone()
            .find_location(FindLocationRequest { path: path.to_string(), first_index: index, count: 1 })
            .await
            .map(|r| r.into_inner())
    }

    async fn replicas(&self, path: &str, index: u64) -> Vec<Replica> {
        self.find_location(path, index).await.map(|r| r.chunks.first().map(|c| c.replicas.clone()).unwrap_or_default()).unwrap_or_default()
    }

    async fn find_lease_holder(&self, path: &str, index: u64) -> Result<FindLeaseHolderResponse, Status> {
        self.client.clone().find_lease_holder(FindLeaseHolderRequest { path: path.to_string(), index }).await.map(|r| r.into_inner())
    }
}

impl Drop for MasterHarness {
    fn drop(&mut self) {
        if let Some((_, task)) = self.server.take() {
            task.abort();
        }
        if let Some(master) = self.master.take() {
            master.stop();
        }
    }
}

struct TestContext {
    _dir: tempfile::TempDir,
    harness: MasterHarness,
    fakes: Vec<FakeChunkserver>,
}

impl TestContext {
    async fn new() -> TestContext {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path().to_str().unwrap());
        Self::with_config(dir, config).await
    }

    async fn with_config(dir: tempfile::TempDir, config: Config) -> TestContext {
        let harness = MasterHarness::start(config).await;
        TestContext { _dir: dir, harness, fakes: Vec::new() }
    }

    async fn start_fakes(&mut self, count: usize) {
        for i in 0..count {
            let mut fake = FakeChunkserver::start(&format!("cs{i}"), &format!("rack{}", i % 2)).await;
            fake.start_heartbeats(self.harness.address(), Duration::from_millis(100));
            self.fakes.push(fake);
        }
        sleep(Duration::from_millis(250)).await;
    }

    fn fake_by_id(&self, id: &str) -> &FakeChunkserver {
        self.fakes.iter().find(|f| f.id() == id).expect("fake chunkserver by id")
    }

    async fn finish(mut self) {
        for fake in &mut self.fakes {
            fake.stop_heartbeats();
        }
        self.harness.stop().await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn namespace_operations() {
    let t = TestContext::new().await;
    let h = &t.harness;
    assert!(h.create("/a/b").await.is_ok());
    assert!(h.create("/a/c").await.is_ok());
    assert_eq!(code(h.create("/a/b").await), Code::AlreadyExists);
    assert_eq!(code(h.create("/a/b/x").await), Code::InvalidArgument);
    assert_eq!(code(h.create("/a").await), Code::AlreadyExists);
    assert_eq!(code(h.create("bad").await), Code::InvalidArgument);
    assert_eq!(h.open("/a/b").await.unwrap(), 0);
    assert_eq!(h.list("/", false).await.unwrap(), vec!["a/"]);
    assert_eq!(h.list("/a", false).await.unwrap(), vec!["b", "c"]);
    assert_eq!(code(h.list("/nope", false).await), Code::NotFound);

    assert!(h.remove("/a/b").await.is_ok());
    assert_eq!(code(h.open("/a/b").await), Code::NotFound);
    assert_eq!(h.list("/a", false).await.unwrap(), vec!["c"]);
    let hidden = h.list("/a", true).await.unwrap();
    assert_eq!(hidden.len(), 2);
    assert!(hidden[0].starts_with(".deleted."));
    assert!(h.rename(&format!("/a/{}", hidden[0]), "/a/b").await.is_ok());
    assert!(h.open("/a/b").await.is_ok());
    assert!(h.remove("/a/b").await.is_ok());
    let hidden = h.list("/a", true).await.unwrap();
    assert!(h.remove(&format!("/a/{}", hidden[0])).await.is_ok());
    assert_eq!(h.list("/a", true).await.unwrap(), vec!["c"]);

    assert_eq!(code(h.remove("/a").await), Code::NotFound);
    assert_eq!(code(h.remove("/missing").await), Code::NotFound);
    assert!(h.rename("/a/c", "/z/c").await.is_ok());
    assert!(h.rename("/z", "/y").await.is_ok());
    assert!(h.open("/y/c").await.is_ok());
    assert_eq!(code(h.rename("/y", "/y/inside").await), Code::InvalidArgument);
    assert_eq!(code(h.rename("/y/c", "/y/c").await), Code::AlreadyExists);
    assert_eq!(h.list("/", false).await.unwrap(), vec!["y/"]);
    t.finish().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn add_chunk_and_lease_grant() {
    let mut t = TestContext::new().await;
    t.start_fakes(3).await;
    let h = &t.harness;
    assert!(h.create("/f").await.is_ok());
    let added = h.add_chunk("/f", 0).await.unwrap();
    assert_eq!(added.code(), ResultCode::Ok);
    let chunk = added.chunk.unwrap();
    assert_eq!(chunk.replicas.len(), 3);
    assert_eq!(chunk.version, 1);
    let handle = chunk.handle;
    for fake in &t.fakes {
        let creates = fake.creates();
        assert_eq!(creates.len(), 1);
        assert_eq!(creates[0].handle, handle);
        assert_eq!(creates[0].version, 1);
        assert_eq!(creates[0].copy_from, 0);
    }
    assert_eq!(h.added_handle("/f", 0).await, handle);
    assert_eq!(code(h.add_chunk("/f", 5).await), Code::InvalidArgument);
    assert_eq!(h.open("/f").await.unwrap(), 1);

    let located = h.find_location("/f", 0).await.unwrap();
    assert_eq!(located.chunks.len(), 1);
    assert_eq!(located.chunks[0].replicas.len(), 3);

    let lease = h.find_lease_holder("/f", 0).await.unwrap();
    assert_eq!(lease.code(), ResultCode::Ok);
    assert_eq!(lease.handle, handle);
    assert_eq!(lease.version, 2);
    assert_eq!(lease.secondaries.len(), 2);
    let primary_id = lease.primary.as_ref().unwrap().chunkserver_id.clone();
    let primary = t.fake_by_id(&primary_id);
    let grants = primary.grants();
    assert_eq!(grants.len(), 1);
    assert_eq!(grants[0].version, 2);
    assert_eq!(grants[0].secondaries.len(), 2);
    assert_eq!(grants[0].lease_ms, 2000);
    for s in &lease.secondaries {
        let updates = t.fake_by_id(&s.chunkserver_id).updates();
        assert_eq!(updates.len(), 1);
        assert_eq!(updates[0].version, 2);
    }
    let again = h.find_lease_holder("/f", 0).await.unwrap();
    assert_eq!(again.primary.unwrap().chunkserver_id, primary_id);
    assert_eq!(again.version, 2);
    assert_eq!(primary.grants().len(), 1);
    assert_eq!(h.find_location("/f", 0).await.unwrap().chunks[0].version, 2);

    sleep(Duration::from_millis(300)).await;
    {
        let state = h.master().state().lock();
        let lease = state.chunks.find(handle).unwrap().lease.clone().unwrap();
        assert!(lease.expiry > Instant::now() + Duration::from_millis(1500));
    }
    t.finish().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn stale_replica_is_excluded_and_told_to_delete() {
    let mut t = TestContext::new().await;
    t.start_fakes(3).await;
    let h = &t.harness;
    assert!(h.create("/s").await.is_ok());
    let handle = h.added_handle("/s", 0).await;
    let lease = h.find_lease_holder("/s", 0).await.unwrap();
    assert_eq!(lease.code(), ResultCode::Ok);
    let stale = t.fake_by_id(&lease.secondaries[0].chunkserver_id);
    stale.set_version(handle, 1);
    assert!(wait_until(|| async { h.replicas("/s", 0).await.len() == 2 }, Duration::from_secs(2)).await);
    for r in h.replicas("/s", 0).await {
        assert_ne!(r.chunkserver_id, stale.id());
    }
    assert!(wait_until(|| async { !stale.holds(handle) }, Duration::from_secs(2)).await);
    let primary = t.fake_by_id(&lease.primary.as_ref().unwrap().chunkserver_id);
    assert!(wait_until(|| async { primary.grants().len() >= 2 }, Duration::from_secs(2)).await);
    assert_eq!(primary.grants().last().unwrap().secondaries.len(), 1);
    t.finish().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn dead_chunkserver_shrinks_replica_set_and_primary_death_waits_for_expiry() {
    let mut t = TestContext::new().await;
    t.start_fakes(3).await;
    assert!(t.harness.create("/d").await.is_ok());
    t.harness.add_chunk("/d", 0).await.unwrap();
    let lease = t.harness.find_lease_holder("/d", 0).await.unwrap();
    assert_eq!(lease.code(), ResultCode::Ok);
    let primary_id = lease.primary.as_ref().unwrap().chunkserver_id.clone();
    let secondary_id = lease.secondaries[0].chunkserver_id.clone();
    let secondary_index = t.fakes.iter().position(|f| f.id() == secondary_id).unwrap();
    t.fakes[secondary_index].stop_heartbeats();
    let h = &t.harness;
    assert!(wait_until(|| async { h.replicas("/d", 0).await.len() == 2 }, Duration::from_secs(3)).await);
    let primary = t.fake_by_id(&primary_id);
    assert!(wait_until(|| async { primary.grants().len() >= 2 }, Duration::from_secs(2)).await);
    assert_eq!(primary.grants().last().unwrap().version, 3);
    assert_eq!(primary.grants().last().unwrap().secondaries.len(), 1);
    assert_eq!(h.find_lease_holder("/d", 0).await.unwrap().version, 3);

    let primary_index = t.fakes.iter().position(|f| f.id() == primary_id).unwrap();
    t.fakes[primary_index].stop_heartbeats();
    let h = &t.harness;
    assert!(wait_until(|| async { h.replicas("/d", 0).await.len() == 1 }, Duration::from_secs(3)).await);
    let started = Instant::now();
    let replacement = h.find_lease_holder("/d", 0).await.unwrap();
    let elapsed = started.elapsed();
    assert_eq!(replacement.code(), ResultCode::Ok);
    assert_ne!(replacement.primary.unwrap().chunkserver_id, primary_id);
    assert_eq!(replacement.version, 4);
    assert!(elapsed > Duration::from_millis(500), "elapsed {elapsed:?}");
    assert!(elapsed < Duration::from_millis(3000), "elapsed {elapsed:?}");
    t.finish().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn snapshot_revokes_then_copies_on_write() {
    let mut t = TestContext::new().await;
    t.start_fakes(3).await;
    let h = &t.harness;
    assert!(h.create("/dir/s").await.is_ok());
    let old_handle = h.added_handle("/dir/s", 0).await;
    let lease = h.find_lease_holder("/dir/s", 0).await.unwrap();
    assert_eq!(lease.code(), ResultCode::Ok);
    let primary = t.fake_by_id(&lease.primary.as_ref().unwrap().chunkserver_id);

    let started = Instant::now();
    assert!(h.snapshot("/dir", "/copy").await.is_ok());
    assert!(started.elapsed() < Duration::from_millis(1000));
    let revokes = primary.revokes();
    assert_eq!(revokes.len(), 1);
    assert_eq!(revokes[0].handle, old_handle);
    assert_eq!(code(h.snapshot("/dir", "/copy").await), Code::AlreadyExists);
    assert_eq!(h.list("/copy", false).await.unwrap(), vec!["s"]);
    assert_eq!(h.find_location("/copy/s", 0).await.unwrap().chunks[0].handle, old_handle);

    let cow = h.find_lease_holder("/dir/s", 0).await.unwrap();
    assert_eq!(cow.code(), ResultCode::Ok);
    assert_ne!(cow.handle, old_handle);
    assert_eq!(cow.version, 2);
    assert_eq!(h.find_location("/dir/s", 0).await.unwrap().chunks[0].handle, cow.handle);
    assert_eq!(h.find_location("/copy/s", 0).await.unwrap().chunks[0].handle, old_handle);
    for fake in &t.fakes {
        let creates = fake.creates();
        assert_eq!(creates.len(), 2);
        assert_eq!(creates[1].handle, cow.handle);
        assert_eq!(creates[1].copy_from, old_handle);
    }
    let copy_lease = h.find_lease_holder("/copy/s", 0).await.unwrap();
    assert_eq!(copy_lease.code(), ResultCode::Ok);
    assert_eq!(copy_lease.handle, old_handle);
    assert_eq!(copy_lease.version, 3);
    for fake in &t.fakes {
        assert_eq!(fake.creates().len(), 2);
    }
    t.finish().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn recovery_replays_log_and_reconnects_replicas() {
    let mut t = TestContext::new().await;
    t.start_fakes(3).await;
    assert!(t.harness.create("/r/a").await.is_ok());
    assert!(t.harness.create("/r/b").await.is_ok());
    let handle = t.harness.added_handle("/r/a", 0).await;
    let second = t.harness.added_handle("/r/a", 1).await;
    let lease = t.harness.find_lease_holder("/r/a", 0).await.unwrap();
    assert_eq!(lease.version, 2);
    assert!(t.harness.rename("/r/b", "/r/c").await.is_ok());
    assert!(t.harness.remove("/r/c").await.is_ok());

    for fake in &mut t.fakes {
        fake.stop_heartbeats();
    }
    t.harness.restart().await;
    let h = &t.harness;
    assert_eq!(h.open("/r/a").await.unwrap(), 2);
    assert_eq!(code(h.open("/r/c").await), Code::NotFound);
    assert_eq!(h.list("/r", true).await.unwrap().len(), 2);
    let located = h.find_location("/r/a", 0).await.unwrap();
    assert_eq!(located.chunks.len(), 1);
    assert_eq!(located.chunks[0].handle, handle);
    assert_eq!(located.chunks[0].version, 2);
    assert_eq!(located.chunks[0].replicas.len(), 0);
    assert_eq!(h.find_lease_holder("/r/a", 0).await.unwrap().code(), ResultCode::NoReplicas);

    let address = t.harness.address().to_string();
    for fake in &mut t.fakes {
        fake.start_heartbeats(&address, Duration::from_millis(100));
    }
    let h = &t.harness;
    assert!(wait_until(|| async { h.replicas("/r/a", 0).await.len() == 3 }, Duration::from_secs(3)).await);
    assert_eq!(h.find_location("/r/a", 1).await.unwrap().chunks[0].handle, second);
    assert!(h.create("/r/new").await.is_ok());
    assert!(h.added_handle("/r/new", 0).await > second);
    let regranted = h.find_lease_holder("/r/a", 0).await.unwrap();
    assert_eq!(regranted.code(), ResultCode::Ok);
    assert_eq!(regranted.version, 3);
    t.finish().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn garbage_collection_removes_hidden_files_and_orphan_chunks() {
    let mut t = TestContext::new().await;
    t.start_fakes(3).await;
    let h = &t.harness;
    assert!(h.create("/g").await.is_ok());
    let handle = h.added_handle("/g", 0).await;
    for fake in &t.fakes {
        assert!(fake.holds(handle));
    }
    assert!(h.remove("/g").await.is_ok());
    assert_eq!(h.list("/", true).await.unwrap().len(), 1);
    assert!(wait_until(|| async { h.list("/", true).await.unwrap().is_empty() }, Duration::from_secs(4)).await);
    assert!(wait_until(|| async { h.master().state().lock().chunks.find(handle).is_none() }, Duration::from_secs(2)).await);
    for fake in &t.fakes {
        assert!(wait_until(|| async { !fake.holds(handle) }, Duration::from_secs(2)).await);
    }
    t.finish().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn checkpoint_rotation_survives_restart() {
    let dir = tempfile::tempdir().unwrap();
    let mut config = test_config(dir.path().to_str().unwrap());
    config.checkpoint_log_threshold = 512;
    let data_dir = dir.path().to_path_buf();
    let mut t = TestContext::with_config(dir, config).await;
    for i in 0..60 {
        assert!(t.harness.create(&format!("/many/f{i}")).await.is_ok());
    }
    assert!(wait_until(|| async { !checkpoint::list(&data_dir).is_empty() }, Duration::from_secs(3)).await);
    t.harness.restart().await;
    assert_eq!(t.harness.list("/many", false).await.unwrap().len(), 60);
    assert!(t.harness.create("/many/after").await.is_ok());
    t.harness.restart().await;
    assert_eq!(t.harness.list("/many", false).await.unwrap().len(), 61);
    t.finish().await;
}

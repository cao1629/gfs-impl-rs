use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use futures::future::join_all;
use parking_lot::Mutex;
use tokio::sync::mpsc;
use tokio::task::{JoinHandle, spawn_blocking};
use tokio::time::{sleep, timeout};
use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::Channel;
use tonic::{Status, Streaming};
use tracing::{info, warn};

use crate::chunkserver::chunk_store::ChunkStore;
use crate::chunkserver::data_buffer::{BufferKey, DataBuffer};
use crate::chunkserver::lease_table::{LeaseCheck, LeaseInfo, LeaseTable};
use crate::common::config::Config;
use crate::common::ids::handle_to_hex;
use crate::common::net::lazy_channel;
use crate::rpc::chunkserver_client::ChunkserverClient;
use crate::rpc::master_client::MasterClient;
use crate::rpc::push_data_request::Payload;
use crate::rpc::{
    ApplyMutationRequest, ApplyMutationResponse, ChunkReport, CreateChunkRequest, CreateChunkResponse, GetChunkLengthRequest,
    GetChunkLengthResponse, GrantLeaseRequest, GrantLeaseResponse, HeartBeatRequest, MutationKind, PushDataRequest, PushDataResponse,
    PushHeader, ReadRequest, ReadResponse, RecordAppendRequest, RecordAppendResponse, Replica, ResultCode, RevokeLeaseRequest,
    RevokeLeaseResponse, UpdateVersionRequest, UpdateVersionResponse, WriteRequest, WriteResponse,
};

pub fn message_limit(chunk_size: u64) -> usize {
    (chunk_size + (1 << 20)).min(i32::MAX as u64) as usize
}

pub struct Chunkserver {
    config: Config,
    id: String,
    store: Arc<ChunkStore>,
    buffer: DataBuffer,
    leases: LeaseTable,
    master: MasterClient<Channel>,
    channels: Mutex<HashMap<String, Channel>>,
    advertise: Mutex<String>,
    heartbeat: Mutex<Option<JoinHandle<()>>>,
    master_reachable: AtomicBool,
}

type Mutation = tokio::sync::OwnedMutexGuard<()>;

type Forward = (Replica, mpsc::Sender<PushDataRequest>, JoinHandle<Result<PushDataResponse, Status>>);

fn push_failed(code: ResultCode, failed_at: String) -> PushDataResponse {
    PushDataResponse { code: code as i32, failed_at }
}

impl Chunkserver {
    pub fn new(config: Config, id: String, store: Arc<ChunkStore>) -> Arc<Chunkserver> {
        let master = MasterClient::new(lazy_channel(&config.master_address));
        let advertise = config.effective_advertise();
        Arc::new(Chunkserver {
            buffer: DataBuffer::new(config.data_buffer_capacity),
            leases: LeaseTable::new(config.lease_clock_skew_margin),
            config,
            id,
            store,
            master,
            channels: Mutex::new(HashMap::new()),
            advertise: Mutex::new(advertise),
            heartbeat: Mutex::new(None),
            master_reachable: AtomicBool::new(true),
        })
    }

    pub fn id(&self) -> &str {
        &self.id
    }

    pub fn config(&self) -> &Config {
        &self.config
    }

    pub fn store(&self) -> &Arc<ChunkStore> {
        &self.store
    }

    pub fn set_advertise_address(&self, address: String) {
        *self.advertise.lock() = address;
    }

    fn client_for(&self, address: &str) -> ChunkserverClient<Channel> {
        let channel = self.channels.lock().entry(address.to_string()).or_insert_with(|| lazy_channel(address)).clone();
        let limit = message_limit(self.config.chunk_size);
        ChunkserverClient::new(channel).max_decoding_message_size(limit).max_encoding_message_size(limit)
    }

    async fn blocking<T, F>(&self, f: F) -> T
    where
        T: Send + 'static,
        F: FnOnce(Arc<ChunkStore>) -> T + Send + 'static,
    {
        let store = self.store.clone();
        spawn_blocking(move || f(store)).await.expect("chunk store task panicked")
    }

    pub async fn push_data(&self, mut stream: Streaming<PushDataRequest>) -> PushDataResponse {
        let header = match stream.message().await {
            Ok(Some(PushDataRequest { payload: Some(Payload::Header(header)) })) => header,
            _ => return push_failed(ResultCode::Failed, self.id.clone()),
        };
        let mut data = Vec::with_capacity(header.total_length.min(message_limit(self.config.chunk_size) as u64) as usize);
        let mut forward: Option<Forward> = None;
        if let Some(next) = header.forward_to.first().cloned() {
            let (tx, rx) = mpsc::channel(4);
            let mut client = self.client_for(&next.address);
            let deadline = self.config.client_rpc_deadline;
            let task = tokio::spawn(async move {
                match timeout(deadline, client.push_data(ReceiverStream::new(rx))).await {
                    Ok(Ok(resp)) => Ok(resp.into_inner()),
                    Ok(Err(status)) => Err(status),
                    Err(_) => Err(Status::deadline_exceeded("push forward timed out")),
                }
            });
            let forwarded = PushHeader { forward_to: header.forward_to[1..].to_vec(), ..header.clone() };
            let _ = tx.send(PushDataRequest { payload: Some(Payload::Header(forwarded)) }).await;
            forward = Some((next, tx, task));
        }

        let mut forward_broken = false;
        while let Ok(Some(message)) = stream.message().await {
            let Some(Payload::Data(bytes)) = message.payload else { continue };
            data.extend_from_slice(&bytes);
            if let Some((_, tx, _)) = &forward
                && !forward_broken
                && tx.send(PushDataRequest { payload: Some(Payload::Data(bytes)) }).await.is_err()
            {
                forward_broken = true;
            }
        }

        if let Some((next, tx, task)) = forward {
            drop(tx);
            match task.await {
                Ok(Ok(downstream)) => {
                    if forward_broken {
                        warn!("push forward to {} broke mid-stream", next.address);
                        return push_failed(ResultCode::Failed, next.chunkserver_id);
                    }
                    if downstream.code() != ResultCode::Ok {
                        return push_failed(downstream.code(), downstream.failed_at);
                    }
                }
                Ok(Err(status)) => {
                    warn!("push forward to {} failed: {}", next.address, status.message());
                    return push_failed(ResultCode::Failed, next.chunkserver_id);
                }
                Err(_) => return push_failed(ResultCode::Failed, next.chunkserver_id),
            }
        }

        if header.total_length != 0 && data.len() as u64 != header.total_length {
            return push_failed(ResultCode::Failed, self.id.clone());
        }
        self.buffer.put(BufferKey::new(header.client_id, header.sequence), data);
        PushDataResponse { code: ResultCode::Ok as i32, failed_at: String::new() }
    }

    pub async fn read(&self, req: ReadRequest) -> ReadResponse {
        let Some(version) = self.store.version(req.handle) else {
            return ReadResponse { code: ResultCode::NoSuchChunk as i32, data: Vec::new() };
        };
        if version < req.version {
            return ReadResponse { code: ResultCode::StaleVersion as i32, data: Vec::new() };
        }
        match self.blocking(move |store| store.read(req.handle, req.offset, req.length)).await {
            Ok(data) => ReadResponse { code: ResultCode::Ok as i32, data },
            Err(code) => ReadResponse { code: code as i32, data: Vec::new() },
        }
    }

    async fn prepare_mutation(&self, handle: u64, version: u64) -> Result<(Mutation, LeaseInfo), ResultCode> {
        let Some(mutex) = self.store.mutation_lock(handle) else { return Err(ResultCode::NoSuchChunk) };
        let lock = mutex.lock_owned().await;
        let Some(current) = self.store.version(handle) else { return Err(ResultCode::NoSuchChunk) };
        if current != version {
            return Err(ResultCode::StaleVersion);
        }
        match self.leases.check(handle) {
            LeaseCheck::NotHeld => Err(ResultCode::NotPrimary),
            LeaseCheck::Expired => Err(ResultCode::LeaseExpired),
            LeaseCheck::Primary(info) => Ok((lock, info)),
        }
    }

    async fn forward_mutation(&self, req: ApplyMutationRequest, secondaries: &[Replica]) -> Option<String> {
        let calls = secondaries.iter().map(|secondary| {
            let mut client = self.client_for(&secondary.address);
            let req = req.clone();
            let deadline = self.config.rpc_deadline;
            let address = secondary.address.clone();
            async move {
                match timeout(deadline, client.apply_mutation(req)).await {
                    Ok(Ok(resp)) if resp.get_ref().code() == ResultCode::Ok => true,
                    Ok(Ok(resp)) => {
                        warn!("apply mutation on {address} rejected: {}", resp.get_ref().code().as_str_name());
                        false
                    }
                    Ok(Err(status)) => {
                        warn!("apply mutation on {address} failed: {}", status.message());
                        false
                    }
                    Err(_) => {
                        warn!("apply mutation on {address} timed out");
                        false
                    }
                }
            }
        });
        let results = join_all(calls).await;
        secondaries.iter().zip(results).find(|(_, ok)| !ok).map(|(secondary, _)| secondary.chunkserver_id.clone())
    }

    pub async fn write(&self, req: WriteRequest) -> WriteResponse {
        let failed = |code: ResultCode, at: String| WriteResponse { code: code as i32, failed_at: at };
        let (_lock, lease) = match self.prepare_mutation(req.handle, req.version).await {
            Ok(prepared) => prepared,
            Err(code) => return failed(code, String::new()),
        };
        let key = BufferKey::new(req.client_id.clone(), req.sequence);
        let Some(size) = self.buffer.size_of(&key) else { return failed(ResultCode::DataMissing, String::new()) };
        if req.offset.saturating_add(size) > self.config.chunk_size {
            return failed(ResultCode::OutOfRange, String::new());
        }
        let Some(data) = self.buffer.take(&key) else { return failed(ResultCode::DataMissing, String::new()) };
        let serial = self.leases.next_serial(req.handle);
        let (handle, offset) = (req.handle, req.offset);
        if self.blocking(move |store| store.write(handle, offset, &data)).await != ResultCode::Ok {
            return failed(ResultCode::Failed, self.id.clone());
        }
        let apply = ApplyMutationRequest {
            handle: req.handle,
            version: req.version,
            serial,
            kind: MutationKind::Write as i32,
            offset: req.offset,
            client_id: req.client_id,
            sequence: req.sequence,
        };
        if let Some(at) = self.forward_mutation(apply, &lease.secondaries).await {
            return failed(ResultCode::Failed, at);
        }
        WriteResponse { code: ResultCode::Ok as i32, failed_at: String::new() }
    }

    pub async fn record_append(&self, req: RecordAppendRequest) -> RecordAppendResponse {
        let failed = |code: ResultCode, at: String| RecordAppendResponse { code: code as i32, offset: 0, failed_at: at };
        let (_lock, lease) = match self.prepare_mutation(req.handle, req.version).await {
            Ok(prepared) => prepared,
            Err(code) => return failed(code, String::new()),
        };
        let Some(data) = self.buffer.take(&BufferKey::new(req.client_id.clone(), req.sequence)) else {
            return failed(ResultCode::DataMissing, String::new());
        };
        let Ok(current) = self.store.length(req.handle) else { return failed(ResultCode::NoSuchChunk, String::new()) };
        let serial = self.leases.next_serial(req.handle);
        let mut apply = ApplyMutationRequest {
            handle: req.handle,
            version: req.version,
            serial,
            kind: MutationKind::Write as i32,
            offset: current,
            client_id: req.client_id,
            sequence: req.sequence,
        };
        let handle = req.handle;
        if current + data.len() as u64 > self.config.chunk_size {
            if self.blocking(move |store| store.pad(handle, current)).await != ResultCode::Ok {
                return failed(ResultCode::Failed, self.id.clone());
            }
            apply.set_kind(MutationKind::Pad);
            if let Some(at) = self.forward_mutation(apply, &lease.secondaries).await {
                warn!("padding chunk {} did not reach {at}", handle_to_hex(handle));
                return failed(ResultCode::Failed, at);
            }
            return failed(ResultCode::RetryNextChunk, String::new());
        }
        if self.blocking(move |store| store.write(handle, current, &data)).await != ResultCode::Ok {
            return failed(ResultCode::Failed, self.id.clone());
        }
        if let Some(at) = self.forward_mutation(apply, &lease.secondaries).await {
            return failed(ResultCode::Failed, at);
        }
        RecordAppendResponse { code: ResultCode::Ok as i32, offset: current, failed_at: String::new() }
    }

    pub async fn get_chunk_length(&self, req: GetChunkLengthRequest) -> GetChunkLengthResponse {
        match self.store.length(req.handle) {
            Ok(length) => GetChunkLengthResponse { code: ResultCode::Ok as i32, length },
            Err(code) => GetChunkLengthResponse { code: code as i32, length: 0 },
        }
    }

    pub async fn apply_mutation(&self, req: ApplyMutationRequest) -> ApplyMutationResponse {
        let reply = |code: ResultCode| ApplyMutationResponse { code: code as i32 };
        let Some(mutex) = self.store.mutation_lock(req.handle) else { return reply(ResultCode::NoSuchChunk) };
        let _lock = mutex.lock_owned().await;
        let Some(current) = self.store.version(req.handle) else { return reply(ResultCode::NoSuchChunk) };
        if current != req.version {
            return reply(ResultCode::StaleVersion);
        }
        let (handle, offset) = (req.handle, req.offset);
        if req.kind() == MutationKind::Pad {
            return reply(self.blocking(move |store| store.pad(handle, offset)).await);
        }
        let Some(data) = self.buffer.take(&BufferKey::new(req.client_id, req.sequence)) else { return reply(ResultCode::DataMissing) };
        if offset.saturating_add(data.len() as u64) > self.config.chunk_size {
            return reply(ResultCode::OutOfRange);
        }
        reply(self.blocking(move |store| store.write(handle, offset, &data)).await)
    }

    pub async fn create_chunk(&self, req: CreateChunkRequest) -> CreateChunkResponse {
        if let Some(existing) = self.store.version(req.handle) {
            let code = if existing == req.version { ResultCode::Ok } else { ResultCode::Failed };
            return CreateChunkResponse { code: code as i32 };
        }
        let (handle, version, copy_from) = (req.handle, req.version, req.copy_from);
        let mut code = self
            .blocking(
                move |store| if copy_from != 0 { store.create_copy(handle, version, copy_from) } else { store.create(handle, version) },
            )
            .await;
        if code == ResultCode::Failed && self.store.version(handle) == Some(version) {
            code = ResultCode::Ok;
        }
        CreateChunkResponse { code: code as i32 }
    }

    pub async fn grant_lease(&self, req: GrantLeaseRequest) -> GrantLeaseResponse {
        let (handle, version) = (req.handle, req.version);
        let code = self.blocking(move |store| store.set_version(handle, version)).await;
        if code != ResultCode::Ok {
            return GrantLeaseResponse { code: code as i32 };
        }
        self.leases.grant(req.handle, Duration::from_millis(req.lease_ms), req.secondaries);
        GrantLeaseResponse { code: ResultCode::Ok as i32 }
    }

    pub async fn revoke_lease(&self, req: RevokeLeaseRequest) -> RevokeLeaseResponse {
        self.leases.revoke(req.handle);
        RevokeLeaseResponse { code: ResultCode::Ok as i32 }
    }

    pub async fn update_version(&self, req: UpdateVersionRequest) -> UpdateVersionResponse {
        let (handle, version) = (req.handle, req.version);
        let code = self.blocking(move |store| store.set_version(handle, version)).await;
        if code == ResultCode::Ok {
            self.leases.revoke(handle);
        }
        UpdateVersionResponse { code: code as i32 }
    }

    pub fn start_heartbeat(self: &Arc<Self>) {
        let mut heartbeat = self.heartbeat.lock();
        if heartbeat.is_some() {
            return;
        }
        let weak = Arc::downgrade(self);
        let interval = self.config.heartbeat_interval;
        *heartbeat = Some(tokio::spawn(async move {
            loop {
                {
                    let Some(this) = weak.upgrade() else { return };
                    this.send_heartbeat().await;
                }
                sleep(interval).await;
            }
        }));
    }

    pub fn stop(&self) {
        if let Some(task) = self.heartbeat.lock().take() {
            task.abort();
        }
    }

    pub async fn send_heartbeat(&self) -> bool {
        let mut req = HeartBeatRequest {
            chunkserver_id: self.id.clone(),
            address: self.advertise.lock().clone(),
            rack: self.config.rack.clone(),
            ..Default::default()
        };
        for listing in self.store.list() {
            req.chunks.push(ChunkReport { handle: listing.handle, version: listing.version, length: listing.length });
        }
        req.lease_extension_requests = self.leases.handles_to_extend();
        let corrupt = self.store.corrupt_handles();
        req.corrupt = corrupt.clone();

        let mut master = self.master.clone();
        let resp = match timeout(self.config.rpc_deadline, master.heart_beat(req)).await {
            Ok(Ok(resp)) => resp.into_inner(),
            Ok(Err(status)) => {
                if self.master_reachable.swap(false, Ordering::SeqCst) {
                    warn!("master {} unreachable: {}", self.config.master_address, status.message());
                }
                return false;
            }
            Err(_) => {
                if self.master_reachable.swap(false, Ordering::SeqCst) {
                    warn!("master {} unreachable: heartbeat timed out", self.config.master_address);
                }
                return false;
            }
        };
        if !self.master_reachable.swap(true, Ordering::SeqCst) {
            info!("master {} reachable again", self.config.master_address);
        }
        self.store.clear_corrupt(&corrupt);
        let deletes = resp.delete_handles.clone();
        for handle in &deletes {
            self.leases.revoke(*handle);
        }
        if !deletes.is_empty() {
            let deleted =
                self.blocking(move |store| deletes.into_iter().filter(|handle| store.remove(*handle)).collect::<Vec<u64>>()).await;
            for handle in deleted {
                info!("deleted chunk {} on master's word", handle_to_hex(handle));
            }
        }
        for extension in resp.extended {
            self.leases.extend(extension.handle, Duration::from_millis(extension.lease_ms));
        }
        true
    }
}

impl Drop for Chunkserver {
    fn drop(&mut self) {
        self.stop();
    }
}

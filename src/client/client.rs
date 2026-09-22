use std::collections::HashMap;
use std::fmt;
use std::future::Future;
use std::hash::{DefaultHasher, Hash, Hasher};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use futures::stream;
use parking_lot::Mutex;
use tokio::sync::OnceCell;
use tokio::time::{sleep, timeout};
use tonic::transport::Channel;
use tonic::{Code, Response, Status};

use crate::client::location_cache::{CachedChunk, CachedLease, LeaseCache, LocationCache};
use crate::common::config::Config;
use crate::common::distance::{Endpoint, order_push_chain};
use crate::common::ids::random_hex_id;
use crate::common::net::lazy_channel;
use crate::rpc::chunkserver_client::ChunkserverClient;
use crate::rpc::master_client::MasterClient;
use crate::rpc::push_data_request::Payload;
use crate::rpc::{
    AddChunkRequest, CreateRequest, DeleteRequest, FindLeaseHolderRequest, FindLocationRequest, FindMatchingFilesRequest,
    GetChunkLengthRequest, GetClusterInfoRequest, OpenRequest, PushDataRequest, PushHeader, ReadRequest, RecordAppendRequest,
    RenameRequest, Replica, ResultCode, SnapshotRequest, WriteRequest,
};

const LOCATION_BATCH: u32 = 4;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ErrorCode {
    NotFound,
    AlreadyExists,
    InvalidArgument,
    Unavailable,
    Stale,
    Failed,
}

impl ErrorCode {
    pub fn name(self) -> &'static str {
        match self {
            ErrorCode::NotFound => "not found",
            ErrorCode::AlreadyExists => "already exists",
            ErrorCode::InvalidArgument => "invalid argument",
            ErrorCode::Unavailable => "unavailable",
            ErrorCode::Stale => "stale",
            ErrorCode::Failed => "failed",
        }
    }
}

impl fmt::Display for ErrorCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.name())
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Error {
    pub code: ErrorCode,
    pub message: String,
}

impl Error {
    pub fn new(code: ErrorCode, message: impl Into<String>) -> Error {
        Error { code, message: message.into() }
    }

    fn from_status(status: &Status) -> Error {
        let code = match status.code() {
            Code::NotFound => ErrorCode::NotFound,
            Code::AlreadyExists => ErrorCode::AlreadyExists,
            Code::InvalidArgument => ErrorCode::InvalidArgument,
            Code::Unavailable | Code::DeadlineExceeded => ErrorCode::Unavailable,
            _ => ErrorCode::Failed,
        };
        Error::new(code, status.message())
    }

    fn from_result_code(code: ResultCode, context: &str) -> Error {
        let mapped = match code {
            ResultCode::StaleVersion | ResultCode::NotPrimary | ResultCode::LeaseExpired | ResultCode::NoSuchChunk => ErrorCode::Stale,
            ResultCode::OutOfRange => ErrorCode::InvalidArgument,
            ResultCode::NoReplicas | ResultCode::ChecksumMismatch | ResultCode::DataMissing => ErrorCode::Unavailable,
            _ => ErrorCode::Failed,
        };
        Error::new(mapped, format!("{context}: {}", code.as_str_name()))
    }

    fn retryable_later(&self) -> bool {
        matches!(self.code, ErrorCode::Unavailable | ErrorCode::Stale | ErrorCode::Failed)
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.message.is_empty() { write!(f, "{}", self.code) } else { write!(f, "{}: {}", self.code, self.message) }
    }
}

impl std::error::Error for Error {}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct FileInfo {
    pub chunk_count: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DirEntry {
    pub name: String,
    pub is_directory: bool,
}

enum Append {
    Done(u64),
    NextChunk,
}

pub struct Client {
    config: Config,
    client_id: String,
    replica_seed: usize,
    sequence: AtomicU64,
    master: MasterClient<Channel>,
    chunkservers: Mutex<HashMap<String, Channel>>,
    locations: LocationCache,
    leases: LeaseCache,
    cluster_info: OnceCell<(u64, u64)>,
}

impl Client {
    pub fn new(config: Config) -> Client {
        let client_id = random_hex_id(16);
        let mut hasher = DefaultHasher::new();
        client_id.hash(&mut hasher);
        let replica_seed = hasher.finish() as usize;
        Client {
            master: MasterClient::new(lazy_channel(&config.master_address)),
            locations: LocationCache::new(config.client_location_cache_ttl),
            leases: LeaseCache::new(config.client_location_cache_ttl),
            config,
            client_id,
            replica_seed,
            sequence: AtomicU64::new(1),
            chunkservers: Mutex::new(HashMap::new()),
            cluster_info: OnceCell::new(),
        }
    }

    pub fn client_id(&self) -> &str {
        &self.client_id
    }

    pub fn config(&self) -> &Config {
        &self.config
    }

    pub async fn chunk_size(&self) -> u64 {
        self.ensure_cluster_info().await.map(|(chunk_size, _)| chunk_size).unwrap_or(0)
    }

    pub async fn create(&self, path: &str) -> Result<(), Error> {
        let mut master = self.master.clone();
        self.call(self.config.client_rpc_deadline, master.create(CreateRequest { path: path.to_string() })).await?;
        Ok(())
    }

    pub async fn remove(&self, path: &str) -> Result<(), Error> {
        let mut master = self.master.clone();
        let result = self.call(self.config.client_rpc_deadline, master.delete(DeleteRequest { path: path.to_string() })).await;
        self.forget_file(path);
        result.map(|_| ())
    }

    pub async fn rename(&self, source: &str, target: &str) -> Result<(), Error> {
        let mut master = self.master.clone();
        let request = RenameRequest { source: source.to_string(), target: target.to_string() };
        let result = self.call(self.config.client_rpc_deadline, master.rename(request)).await;
        self.forget_file(source);
        self.forget_file(target);
        result.map(|_| ())
    }

    pub async fn snapshot(&self, source: &str, target: &str) -> Result<(), Error> {
        let mut master = self.master.clone();
        let request = SnapshotRequest { source: source.to_string(), target: target.to_string() };
        let result = self.call(self.config.client_rpc_deadline, master.snapshot(request)).await;
        self.forget_file(source);
        result.map(|_| ())
    }

    pub async fn list(&self, directory: &str, include_hidden: bool) -> Result<Vec<DirEntry>, Error> {
        let mut master = self.master.clone();
        let request = FindMatchingFilesRequest { directory: directory.to_string(), include_hidden };
        let resp = self.call(self.config.client_rpc_deadline, master.find_matching_files(request)).await?;
        Ok(resp.entries.into_iter().map(|e| DirEntry { name: e.name, is_directory: e.is_directory }).collect())
    }

    pub async fn open(&self, path: &str) -> Result<FileInfo, Error> {
        let mut master = self.master.clone();
        let resp = self.call(self.config.client_rpc_deadline, master.open(OpenRequest { path: path.to_string() })).await?;
        Ok(FileInfo { chunk_count: resp.chunk_count })
    }

    pub async fn length(&self, path: &str) -> Result<u64, Error> {
        let (chunk_size, _) = self.ensure_cluster_info().await?;
        let info = self.open(path).await?;
        if info.chunk_count == 0 {
            return Ok(0);
        }
        let last = info.chunk_count - 1;
        for _attempt in 0..2 {
            let Some(chunk) = self.locate(path, last).await? else { return Ok(last * chunk_size) };
            let n = chunk.replicas.len();
            for k in 0..n {
                let replica = &chunk.replicas[(self.replica_seed + k) % n];
                let mut client = self.chunkserver(&replica.address);
                let request = GetChunkLengthRequest { handle: chunk.handle };
                match self.call(self.config.client_rpc_deadline, client.get_chunk_length(request)).await {
                    Ok(resp) if resp.code() == ResultCode::Ok => return Ok(last * chunk_size + resp.length),
                    _ => continue,
                }
            }
            self.locations.invalidate(path, last);
        }
        Err(Error::new(ErrorCode::Unavailable, "no replica could report the length of the last chunk"))
    }

    pub async fn read(&self, path: &str, offset: u64, length: u64) -> Result<Vec<u8>, Error> {
        let mut data = Vec::new();
        if length == 0 {
            return Ok(data);
        }
        let (chunk_size, _) = self.ensure_cluster_info().await?;
        let mut pos = offset;
        let end = offset.saturating_add(length);
        while pos < end {
            let index = pos / chunk_size;
            let in_chunk = pos % chunk_size;
            let want = (end - pos).min(chunk_size - in_chunk);
            let Some(piece) = self.read_chunk(path, index, in_chunk, want).await? else { break };
            let got = piece.len() as u64;
            data.extend_from_slice(&piece);
            pos += got;
            if got < want {
                break;
            }
        }
        Ok(data)
    }

    pub async fn write(&self, path: &str, offset: u64, data: &[u8]) -> Result<(), Error> {
        if data.is_empty() {
            return Ok(());
        }
        let (chunk_size, _) = self.ensure_cluster_info().await?;
        let mut pos = 0u64;
        while pos < data.len() as u64 {
            let absolute = offset + pos;
            let index = absolute / chunk_size;
            let in_chunk = absolute % chunk_size;
            let len = (data.len() as u64 - pos).min(chunk_size - in_chunk);
            self.write_chunk(path, index, in_chunk, &data[pos as usize..(pos + len) as usize]).await?;
            pos += len;
        }
        Ok(())
    }

    pub async fn record_append(&self, path: &str, data: &[u8]) -> Result<u64, Error> {
        let (chunk_size, max_append) = self.ensure_cluster_info().await?;
        if data.len() as u64 > max_append {
            return Err(Error::new(
                ErrorCode::InvalidArgument,
                format!("record of {} bytes exceeds the append limit of {max_append}", data.len()),
            ));
        }
        let info = self.open(path).await?;
        let mut index = info.chunk_count.saturating_sub(1);
        if info.chunk_count == 0 {
            self.ensure_chunk(path, 0).await?;
        }
        let mut last = Error::new(ErrorCode::Failed, "record append retries exhausted");
        let mut outer = 0u32;
        let mut ensured = false;
        while outer <= self.config.mutation_retry_outer {
            let (lease, from_cache) = match self.find_lease(path, index).await {
                Ok(found) => found,
                Err(e) if e.code == ErrorCode::NotFound && !ensured => {
                    match self.ensure_chunk(path, index).await {
                        Err(e) if matches!(e.code, ErrorCode::NotFound | ErrorCode::InvalidArgument) => return Err(e),
                        _ => {}
                    }
                    ensured = true;
                    continue;
                }
                Err(e) => {
                    if !e.retryable_later() {
                        return Err(e);
                    }
                    last = e;
                    outer += 1;
                    sleep(self.config.effective_outer_retry_delay()).await;
                    continue;
                }
            };
            match self.try_append(&lease, data).await {
                Ok(Append::Done(in_chunk)) => return Ok(index * chunk_size + in_chunk),
                Ok(Append::NextChunk) => match self.ensure_chunk(path, index + 1).await {
                    Ok(()) => {
                        index += 1;
                        ensured = true;
                        outer = 0;
                    }
                    Err(e) if !e.retryable_later() => return Err(e),
                    Err(e) => {
                        last = e;
                        outer += 1;
                        sleep(self.config.effective_outer_retry_delay()).await;
                    }
                },
                Err(e) => {
                    if e.code == ErrorCode::InvalidArgument {
                        return Err(e);
                    }
                    last = e;
                    self.leases.invalidate(path, index);
                    if from_cache {
                        continue;
                    }
                    outer += 1;
                    sleep(self.config.effective_outer_retry_delay()).await;
                }
            }
        }
        Err(last)
    }

    async fn call<T, F>(&self, deadline: Duration, future: F) -> Result<T, Error>
    where
        F: Future<Output = Result<Response<T>, Status>>,
    {
        match timeout(deadline, future).await {
            Ok(Ok(resp)) => Ok(resp.into_inner()),
            Ok(Err(status)) => Err(Error::from_status(&status)),
            Err(_) => Err(Error::new(ErrorCode::Unavailable, "deadline exceeded")),
        }
    }

    fn chunkserver(&self, address: &str) -> ChunkserverClient<Channel> {
        let channel = self.chunkservers.lock().entry(address.to_string()).or_insert_with(|| lazy_channel(address)).clone();
        ChunkserverClient::new(channel).max_decoding_message_size(usize::MAX).max_encoding_message_size(usize::MAX)
    }

    async fn ensure_cluster_info(&self) -> Result<(u64, u64), Error> {
        self.cluster_info
            .get_or_try_init(|| async {
                let mut master = self.master.clone();
                let resp = self.call(self.config.client_rpc_deadline, master.get_cluster_info(GetClusterInfoRequest {})).await?;
                if resp.chunk_size == 0 {
                    return Err(Error::new(ErrorCode::Failed, "master reported a chunk size of zero"));
                }
                let max_append = if resp.max_record_append_size == 0 { resp.chunk_size / 4 } else { resp.max_record_append_size };
                Ok((resp.chunk_size, max_append))
            })
            .await
            .copied()
    }

    fn forget_file(&self, path: &str) {
        self.locations.invalidate_file(path);
        self.leases.invalidate_file(path);
    }

    async fn locate(&self, path: &str, index: u64) -> Result<Option<CachedChunk>, Error> {
        if let Some(cached) = self.locations.get(path, index) {
            return Ok(Some(cached));
        }
        let mut master = self.master.clone();
        let request = FindLocationRequest { path: path.to_string(), first_index: index, count: LOCATION_BATCH };
        let resp = self.call(self.config.client_rpc_deadline, master.find_location(request)).await?;
        let mut found = None;
        for chunk in resp.chunks {
            let entry = CachedChunk { handle: chunk.handle, version: chunk.version, replicas: chunk.replicas };
            if chunk.index == index {
                found = Some(entry.clone());
            }
            self.locations.put(path, chunk.index, entry);
        }
        Ok(found)
    }

    async fn read_chunk(&self, path: &str, index: u64, in_chunk: u64, want: u64) -> Result<Option<Vec<u8>>, Error> {
        let mut last = Error::new(ErrorCode::Unavailable, "no replicas");
        for _attempt in 0..2 {
            let Some(chunk) = self.locate(path, index).await? else { return Ok(None) };
            let n = chunk.replicas.len();
            for k in 0..n {
                let replica = &chunk.replicas[(self.replica_seed + k) % n];
                let mut client = self.chunkserver(&replica.address);
                let request = ReadRequest { handle: chunk.handle, version: chunk.version, offset: in_chunk, length: want };
                match self.call(self.config.client_rpc_deadline, client.read(request)).await {
                    Ok(resp) if resp.code() == ResultCode::Ok => return Ok(Some(resp.data)),
                    Ok(resp) if resp.code() == ResultCode::OutOfRange => return Ok(Some(Vec::new())),
                    Ok(resp) => last = Error::from_result_code(resp.code(), &format!("read from {}", replica.address)),
                    Err(e) => last = e,
                }
            }
            self.locations.invalidate(path, index);
        }
        Err(Error::new(ErrorCode::Unavailable, format!("no replica could serve chunk {index} of {path}: {}", last.message)))
    }

    async fn ensure_chunk(&self, path: &str, index: u64) -> Result<(), Error> {
        let info = self.open(path).await?;
        for next in info.chunk_count..=index {
            let mut master = self.master.clone();
            let request = AddChunkRequest { path: path.to_string(), index: next };
            let resp = self.call(self.config.client_rpc_deadline, master.add_chunk(request)).await?;
            if resp.code() != ResultCode::Ok {
                return Err(Error::from_result_code(resp.code(), &format!("add chunk {next} to {path}")));
            }
        }
        Ok(())
    }

    async fn find_lease(&self, path: &str, index: u64) -> Result<(CachedLease, bool), Error> {
        if let Some(cached) = self.leases.get(path, index) {
            return Ok((cached, true));
        }
        let mut master = self.master.clone();
        let request = FindLeaseHolderRequest { path: path.to_string(), index };
        let deadline = self.config.lease_duration + 2 * self.config.lease_clock_skew_margin + self.config.client_rpc_deadline;
        let resp = self.call(deadline, master.find_lease_holder(request)).await?;
        if resp.code() != ResultCode::Ok {
            return Err(Error::from_result_code(resp.code(), &format!("find lease holder for chunk {index} of {path}")));
        }
        let lease = CachedLease {
            handle: resp.handle,
            version: resp.version,
            primary: resp.primary.unwrap_or_default(),
            secondaries: resp.secondaries,
        };
        self.leases.put(path, index, lease.clone());
        Ok((lease, false))
    }

    async fn push_data(&self, lease: &CachedLease, sequence: u64, data: &[u8]) -> Result<(), Error> {
        let mut replicas = vec![lease.primary.clone()];
        replicas.extend(lease.secondaries.iter().cloned());
        let origin = Endpoint { address: String::new(), rack: self.config.rack.clone() };
        let chain = order_push_chain(&origin, replicas);
        let first = chain[0].clone();
        let header =
            PushHeader { client_id: self.client_id.clone(), sequence, total_length: data.len() as u64, forward_to: chain[1..].to_vec() };
        let frame = if self.config.push_frame_size == 0 { 256 << 10 } else { self.config.push_frame_size as usize };
        let owned = data.to_vec();
        let frames = stream::unfold((Some(header), owned, 0usize), move |(header, owned, pos)| async move {
            if let Some(header) = header {
                return Some((PushDataRequest { payload: Some(Payload::Header(header)) }, (None, owned, pos)));
            }
            if pos >= owned.len() {
                return None;
            }
            let end = (pos + frame).min(owned.len());
            let message = PushDataRequest { payload: Some(Payload::Data(owned[pos..end].to_vec())) };
            Some((message, (None, owned, end)))
        });
        let mut client = self.chunkserver(&first.address);
        let resp = self
            .call(self.config.client_rpc_deadline, client.push_data(frames))
            .await
            .map_err(|e| Error::new(ErrorCode::Unavailable, format!("push to {}: {}", first.address, e.message)))?;
        if resp.code() != ResultCode::Ok {
            let code = resp.code();
            let failed_at = if resp.failed_at.is_empty() { first.chunkserver_id.clone() } else { resp.failed_at };
            return Err(Error::new(ErrorCode::Unavailable, format!("push failed at {failed_at}: {}", code.as_str_name())));
        }
        Ok(())
    }

    fn backoff(&self, attempt: u32) -> Duration {
        self.config.retry_backoff_base * (1u32 << attempt.min(8))
    }

    async fn try_write(&self, lease: &CachedLease, in_chunk: u64, data: &[u8]) -> Result<(), Error> {
        let mut last = Error::new(ErrorCode::Failed, "write retries exhausted");
        for inner in 0..self.config.mutation_retry_inner.max(1) {
            if inner > 0 {
                sleep(self.backoff(inner - 1)).await;
            }
            let sequence = self.sequence.fetch_add(1, Ordering::SeqCst);
            self.push_data(lease, sequence, data).await?;
            let request = WriteRequest {
                handle: lease.handle,
                version: lease.version,
                offset: in_chunk,
                client_id: self.client_id.clone(),
                sequence,
            };
            let mut client = self.chunkserver(&lease.primary.address);
            let resp = self
                .call(self.config.client_rpc_deadline, client.write(request))
                .await
                .map_err(|e| Error::new(ErrorCode::Unavailable, format!("write to primary {}: {}", lease.primary.address, e.message)))?;
            match resp.code() {
                ResultCode::Ok => return Ok(()),
                ResultCode::DataMissing | ResultCode::Failed => {
                    last = Error::from_result_code(resp.code(), &format!("write at primary {}", lease.primary.address));
                }
                code => return Err(Error::from_result_code(code, &format!("write at primary {}", lease.primary.address))),
            }
        }
        Err(last)
    }

    async fn write_chunk(&self, path: &str, index: u64, in_chunk: u64, data: &[u8]) -> Result<(), Error> {
        let mut last = Error::new(ErrorCode::Failed, "write retries exhausted");
        let mut outer = 0u32;
        let mut ensured = false;
        while outer <= self.config.mutation_retry_outer {
            let (lease, from_cache) = match self.find_lease(path, index).await {
                Ok(found) => found,
                Err(e) if e.code == ErrorCode::NotFound && !ensured => {
                    match self.ensure_chunk(path, index).await {
                        Err(e) if matches!(e.code, ErrorCode::NotFound | ErrorCode::InvalidArgument) => return Err(e),
                        _ => {}
                    }
                    ensured = true;
                    continue;
                }
                Err(e) => {
                    if !e.retryable_later() {
                        return Err(e);
                    }
                    last = e;
                    outer += 1;
                    sleep(self.config.effective_outer_retry_delay()).await;
                    continue;
                }
            };
            match self.try_write(&lease, in_chunk, data).await {
                Ok(()) => return Ok(()),
                Err(e) if e.code == ErrorCode::InvalidArgument => return Err(e),
                Err(e) => {
                    last = e;
                    self.leases.invalidate(path, index);
                    if from_cache {
                        continue;
                    }
                    outer += 1;
                    sleep(self.config.effective_outer_retry_delay()).await;
                }
            }
        }
        Err(last)
    }

    async fn try_append(&self, lease: &CachedLease, data: &[u8]) -> Result<Append, Error> {
        let mut last = Error::new(ErrorCode::Failed, "record append retries exhausted");
        for inner in 0..self.config.mutation_retry_inner.max(1) {
            if inner > 0 {
                sleep(self.backoff(inner - 1)).await;
            }
            let sequence = self.sequence.fetch_add(1, Ordering::SeqCst);
            self.push_data(lease, sequence, data).await?;
            let request = RecordAppendRequest { handle: lease.handle, version: lease.version, client_id: self.client_id.clone(), sequence };
            let mut client = self.chunkserver(&lease.primary.address);
            let resp = self.call(self.config.client_rpc_deadline, client.record_append(request)).await.map_err(|e| {
                Error::new(ErrorCode::Unavailable, format!("record append to primary {}: {}", lease.primary.address, e.message))
            })?;
            match resp.code() {
                ResultCode::Ok => return Ok(Append::Done(resp.offset)),
                ResultCode::RetryNextChunk => return Ok(Append::NextChunk),
                ResultCode::DataMissing | ResultCode::Failed => {
                    last = Error::from_result_code(resp.code(), &format!("record append at primary {}", lease.primary.address));
                }
                code => return Err(Error::from_result_code(code, &format!("record append at primary {}", lease.primary.address))),
            }
        }
        Err(last)
    }
}

pub fn replica_of(id: &str, address: &str) -> Replica {
    Replica { chunkserver_id: id.to_string(), address: address.to_string(), rack: String::new() }
}

use std::collections::{BTreeSet, HashSet};
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Instant;

use futures::future::join_all;
use parking_lot::Mutex;
use tokio::task::JoinHandle;
use tokio::time::{sleep, sleep_until, timeout};
use tonic::Status;
use tracing::{info, warn};

use crate::common::clock::{now, unix_seconds};
use crate::common::config::Config;
use crate::common::paths::{hidden_name_for, is_ancestor_or_self, is_hidden_path, is_valid_path, parse_hidden_name};
use crate::master::checkpoint;
use crate::master::chunk_table::ChunkMeta;
use crate::master::chunkserver_registry::ChunkserverRegistry;
use crate::master::lease_manager::LeaseManager;
use crate::master::lock_table::{LockMode, LockTable};
use crate::master::master_state::MasterState;
use crate::master::oplog::{self, LocalFileSink, OpLog};
use crate::rpc::{
    AddChunkRequest, AddChunkResponse, ChunkInfo, CreateChunkRequest, CreateRequest, CreateResponse, DeleteRequest, DeleteResponse,
    DirEntry, FindLeaseHolderRequest, FindLeaseHolderResponse, FindLocationRequest, FindLocationResponse, FindMatchingFilesRequest,
    FindMatchingFilesResponse, GetClusterInfoRequest, GetClusterInfoResponse, HeartBeatRequest, HeartBeatResponse, LeaseExtension,
    OpenRequest, OpenResponse, RenameRequest, RenameResponse, Replica, ResultCode, SnapshotRequest, SnapshotResponse,
};
use crate::state::log_record::Body;
use crate::state::{
    AddChunkRecord, AllocHandleRecord, BumpVersionRecord, CreateRecord, DropChunkRecord, LogRecord, RemoveRecord, RenameRecord,
    ReplaceChunkRecord, SnapshotRecord,
};

fn record(body: Body) -> LogRecord {
    LogRecord { body: Some(body) }
}

fn check_path(path: &str, allow_root: bool) -> Result<(), Status> {
    if !is_valid_path(path) {
        return Err(Status::invalid_argument(format!("malformed path: {path}")));
    }
    if !allow_root && path == "/" {
        return Err(Status::invalid_argument("the root is not a file"));
    }
    Ok(())
}

pub struct Master {
    config: Config,
    state: Arc<Mutex<MasterState>>,
    locks: Arc<LockTable>,
    oplog: Arc<OpLog>,
    registry: Arc<ChunkserverRegistry>,
    leases: Arc<LeaseManager>,
    cow: tokio::sync::Mutex<()>,
    tasks: Mutex<Vec<JoinHandle<()>>>,
    started: AtomicBool,
}

impl Master {
    pub fn new(config: Config) -> Arc<Master> {
        let state = Arc::new(Mutex::new(MasterState::default()));
        let oplog = OpLog::new(config.clone(), state.clone());
        let registry = Arc::new(ChunkserverRegistry::new(config.clone()));
        let leases = Arc::new(LeaseManager::new(config.clone(), state.clone(), registry.clone(), oplog.clone()));
        Arc::new(Master {
            config,
            state,
            locks: LockTable::new(),
            oplog,
            registry,
            leases,
            cow: tokio::sync::Mutex::new(()),
            tasks: Mutex::new(Vec::new()),
            started: AtomicBool::new(false),
        })
    }

    pub fn start(self: &Arc<Self>) {
        if self.started.swap(true, Ordering::SeqCst) {
            return;
        }
        self.recover();
        let sweep = {
            let weak = Arc::downgrade(self);
            let interval = self.config.heartbeat_interval;
            tokio::spawn(async move {
                loop {
                    sleep(interval).await;
                    let Some(master) = weak.upgrade() else { return };
                    master.sweep_dead();
                }
            })
        };
        let gc = {
            let weak = Arc::downgrade(self);
            let interval = self.config.gc_interval;
            tokio::spawn(async move {
                loop {
                    sleep(interval).await;
                    let Some(master) = weak.upgrade() else { return };
                    master.gc_pass().await;
                }
            })
        };
        self.tasks.lock().extend([sweep, gc]);
        let state = self.state.lock();
        info!("master started with {} files and {} chunks, next handle {}", state.files.len(), state.chunks.len(), state.next_handle);
    }

    pub fn stop(&self) {
        if !self.started.swap(false, Ordering::SeqCst) {
            return;
        }
        for task in self.tasks.lock().drain(..) {
            task.abort();
        }
        self.oplog.stop();
    }

    pub fn state(&self) -> &Arc<Mutex<MasterState>> {
        &self.state
    }

    pub fn locks(&self) -> &Arc<LockTable> {
        &self.locks
    }

    pub fn config(&self) -> &Config {
        &self.config
    }

    fn recover(&self) {
        let dir = Path::new(&self.config.data_dir);
        let _ = std::fs::create_dir_all(dir);
        let mut checkpoint_number = 0;
        if let Some((number, checkpoint)) = checkpoint::load_latest(dir) {
            self.state.lock().load(&checkpoint);
            checkpoint_number = number;
            info!("loaded checkpoint {number}");
        }
        let segments = oplog::list_segments(dir);
        let first = if checkpoint_number > 0 { checkpoint_number } else { 1 };
        let mut replayed = 0;
        for &segment in &segments {
            if segment < first {
                continue;
            }
            let replay = oplog::read_segment(dir, segment);
            {
                let mut state = self.state.lock();
                for record in &replay.records {
                    state.apply(record);
                }
            }
            replayed += replay.records.len();
            if replay.torn_tail {
                warn!("segment {segment} has a torn tail, truncating to {} bytes", replay.valid_bytes);
                oplog::truncate_segment(dir, segment, replay.valid_bytes);
            }
        }
        self.state.lock().recompute_refcounts();
        let active = segments.last().copied().unwrap_or(1).max(first);
        info!("replayed {replayed} records, active segment {active}");
        self.oplog.add_sink(Box::new(LocalFileSink::new(dir)));
        self.oplog.enable_checkpoints(dir);
        self.oplog.open(active);
    }

    async fn alloc_handle(&self) -> u64 {
        let (handle, seq) = {
            let mut state = self.state.lock();
            let handle = state.next_handle;
            state.apply_alloc_handle(handle);
            let seq = self.oplog.append(&record(Body::AllocHandle(AllocHandleRecord { handle })));
            state.chunks.create(handle, 1).pending = true;
            (handle, seq)
        };
        self.oplog.wait_flushed(seq).await;
        handle
    }

    fn choose_servers(&self, count: usize) -> Vec<String> {
        let servers = self.registry.alive();
        let mut ranked: Vec<(usize, String, String)> = {
            let state = self.state.lock();
            servers.into_iter().map(|s| (state.chunks.held_count(&s.id), s.id, s.rack)).collect()
        };
        ranked.sort_by_key(|(held, _, _)| *held);
        let mut chosen: Vec<String> = Vec::new();
        let mut racks: HashSet<&str> = HashSet::new();
        for (_, id, rack) in &ranked {
            if chosen.len() >= count {
                break;
            }
            if !racks.insert(rack.as_str()) {
                continue;
            }
            chosen.push(id.clone());
        }
        for (_, id, _) in &ranked {
            if chosen.len() >= count {
                break;
            }
            if !chosen.contains(id) {
                chosen.push(id.clone());
            }
        }
        chosen
    }

    async fn create_chunk_acked(&self, id: &str, request: CreateChunkRequest) -> bool {
        let Some(mut client) = self.registry.stub(id) else { return false };
        matches!(timeout(self.config.rpc_deadline, client.create_chunk(request)).await, Ok(Ok(resp)) if resp.get_ref().code() == ResultCode::Ok)
    }

    async fn create_chunk_on(&self, ids: &[String], handle: u64, version: u64, copy_from: u64) -> Vec<String> {
        let results = join_all(ids.iter().map(|id| self.create_chunk_acked(id, CreateChunkRequest { handle, version, copy_from }))).await;
        let mut ok = Vec::new();
        for (id, success) in ids.iter().zip(results) {
            if success {
                ok.push(id.clone());
            } else {
                warn!("CreateChunk {handle} failed on {id}");
            }
        }
        ok
    }

    fn resolve_locations(&self, meta: &ChunkMeta) -> Vec<Replica> {
        meta.locations.keys().filter_map(|id| self.registry.resolve(id)).collect()
    }

    fn chunk_info(&self, index: u64, handle: u64, meta: &ChunkMeta) -> ChunkInfo {
        ChunkInfo { index, handle, version: meta.version, replicas: self.resolve_locations(meta) }
    }

    pub fn get_cluster_info(&self, _req: GetClusterInfoRequest) -> GetClusterInfoResponse {
        GetClusterInfoResponse {
            chunk_size: self.config.chunk_size,
            max_record_append_size: self.config.effective_max_record_append_size(),
        }
    }

    pub async fn create(&self, req: CreateRequest) -> Result<CreateResponse, Status> {
        check_path(&req.path, false)?;
        let _locks = self.locks.acquire(LockTable::for_path(&req.path, LockMode::Write)).await;
        let seq = {
            let mut state = self.state.lock();
            if state.files.exists(&req.path) || state.files.is_directory(&req.path) {
                return Err(Status::already_exists(&req.path));
            }
            if state.files.has_file_ancestor(&req.path) {
                return Err(Status::invalid_argument(format!("an ancestor of {} is a file", req.path)));
            }
            state.apply_create(&req.path);
            self.oplog.append(&record(Body::Create(CreateRecord { path: req.path.clone() })))
        };
        self.oplog.wait_flushed(seq).await;
        Ok(CreateResponse {})
    }

    pub async fn open(&self, req: OpenRequest) -> Result<OpenResponse, Status> {
        check_path(&req.path, false)?;
        let _locks = self.locks.acquire(LockTable::for_path(&req.path, LockMode::Read)).await;
        let state = self.state.lock();
        match state.files.find(&req.path) {
            Some(meta) => Ok(OpenResponse { chunk_count: meta.chunks.len() as u64 }),
            None => Err(Status::not_found(&req.path)),
        }
    }

    pub async fn delete(&self, req: DeleteRequest) -> Result<DeleteResponse, Status> {
        check_path(&req.path, false)?;
        let _locks = self.locks.acquire(LockTable::for_path(&req.path, LockMode::Write)).await;
        let seq = {
            let mut state = self.state.lock();
            if !state.files.exists(&req.path) {
                return Err(Status::not_found(&req.path));
            }
            if is_hidden_path(&req.path) {
                state.apply_remove(&req.path);
                self.oplog.append(&record(Body::Remove(RemoveRecord { path: req.path.clone() })))
            } else {
                let mut stamp = unix_seconds();
                let mut hidden = hidden_name_for(&req.path, stamp);
                while state.files.exists(&hidden) {
                    stamp += 1;
                    hidden = hidden_name_for(&req.path, stamp);
                }
                state.apply_rename(&req.path, &hidden);
                self.oplog.append(&record(Body::Rename(RenameRecord { source: req.path.clone(), target: hidden })))
            }
        };
        self.oplog.wait_flushed(seq).await;
        Ok(DeleteResponse {})
    }

    async fn revoke_leases_on(&self, handles: &[u64]) -> Instant {
        let mut wait_until = now();
        for &handle in handles {
            let live = {
                let state = self.state.lock();
                state.chunks.find(handle).is_some_and(|meta| meta.lease.is_some())
            };
            if !live {
                continue;
            }
            let result = self.leases.revoke(handle).await;
            if let (true, false, Some(expiry)) = (result.had_lease, result.acked, result.expiry) {
                wait_until = wait_until.max(expiry + self.config.lease_clock_skew_margin + std::time::Duration::from_millis(1));
            }
        }
        wait_until
    }

    fn check_move(&self, source: &str, target: &str) -> Result<(), Status> {
        check_path(source, false)?;
        check_path(target, false)?;
        if source == target {
            return Err(Status::already_exists(target));
        }
        if is_ancestor_or_self(source, target) {
            return Err(Status::invalid_argument("target is inside the source"));
        }
        Ok(())
    }

    fn check_move_locked(&self, state: &MasterState, source: &str, target: &str) -> Result<(), Status> {
        if !state.files.exists(source) && !state.files.is_directory(source) {
            return Err(Status::not_found(source));
        }
        if state.files.exists(target) || state.files.is_directory(target) {
            return Err(Status::already_exists(target));
        }
        if state.files.has_file_ancestor(target) {
            return Err(Status::invalid_argument(format!("an ancestor of {target} is a file")));
        }
        Ok(())
    }

    pub async fn rename(&self, req: RenameRequest) -> Result<RenameResponse, Status> {
        self.check_move(&req.source, &req.target)?;
        let _locks = self.locks.acquire(LockTable::for_paths(&req.source, LockMode::Write, &req.target, LockMode::Write)).await;
        let handles: Vec<u64> = {
            let state = self.state.lock();
            self.check_move_locked(&state, &req.source, &req.target)?;
            state.files.subtree(&req.source, false).into_iter().flat_map(|(_, meta)| meta.chunks).collect()
        };
        self.revoke_leases_on(&handles).await;
        let seq = {
            let mut state = self.state.lock();
            state.apply_rename(&req.source, &req.target);
            self.oplog.append(&record(Body::Rename(RenameRecord { source: req.source.clone(), target: req.target.clone() })))
        };
        self.oplog.wait_flushed(seq).await;
        Ok(RenameResponse {})
    }

    pub async fn snapshot(&self, req: SnapshotRequest) -> Result<SnapshotResponse, Status> {
        self.check_move(&req.source, &req.target)?;
        let _locks = self.locks.acquire(LockTable::for_paths(&req.source, LockMode::Write, &req.target, LockMode::Write)).await;
        let handles: Vec<u64> = {
            let state = self.state.lock();
            self.check_move_locked(&state, &req.source, &req.target)?;
            let unique: BTreeSet<u64> = state.files.subtree(&req.source, true).into_iter().flat_map(|(_, meta)| meta.chunks).collect();
            unique.into_iter().collect()
        };
        let wait_until = self.revoke_leases_on(&handles).await;
        if wait_until > now() {
            info!("snapshot {} waiting for lease expiry", req.source);
            sleep_until(tokio::time::Instant::from_std(wait_until)).await;
        }
        let seq = {
            let mut state = self.state.lock();
            let t = now();
            for &handle in &handles {
                if let Some(meta) = state.chunks.find_mut(handle)
                    && meta.lease.is_some()
                    && !self.leases.is_valid(meta, t)
                {
                    meta.lease = None;
                }
            }
            state.apply_snapshot(&req.source, &req.target);
            self.oplog.append(&record(Body::Snapshot(SnapshotRecord { source: req.source.clone(), target: req.target.clone() })))
        };
        self.oplog.wait_flushed(seq).await;
        info!("snapshot {} -> {} done", req.source, req.target);
        Ok(SnapshotResponse {})
    }

    pub async fn find_matching_files(&self, req: FindMatchingFilesRequest) -> Result<FindMatchingFilesResponse, Status> {
        check_path(&req.directory, true)?;
        let _locks = self.locks.acquire(LockTable::for_path(&req.directory, LockMode::Read)).await;
        let state = self.state.lock();
        if state.files.exists(&req.directory) {
            return Err(Status::invalid_argument(format!("{} is a file", req.directory)));
        }
        if req.directory != "/" && !state.files.is_directory(&req.directory) {
            return Err(Status::not_found(&req.directory));
        }
        let entries = state
            .files
            .list(&req.directory, req.include_hidden)
            .into_iter()
            .map(|entry| DirEntry { name: entry.name, is_directory: entry.is_directory })
            .collect();
        Ok(FindMatchingFilesResponse { entries })
    }

    pub async fn find_location(&self, req: FindLocationRequest) -> Result<FindLocationResponse, Status> {
        check_path(&req.path, false)?;
        let _locks = self.locks.acquire(LockTable::for_path(&req.path, LockMode::Read)).await;
        let state = self.state.lock();
        let Some(meta) = state.files.find(&req.path) else { return Err(Status::not_found(&req.path)) };
        let count = (req.count as u64).max(1);
        let mut chunks = Vec::new();
        let mut index = req.first_index;
        while (index as usize) < meta.chunks.len() && index < req.first_index.saturating_add(count) {
            let handle = meta.chunks[index as usize];
            if let Some(chunk) = state.chunks.find(handle) {
                chunks.push(self.chunk_info(index, handle, chunk));
            }
            index += 1;
        }
        Ok(FindLocationResponse { chunks })
    }

    async fn copy_on_write(&self, path: &str, index: u64, old_handle: u64) -> u64 {
        let _cow = self.cow.lock().await;
        let holders: Vec<String> = {
            let state = self.state.lock();
            let current = state.files.find(path).and_then(|file| file.chunks.get(index as usize).copied());
            match current {
                Some(handle) if handle == old_handle => {}
                Some(handle) => return handle,
                None => return 0,
            }
            let Some(meta) = state.chunks.find(old_handle) else { return 0 };
            if meta.refcount <= 1 {
                return old_handle;
            }
            meta.locations.keys().filter(|id| self.registry.resolve(id).is_some()).cloned().collect()
        };
        let fresh = self.alloc_handle().await;
        let ok = self.create_chunk_on(&holders, fresh, 1, old_handle).await;
        let seq = {
            let mut state = self.state.lock();
            if ok.len() < self.config.min_replicas_for_write as usize || ok.is_empty() {
                state.chunks.erase(fresh);
                warn!("copy-on-write of chunk {old_handle} failed, not enough replicas");
                return 0;
            }
            let t = now();
            for id in &ok {
                state.chunks.add_location(fresh, id, t);
            }
            state.apply_replace_chunk(path, index, fresh);
            self.oplog.append(&record(Body::ReplaceChunk(ReplaceChunkRecord { path: path.to_string(), index, handle: fresh })))
        };
        self.oplog.wait_flushed(seq).await;
        info!("copy-on-write: {path} chunk {index} {old_handle} -> {fresh}");
        fresh
    }

    pub async fn find_lease_holder(&self, req: FindLeaseHolderRequest) -> Result<FindLeaseHolderResponse, Status> {
        check_path(&req.path, false)?;
        let _locks = self.locks.acquire(LockTable::for_path(&req.path, LockMode::Read)).await;
        loop {
            let (handle, shared) = {
                let state = self.state.lock();
                let Some(file) = state.files.find(&req.path) else { return Err(Status::not_found(&req.path)) };
                let Some(&handle) = file.chunks.get(req.index as usize) else {
                    return Err(Status::not_found(format!("no chunk {} in {}", req.index, req.path)));
                };
                let Some(meta) = state.chunks.find(handle) else { return Err(Status::not_found("chunk metadata missing")) };
                (handle, meta.refcount > 1)
            };
            let handle = if shared {
                let fresh = self.copy_on_write(&req.path, req.index, handle).await;
                if fresh == 0 {
                    return Ok(FindLeaseHolderResponse { code: ResultCode::NoReplicas as i32, ..Default::default() });
                }
                fresh
            } else {
                handle
            };
            let wait_until = {
                let mut state = self.state.lock();
                let t = now();
                let Some(meta) = state.chunks.find_mut(handle) else { return Err(Status::not_found("chunk metadata missing")) };
                if self.leases.is_valid(meta, t) {
                    let primary = meta.lease.as_ref().map(|lease| lease.primary.clone()).unwrap_or_default();
                    return Ok(FindLeaseHolderResponse {
                        code: ResultCode::Ok as i32,
                        handle,
                        version: meta.version,
                        primary: self.registry.resolve(&primary),
                        secondaries: self.resolve_locations(meta).into_iter().filter(|r| r.chunkserver_id != primary).collect(),
                    });
                }
                if self.leases.is_pending_expiry(meta, t) {
                    Some(self.leases.wait_until(meta))
                } else {
                    meta.lease = None;
                    None
                }
            };
            if let Some(until) = wait_until {
                info!("chunk {handle} lease pending expiry, deferring FindLeaseHolder");
                sleep_until(tokio::time::Instant::from_std(until)).await;
                continue;
            }
            let result = self.leases.grant(handle).await;
            let mut resp = FindLeaseHolderResponse { code: result.code as i32, ..Default::default() };
            if result.code == ResultCode::Ok {
                resp.handle = handle;
                resp.version = result.version;
                resp.primary = result.primary;
                resp.secondaries = result.secondaries;
            }
            return Ok(resp);
        }
    }

    pub async fn add_chunk(&self, req: AddChunkRequest) -> Result<AddChunkResponse, Status> {
        check_path(&req.path, false)?;
        let _locks = self.locks.acquire(LockTable::for_path(&req.path, LockMode::Write)).await;
        let existing = {
            let state = self.state.lock();
            let Some(file) = state.files.find(&req.path) else { return Err(Status::not_found(&req.path)) };
            let count = file.chunks.len() as u64;
            if req.index < count {
                let handle = file.chunks[req.index as usize];
                Some(AddChunkResponse {
                    code: ResultCode::Ok as i32,
                    chunk: state.chunks.find(handle).map(|meta| self.chunk_info(req.index, handle, meta)),
                })
            } else if req.index > count {
                return Err(Status::invalid_argument(format!("chunk index {} would leave a hole", req.index)));
            } else {
                None
            }
        };
        if let Some(resp) = existing {
            return Ok(resp);
        }
        let handle = self.alloc_handle().await;
        let servers = self.choose_servers(self.config.replication_goal as usize);
        let ok = if servers.is_empty() { Vec::new() } else { self.create_chunk_on(&servers, handle, 1, 0).await };
        let (seq, resp) = {
            let mut state = self.state.lock();
            if ok.len() < self.config.min_replicas_for_write as usize || ok.is_empty() {
                state.chunks.erase(handle);
                warn!("AddChunk {} index {} failed: {} of {} replicas created", req.path, req.index, ok.len(), servers.len());
                (0, AddChunkResponse { code: ResultCode::NoReplicas as i32, chunk: None })
            } else {
                let t = now();
                for id in &ok {
                    state.chunks.add_location(handle, id, t);
                }
                state.apply_add_chunk(&req.path, req.index, handle);
                let seq = self.oplog.append(&record(Body::AddChunk(AddChunkRecord { path: req.path.clone(), index: req.index, handle })));
                let chunk = state.chunks.find(handle).map(|meta| self.chunk_info(req.index, handle, meta));
                (seq, AddChunkResponse { code: ResultCode::Ok as i32, chunk })
            }
        };
        if seq > 0 {
            self.oplog.wait_flushed(seq).await;
        }
        Ok(resp)
    }

    pub fn heart_beat(&self, req: HeartBeatRequest) -> HeartBeatResponse {
        let t = now();
        self.registry.touch(&req.chunkserver_id, &req.address, &req.rack, t);
        let id = req.chunkserver_id.as_str();
        let mut resp = HeartBeatResponse::default();
        let mut regrants = Vec::new();
        {
            let mut state = self.state.lock();
            let known = state.chunks.held_by(id);
            let mut reported: BTreeSet<u64> = BTreeSet::new();
            for report in &req.chunks {
                let handle = report.handle;
                let (granting, version) = match state.chunks.find(handle) {
                    None => {
                        resp.delete_handles.push(handle);
                        continue;
                    }
                    Some(meta) => (meta.granting, meta.version),
                };
                if granting {
                    if report.version + 1 < version {
                        resp.delete_handles.push(handle);
                        state.chunks.remove_location(handle, id);
                        continue;
                    }
                    reported.insert(handle);
                    state.chunks.add_location(handle, id, t);
                    continue;
                }
                if report.version < version {
                    resp.delete_handles.push(handle);
                    if state.chunks.remove_location(handle, id) && self.lease_valid(&state, handle, t) {
                        regrants.push(handle);
                    }
                    warn!("chunk {handle} on {id} is stale: version {} < {version}", report.version);
                    continue;
                }
                if report.version > version {
                    warn!("chunk {handle} reported at version {} above ours {version}, adopting", report.version);
                    if let Some(meta) = state.chunks.find_mut(handle) {
                        meta.version = report.version;
                    }
                    self.oplog.append(&record(Body::BumpVersion(BumpVersionRecord { handle, version: report.version })));
                }
                reported.insert(handle);
                if state.chunks.add_location(handle, id, t) && self.lease_valid(&state, handle, t) {
                    regrants.push(handle);
                }
            }
            for handle in known {
                if reported.contains(&handle) {
                    continue;
                }
                let recently_added = match state.chunks.find(handle) {
                    None => continue,
                    Some(meta) => meta.locations.get(id).is_some_and(|since| t.duration_since(*since) < self.config.heartbeat_interval * 2),
                };
                if recently_added {
                    continue;
                }
                state.chunks.remove_location(handle, id);
                if self.lease_valid(&state, handle, t) {
                    regrants.push(handle);
                }
            }
            for &handle in &req.corrupt {
                resp.delete_handles.push(handle);
                if state.chunks.find(handle).is_none() {
                    continue;
                }
                if state.chunks.remove_location(handle, id) {
                    warn!("chunk {handle} on {id} reported corrupt");
                }
                if self.lease_valid(&state, handle, t) {
                    regrants.push(handle);
                }
            }
            for &handle in &req.lease_extension_requests {
                let Some(meta) = state.chunks.find_mut(handle) else { continue };
                if !self.leases.extend_locked(meta, id, t) {
                    continue;
                }
                resp.extended.push(LeaseExtension { handle, lease_ms: self.config.lease_duration.as_millis() as u64 });
            }
        }
        for handle in regrants {
            self.leases.request_regrant(handle);
        }
        resp
    }

    fn lease_valid(&self, state: &MasterState, handle: u64, t: Instant) -> bool {
        state.chunks.find(handle).is_some_and(|meta| self.leases.is_valid(meta, t))
    }

    pub fn sweep_dead(&self) {
        let t = now();
        let dead = self.registry.sweep(t);
        if dead.is_empty() {
            return;
        }
        let mut regrants = Vec::new();
        {
            let mut state = self.state.lock();
            for id in &dead {
                for handle in state.chunks.held_by(id) {
                    state.chunks.remove_location(handle, id);
                    if self.lease_valid(&state, handle, t) {
                        regrants.push(handle);
                    }
                }
            }
        }
        for handle in regrants {
            self.leases.request_regrant(handle);
        }
    }

    pub async fn gc_pass(&self) {
        let now_seconds = unix_seconds();
        let retention_seconds = self.config.deleted_file_retention.as_secs() as i64;
        let expired: Vec<String> = {
            let state = self.state.lock();
            state
                .files
                .iter()
                .filter(|(path, _)| parse_hidden_name(path).is_some_and(|(stamp, _)| stamp + retention_seconds <= now_seconds))
                .map(|(path, _)| path.clone())
                .collect()
        };
        let mut last_seq = 0;
        for path in expired {
            let _locks = self.locks.acquire(LockTable::for_path(&path, LockMode::Write)).await;
            let mut state = self.state.lock();
            if !state.files.exists(&path) {
                continue;
            }
            state.apply_remove(&path);
            last_seq = self.oplog.append(&record(Body::Remove(RemoveRecord { path: path.clone() })));
            info!("gc removed {path}");
        }
        {
            let mut state = self.state.lock();
            let referenced: HashSet<u64> = state.files.iter().flat_map(|(_, meta)| meta.chunks.iter().copied()).collect();
            let orphans: Vec<u64> = state
                .chunks
                .iter()
                .filter(|(handle, meta)| !meta.pending && !referenced.contains(handle))
                .map(|(handle, _)| *handle)
                .collect();
            for handle in orphans {
                state.apply_drop_chunk(handle);
                last_seq = self.oplog.append(&record(Body::DropChunk(DropChunkRecord { handle })));
                info!("gc dropped orphan chunk {handle}");
            }
        }
        if last_seq > 0 {
            self.oplog.wait_flushed(last_seq).await;
        }
    }
}

impl Drop for Master {
    fn drop(&mut self) {
        self.stop();
    }
}

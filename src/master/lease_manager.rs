use std::collections::{BTreeSet, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};

use futures::future::join_all;
use parking_lot::Mutex;
use tokio::sync::Notify;
use tokio::time::timeout;
use tracing::{info, warn};

use crate::common::clock::now;
use crate::common::config::Config;
use crate::master::chunk_table::{ChunkMeta, Lease};
use crate::master::chunkserver_registry::ChunkserverRegistry;
use crate::master::master_state::MasterState;
use crate::master::oplog::OpLog;
use crate::rpc::{GrantLeaseRequest, Replica, ResultCode, RevokeLeaseRequest, UpdateVersionRequest};
use crate::state::log_record::Body;
use crate::state::{BumpVersionRecord, LogRecord};

#[derive(Clone, Debug)]
pub struct GrantResult {
    pub code: ResultCode,
    pub version: u64,
    pub primary: Option<Replica>,
    pub secondaries: Vec<Replica>,
}

impl GrantResult {
    fn failed(code: ResultCode) -> GrantResult {
        GrantResult { code, version: 0, primary: None, secondaries: Vec::new() }
    }
}

#[derive(Clone, Debug, Default)]
pub struct RevokeResult {
    pub had_lease: bool,
    pub acked: bool,
    pub expiry: Option<Instant>,
}

struct Attempt {
    primary: String,
    secondaries: Vec<String>,
    version: u64,
}

#[derive(Default)]
struct Busy {
    handles: HashSet<u64>,
    regrant_pending: HashSet<u64>,
}

pub struct LeaseManager {
    config: Config,
    state: Arc<Mutex<MasterState>>,
    registry: Arc<ChunkserverRegistry>,
    oplog: Arc<OpLog>,
    busy: Mutex<Busy>,
    busy_changed: Notify,
}

impl LeaseManager {
    pub fn new(config: Config, state: Arc<Mutex<MasterState>>, registry: Arc<ChunkserverRegistry>, oplog: Arc<OpLog>) -> LeaseManager {
        LeaseManager { config, state, registry, oplog, busy: Mutex::new(Busy::default()), busy_changed: Notify::new() }
    }

    pub fn is_valid(&self, meta: &ChunkMeta, now: Instant) -> bool {
        let Some(lease) = &meta.lease else { return false };
        if lease.revoked || now > lease.expiry + self.config.lease_clock_skew_margin {
            return false;
        }
        meta.locations.contains_key(&lease.primary)
    }

    pub fn is_pending_expiry(&self, meta: &ChunkMeta, now: Instant) -> bool {
        let Some(lease) = &meta.lease else { return false };
        if now > lease.expiry + self.config.lease_clock_skew_margin {
            return false;
        }
        lease.revoked || !meta.locations.contains_key(&lease.primary)
    }

    pub fn wait_until(&self, meta: &ChunkMeta) -> Instant {
        let expiry = meta.lease.as_ref().map(|lease| lease.expiry).unwrap_or_else(now);
        expiry + self.config.lease_clock_skew_margin + Duration::from_millis(1)
    }

    pub fn extend_locked(&self, meta: &mut ChunkMeta, primary: &str, now: Instant) -> bool {
        if !self.is_valid(meta, now) {
            return false;
        }
        let Some(lease) = meta.lease.as_mut() else { return false };
        if lease.primary != primary {
            return false;
        }
        lease.expiry = now + self.config.lease_duration;
        true
    }

    async fn begin_handle(&self, handle: u64) {
        loop {
            let notified = self.busy_changed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self.busy.lock().handles.insert(handle) {
                return;
            }
            notified.await;
        }
    }

    fn end_handle(&self, handle: u64) {
        self.busy.lock().handles.remove(&handle);
        self.busy_changed.notify_waiters();
    }

    fn prepare_locked(&self, state: &mut MasterState, handle: u64) -> Option<Attempt> {
        let meta = state.chunks.find_mut(handle)?;
        let live: Vec<String> = meta.locations.keys().filter(|id| self.registry.resolve(id).is_some()).cloned().collect();
        if live.len() < self.config.min_replicas_for_write as usize || live.is_empty() {
            return None;
        }
        let mut primary = live[0].clone();
        if let Some(lease) = &meta.lease
            && live.contains(&lease.primary)
        {
            primary = lease.primary.clone();
        }
        let secondaries = live.into_iter().filter(|id| *id != primary).collect();
        let version = meta.version + 1;
        meta.granting = true;
        Some(Attempt { primary, secondaries, version })
    }

    fn clear_granting(&self, handle: u64) {
        if let Some(meta) = self.state.lock().chunks.find_mut(handle) {
            meta.granting = false;
        }
    }

    async fn grant_lease_acked(&self, id: &str, request: GrantLeaseRequest) -> bool {
        let Some(mut client) = self.registry.stub(id) else { return false };
        matches!(timeout(self.config.rpc_deadline, client.grant_lease(request)).await, Ok(Ok(resp)) if resp.get_ref().code() == ResultCode::Ok)
    }

    async fn update_version_acked(&self, id: &str, request: UpdateVersionRequest) -> bool {
        let Some(mut client) = self.registry.stub(id) else { return false };
        matches!(timeout(self.config.rpc_deadline, client.update_version(request)).await, Ok(Ok(resp)) if resp.get_ref().code() == ResultCode::Ok)
    }

    async fn revoke_lease_acked(&self, id: &str, request: RevokeLeaseRequest) -> bool {
        let Some(mut client) = self.registry.stub(id) else { return false };
        matches!(timeout(self.config.rpc_deadline, client.revoke_lease(request)).await, Ok(Ok(resp)) if resp.get_ref().code() == ResultCode::Ok)
    }

    async fn grant_serialized(&self, handle: u64) -> GrantResult {
        for _round in 0..8 {
            let attempt = {
                let mut state = self.state.lock();
                self.prepare_locked(&mut state, handle)
            };
            let Some(attempt) = attempt else {
                self.clear_granting(handle);
                return GrantResult::failed(ResultCode::NoReplicas);
            };
            let secondary_replicas: Vec<Replica> = attempt.secondaries.iter().filter_map(|id| self.registry.resolve(id)).collect();
            let grant_request = GrantLeaseRequest {
                handle,
                version: attempt.version,
                lease_ms: self.config.lease_duration.as_millis() as u64,
                secondaries: secondary_replicas,
            };
            let primary_call = self.grant_lease_acked(&attempt.primary, grant_request);
            let secondary_calls = join_all(
                attempt
                    .secondaries
                    .iter()
                    .map(|id| self.update_version_acked(id, UpdateVersionRequest { handle, version: attempt.version })),
            );
            let (primary_ok, secondary_oks) = tokio::join!(primary_call, secondary_calls);
            let mut failed: BTreeSet<String> = BTreeSet::new();
            if !primary_ok {
                failed.insert(attempt.primary.clone());
            }
            for (id, ok) in attempt.secondaries.iter().zip(secondary_oks) {
                if !ok {
                    failed.insert(id.clone());
                }
            }

            let (seq, result) = {
                let mut state = self.state.lock();
                if state.chunks.find(handle).is_none() {
                    return GrantResult::failed(ResultCode::NoSuchChunk);
                }
                for id in &failed {
                    warn!("chunk {handle} replica {id} did not ack version {}, dropping it", attempt.version);
                    state.chunks.remove_location(handle, id);
                }
                if !failed.is_empty() {
                    continue;
                }
                let meta = state.chunks.find_mut(handle).expect("checked above");
                meta.version = meta.version.max(attempt.version);
                let version = meta.version;
                let seq = self.oplog.append(&LogRecord { body: Some(Body::BumpVersion(BumpVersionRecord { handle, version })) });
                meta.granting = false;
                meta.lease = Some(Lease { primary: attempt.primary.clone(), expiry: now() + self.config.lease_duration, revoked: false });
                let result = GrantResult {
                    code: ResultCode::Ok,
                    version,
                    primary: self.registry.resolve(&attempt.primary),
                    secondaries: attempt
                        .secondaries
                        .iter()
                        .filter(|id| !failed.contains(*id))
                        .filter_map(|id| self.registry.resolve(id))
                        .collect(),
                };
                (seq, result)
            };
            self.oplog.wait_flushed(seq).await;
            info!("chunk {handle} lease granted to {} at version {}", attempt.primary, result.version);
            return result;
        }
        self.clear_granting(handle);
        GrantResult::failed(ResultCode::NoReplicas)
    }

    pub async fn grant(&self, handle: u64) -> GrantResult {
        self.begin_handle(handle).await;
        let result = self.grant_serialized(handle).await;
        self.end_handle(handle);
        result
    }

    pub fn request_regrant(self: &Arc<Self>, handle: u64) {
        if !self.busy.lock().regrant_pending.insert(handle) {
            return;
        }
        let this = self.clone();
        tokio::spawn(async move { this.regrant(handle).await });
    }

    pub async fn regrant(&self, handle: u64) {
        self.begin_handle(handle).await;
        self.busy.lock().regrant_pending.remove(&handle);
        let needed = {
            let state = self.state.lock();
            state.chunks.find(handle).is_some_and(|meta| self.is_valid(meta, now()))
        };
        if needed {
            let result = self.grant_serialized(handle).await;
            if result.code != ResultCode::Ok {
                warn!("regrant of chunk {handle} failed with code {}", result.code.as_str_name());
            }
        }
        self.end_handle(handle);
    }

    pub async fn revoke(&self, handle: u64) -> RevokeResult {
        let mut result = RevokeResult::default();
        let primary = {
            let mut state = self.state.lock();
            let Some(meta) = state.chunks.find_mut(handle) else { return result };
            let Some(lease) = meta.lease.as_mut() else { return result };
            if now() > lease.expiry + self.config.lease_clock_skew_margin {
                meta.lease = None;
                return result;
            }
            result.had_lease = true;
            result.expiry = Some(lease.expiry);
            lease.revoked = true;
            lease.primary.clone()
        };
        let acked = self.revoke_lease_acked(&primary, RevokeLeaseRequest { handle }).await;
        let mut state = self.state.lock();
        if let Some(meta) = state.chunks.find_mut(handle)
            && acked
            && meta.lease.as_ref().is_some_and(|lease| lease.primary == primary)
        {
            meta.lease = None;
            result.acked = true;
        }
        if !acked {
            warn!("chunk {handle} revoke not acked by {primary}, waiting for expiry");
        }
        result
    }
}

use std::collections::{BTreeMap, HashMap};
use std::time::{Duration, Instant};

use parking_lot::Mutex;
use tonic::transport::Channel;
use tracing::{info, warn};

use crate::common::config::Config;
use crate::common::net::lazy_channel;
use crate::rpc::Replica;
use crate::rpc::chunkserver_client::ChunkserverClient;

#[derive(Clone, Debug)]
pub struct ChunkserverInfo {
    pub id: String,
    pub address: String,
    pub rack: String,
    pub last_heartbeat: Instant,
    pub alive: bool,
}

pub struct ChunkserverRegistry {
    config: Config,
    servers: Mutex<BTreeMap<String, ChunkserverInfo>>,
    channels: Mutex<HashMap<String, Channel>>,
}

impl ChunkserverRegistry {
    pub fn new(config: Config) -> ChunkserverRegistry {
        ChunkserverRegistry { config, servers: Mutex::new(BTreeMap::new()), channels: Mutex::new(HashMap::new()) }
    }

    pub fn touch(&self, id: &str, address: &str, rack: &str, when: Instant) -> bool {
        let mut servers = self.servers.lock();
        match servers.get_mut(id) {
            None => {
                info!("chunkserver {id} joined at {address} rack {rack}");
                servers.insert(
                    id.to_string(),
                    ChunkserverInfo {
                        id: id.to_string(),
                        address: address.to_string(),
                        rack: rack.to_string(),
                        last_heartbeat: when,
                        alive: true,
                    },
                );
                true
            }
            Some(info) => {
                let newly_alive = !info.alive;
                if !info.alive {
                    info!("chunkserver {id} is back at {address}");
                } else if info.address != address {
                    info!("chunkserver {id} moved to {address}");
                }
                info.address = address.to_string();
                info.rack = rack.to_string();
                info.last_heartbeat = when;
                info.alive = true;
                newly_alive
            }
        }
    }

    pub fn get(&self, id: &str) -> Option<ChunkserverInfo> {
        self.servers.lock().get(id).cloned()
    }

    pub fn resolve(&self, id: &str) -> Option<Replica> {
        let servers = self.servers.lock();
        let info = servers.get(id)?;
        if !info.alive {
            return None;
        }
        Some(Replica { chunkserver_id: id.to_string(), address: info.address.clone(), rack: info.rack.clone() })
    }

    pub fn alive(&self) -> Vec<ChunkserverInfo> {
        self.servers.lock().values().filter(|info| info.alive).cloned().collect()
    }

    pub fn sweep(&self, now: Instant) -> Vec<String> {
        let mut dead = Vec::new();
        for (id, info) in self.servers.lock().iter_mut() {
            if info.alive && now.duration_since(info.last_heartbeat) > self.config.chunkserver_dead_timeout {
                info.alive = false;
                dead.push(id.clone());
                warn!("chunkserver {id} at {} declared dead", info.address);
            }
        }
        dead
    }

    pub fn stub(&self, id: &str) -> Option<ChunkserverClient<Channel>> {
        let address = self.servers.lock().get(id)?.address.clone();
        let channel = self.channels.lock().entry(address.clone()).or_insert_with(|| lazy_channel(&address)).clone();
        Some(ChunkserverClient::new(channel))
    }

    pub fn deadline(&self) -> Duration {
        self.config.rpc_deadline
    }
}

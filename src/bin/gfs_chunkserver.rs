use std::path::Path;
use std::process::ExitCode;
use std::sync::Arc;

use gfs::chunkserver::chunk_store::{ChunkStore, load_or_create_chunkserver_id};
use gfs::chunkserver::chunkserver::Chunkserver;
use gfs::chunkserver::service::ChunkserverService;
use gfs::common::config::Config;
use gfs::common::logging;
use gfs::common::net::{advertised_address, bind, shutdown_signal};
use tonic::transport::Server;
use tracing::{error, info};

#[tokio::main]
async fn main() -> ExitCode {
    logging::init("chunkserver");
    let config = Config::from_args(std::env::args().skip(1));
    if config.data_dir.is_empty() {
        eprintln!("--data_dir is required\n{}", Config::usage());
        return ExitCode::from(2);
    }
    let store = Arc::new(ChunkStore::new(&config.data_dir, config.chunk_size, config.checksum_block_size));
    store.scan();
    let id = load_or_create_chunkserver_id(Path::new(&config.data_dir));
    let chunkserver = Chunkserver::new(config.clone(), id.clone(), store);
    let (incoming, addr) = match bind(&config.listen).await {
        Ok(bound) => bound,
        Err(e) => {
            error!("could not listen on {}: {e}", config.listen);
            return ExitCode::from(1);
        }
    };
    let advertise = if config.advertise.is_empty() { advertised_address(&config.listen, addr) } else { config.advertise.clone() };
    chunkserver.set_advertise_address(advertise);
    chunkserver.start_heartbeat();
    info!("chunkserver {id} listening on {addr}, data in {}, master {}", config.data_dir, config.master_address);
    let served = Server::builder()
        .add_service(ChunkserverService::server(chunkserver.clone()))
        .serve_with_incoming_shutdown(incoming, shutdown_signal())
        .await;
    chunkserver.stop();
    match served {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            error!("server failed: {e}");
            ExitCode::from(1)
        }
    }
}

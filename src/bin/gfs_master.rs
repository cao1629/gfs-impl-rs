use std::process::ExitCode;

use gfs::common::config::Config;
use gfs::common::logging;
use gfs::common::net::{bind, shutdown_signal};
use gfs::master::master::Master;
use gfs::master::service::MasterService;
use tonic::transport::Server;
use tracing::{error, info};

fn main() -> ExitCode {
    logging::init("master");
    let config = Config::from_args(std::env::args().skip(1));
    if config.data_dir.is_empty() {
        eprintln!("--data_dir is required");
        return ExitCode::from(2);
    }
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(config.master_worker_threads.max(2) as usize)
        .enable_all()
        .build()
        .expect("build the tokio runtime");
    runtime.block_on(async move {
        let master = Master::new(config.clone());
        master.start();
        let (incoming, addr) = match bind(&config.listen).await {
            Ok(bound) => bound,
            Err(e) => {
                error!("cannot listen on {}: {e}", config.listen);
                return ExitCode::from(1);
            }
        };
        info!("listening on {addr}, data in {}", config.data_dir);
        let served = Server::builder()
            .add_service(MasterService::server(master.clone()))
            .serve_with_incoming_shutdown(incoming, shutdown_signal())
            .await;
        info!("shutting down");
        master.stop();
        match served {
            Ok(()) => ExitCode::SUCCESS,
            Err(e) => {
                error!("server failed: {e}");
                ExitCode::from(1)
            }
        }
    })
}

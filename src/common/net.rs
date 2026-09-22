use std::net::SocketAddr;

use tokio::net::TcpListener;
use tokio::signal::unix::{SignalKind, signal};
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::{Channel, Endpoint};

pub async fn bind(listen: &str) -> std::io::Result<(TcpListenerStream, SocketAddr)> {
    let listener = TcpListener::bind(listen).await?;
    let addr = listener.local_addr()?;
    Ok((TcpListenerStream::new(listener), addr))
}

pub fn lazy_channel(address: &str) -> Channel {
    Endpoint::from_shared(format!("http://{address}")).unwrap_or_else(|_| Endpoint::from_static("http://127.0.0.1:1")).connect_lazy()
}

pub fn advertised_address(listen: &str, bound: SocketAddr) -> String {
    match listen.rsplit_once(':') {
        Some((host, _)) if !host.is_empty() => format!("{host}:{}", bound.port()),
        _ => bound.to_string(),
    }
}

pub async fn shutdown_signal() {
    let mut terminate = signal(SignalKind::terminate()).expect("install the SIGTERM handler");
    tokio::select! {
        _ = tokio::signal::ctrl_c() => {}
        _ = terminate.recv() => {}
    }
}

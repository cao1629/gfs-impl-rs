#![allow(clippy::module_inception)]

pub mod rpc {
    tonic::include_proto!("gfs.rpc");
}

pub mod state {
    tonic::include_proto!("gfs.state");
}

pub mod chunkserver;
pub mod client;
pub mod common;
pub mod master;

use std::sync::Arc;

use tonic::{Request, Response, Status};

use crate::master::master::Master;
use crate::rpc::master_server::{Master as MasterRpc, MasterServer};
use crate::rpc::{
    AddChunkRequest, AddChunkResponse, CreateRequest, CreateResponse, DeleteRequest, DeleteResponse, FindLeaseHolderRequest,
    FindLeaseHolderResponse, FindLocationRequest, FindLocationResponse, FindMatchingFilesRequest, FindMatchingFilesResponse,
    GetClusterInfoRequest, GetClusterInfoResponse, HeartBeatRequest, HeartBeatResponse, OpenRequest, OpenResponse, RenameRequest,
    RenameResponse, SnapshotRequest, SnapshotResponse,
};

pub struct MasterService {
    master: Arc<Master>,
}

impl MasterService {
    pub fn new(master: Arc<Master>) -> MasterService {
        MasterService { master }
    }

    pub fn server(master: Arc<Master>) -> MasterServer<MasterService> {
        MasterServer::new(MasterService::new(master))
    }
}

#[tonic::async_trait]
impl MasterRpc for MasterService {
    async fn get_cluster_info(&self, request: Request<GetClusterInfoRequest>) -> Result<Response<GetClusterInfoResponse>, Status> {
        Ok(Response::new(self.master.get_cluster_info(request.into_inner())))
    }

    async fn create(&self, request: Request<CreateRequest>) -> Result<Response<CreateResponse>, Status> {
        self.master.create(request.into_inner()).await.map(Response::new)
    }

    async fn open(&self, request: Request<OpenRequest>) -> Result<Response<OpenResponse>, Status> {
        self.master.open(request.into_inner()).await.map(Response::new)
    }

    async fn delete(&self, request: Request<DeleteRequest>) -> Result<Response<DeleteResponse>, Status> {
        self.master.delete(request.into_inner()).await.map(Response::new)
    }

    async fn rename(&self, request: Request<RenameRequest>) -> Result<Response<RenameResponse>, Status> {
        self.master.rename(request.into_inner()).await.map(Response::new)
    }

    async fn snapshot(&self, request: Request<SnapshotRequest>) -> Result<Response<SnapshotResponse>, Status> {
        self.master.snapshot(request.into_inner()).await.map(Response::new)
    }

    async fn find_matching_files(&self, request: Request<FindMatchingFilesRequest>) -> Result<Response<FindMatchingFilesResponse>, Status> {
        self.master.find_matching_files(request.into_inner()).await.map(Response::new)
    }

    async fn find_location(&self, request: Request<FindLocationRequest>) -> Result<Response<FindLocationResponse>, Status> {
        self.master.find_location(request.into_inner()).await.map(Response::new)
    }

    async fn find_lease_holder(&self, request: Request<FindLeaseHolderRequest>) -> Result<Response<FindLeaseHolderResponse>, Status> {
        self.master.find_lease_holder(request.into_inner()).await.map(Response::new)
    }

    async fn add_chunk(&self, request: Request<AddChunkRequest>) -> Result<Response<AddChunkResponse>, Status> {
        self.master.add_chunk(request.into_inner()).await.map(Response::new)
    }

    async fn heart_beat(&self, request: Request<HeartBeatRequest>) -> Result<Response<HeartBeatResponse>, Status> {
        Ok(Response::new(self.master.heart_beat(request.into_inner())))
    }
}

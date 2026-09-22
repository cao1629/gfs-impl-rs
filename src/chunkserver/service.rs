use std::sync::Arc;

use tonic::{Request, Response, Status, Streaming};

use crate::chunkserver::chunkserver::{Chunkserver, message_limit};
use crate::rpc::chunkserver_server::{Chunkserver as ChunkserverRpc, ChunkserverServer};
use crate::rpc::{
    ApplyMutationRequest, ApplyMutationResponse, CreateChunkRequest, CreateChunkResponse, GetChunkLengthRequest, GetChunkLengthResponse,
    GrantLeaseRequest, GrantLeaseResponse, PushDataRequest, PushDataResponse, ReadRequest, ReadResponse, RecordAppendRequest,
    RecordAppendResponse, RevokeLeaseRequest, RevokeLeaseResponse, UpdateVersionRequest, UpdateVersionResponse, WriteRequest,
    WriteResponse,
};

pub struct ChunkserverService {
    chunkserver: Arc<Chunkserver>,
}

impl ChunkserverService {
    pub fn new(chunkserver: Arc<Chunkserver>) -> ChunkserverService {
        ChunkserverService { chunkserver }
    }

    pub fn server(chunkserver: Arc<Chunkserver>) -> ChunkserverServer<ChunkserverService> {
        let limit = message_limit(chunkserver.config().chunk_size);
        ChunkserverServer::new(ChunkserverService::new(chunkserver)).max_decoding_message_size(limit).max_encoding_message_size(limit)
    }
}

#[tonic::async_trait]
impl ChunkserverRpc for ChunkserverService {
    async fn push_data(&self, request: Request<Streaming<PushDataRequest>>) -> Result<Response<PushDataResponse>, Status> {
        Ok(Response::new(self.chunkserver.push_data(request.into_inner()).await))
    }

    async fn read(&self, request: Request<ReadRequest>) -> Result<Response<ReadResponse>, Status> {
        Ok(Response::new(self.chunkserver.read(request.into_inner()).await))
    }

    async fn write(&self, request: Request<WriteRequest>) -> Result<Response<WriteResponse>, Status> {
        Ok(Response::new(self.chunkserver.write(request.into_inner()).await))
    }

    async fn record_append(&self, request: Request<RecordAppendRequest>) -> Result<Response<RecordAppendResponse>, Status> {
        Ok(Response::new(self.chunkserver.record_append(request.into_inner()).await))
    }

    async fn get_chunk_length(&self, request: Request<GetChunkLengthRequest>) -> Result<Response<GetChunkLengthResponse>, Status> {
        Ok(Response::new(self.chunkserver.get_chunk_length(request.into_inner()).await))
    }

    async fn apply_mutation(&self, request: Request<ApplyMutationRequest>) -> Result<Response<ApplyMutationResponse>, Status> {
        Ok(Response::new(self.chunkserver.apply_mutation(request.into_inner()).await))
    }

    async fn create_chunk(&self, request: Request<CreateChunkRequest>) -> Result<Response<CreateChunkResponse>, Status> {
        Ok(Response::new(self.chunkserver.create_chunk(request.into_inner()).await))
    }

    async fn grant_lease(&self, request: Request<GrantLeaseRequest>) -> Result<Response<GrantLeaseResponse>, Status> {
        Ok(Response::new(self.chunkserver.grant_lease(request.into_inner()).await))
    }

    async fn revoke_lease(&self, request: Request<RevokeLeaseRequest>) -> Result<Response<RevokeLeaseResponse>, Status> {
        Ok(Response::new(self.chunkserver.revoke_lease(request.into_inner()).await))
    }

    async fn update_version(&self, request: Request<UpdateVersionRequest>) -> Result<Response<UpdateVersionResponse>, Status> {
        Ok(Response::new(self.chunkserver.update_version(request.into_inner()).await))
    }
}

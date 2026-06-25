//! tonic::server impl：组合 CLI RPC + Worker RPC。

pub mod cli_rpc;
pub mod worker_rpc;

use std::sync::Arc;

use cvd_proto::cvdbench as pb;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};

use crate::state::MasterState;

/// `MasterService` 的具体实现。状态全部从 [`MasterState`] 取，不在 service 上挂业务字段。
pub struct MasterServiceImpl {
    pub state: Arc<MasterState>,
}

impl MasterServiceImpl {
    #[must_use]
    pub fn new(state: Arc<MasterState>) -> Self {
        Self { state }
    }
}

#[tonic::async_trait]
impl pb::master_service_server::MasterService for MasterServiceImpl {
    type WatchJobStream = ReceiverStream<Result<pb::JobEvent, Status>>;

    // ── CLI 操作 ────────────────────────────────────────────────────────────

    async fn create_job(
        &self,
        req: Request<pb::CreateJobRequest>,
    ) -> Result<Response<pb::CreateJobResponse>, Status> {
        cli_rpc::create_job(&self.state, req.into_inner())
    }

    async fn watch_job(
        &self,
        req: Request<pb::WatchJobRequest>,
    ) -> Result<Response<Self::WatchJobStream>, Status> {
        cli_rpc::watch_job(&self.state, req.into_inner()).await
    }

    async fn query_job(
        &self,
        req: Request<pb::QueryJobRequest>,
    ) -> Result<Response<pb::QueryJobResponse>, Status> {
        cli_rpc::query_job(&self.state, req.into_inner())
    }

    async fn delete_job(
        &self,
        req: Request<pb::DeleteJobRequest>,
    ) -> Result<Response<pb::DeleteJobResponse>, Status> {
        cli_rpc::delete_job(&self.state, req.into_inner())
    }

    async fn list_jobs(
        &self,
        req: Request<pb::ListJobsRequest>,
    ) -> Result<Response<pb::ListJobsResponse>, Status> {
        cli_rpc::list_jobs(&self.state, req.into_inner())
    }

    // ── Worker 拉取 ─────────────────────────────────────────────────────────

    async fn fetch_job(
        &self,
        req: Request<pb::FetchJobRequest>,
    ) -> Result<Response<pb::FetchJobResponse>, Status> {
        worker_rpc::fetch_job(&self.state, req.into_inner())
    }

    async fn report_ready(
        &self,
        req: Request<pb::ReportReadyRequest>,
    ) -> Result<Response<pb::ReportReadyResponse>, Status> {
        worker_rpc::report_ready(&self.state, req.into_inner())
    }

    async fn fetch_file_batch(
        &self,
        req: Request<pb::FetchFileBatchRequest>,
    ) -> Result<Response<pb::FetchFileBatchResponse>, Status> {
        worker_rpc::fetch_file_batch(&self.state, req.into_inner())
    }

    async fn report_progress(
        &self,
        req: Request<pb::ReportProgressRequest>,
    ) -> Result<Response<pb::ReportProgressResponse>, Status> {
        worker_rpc::report_progress(&self.state, req.into_inner())
    }

    async fn report_result(
        &self,
        req: Request<pb::ReportResultRequest>,
    ) -> Result<Response<pb::ReportResultResponse>, Status> {
        worker_rpc::report_result(&self.state, req.into_inner())
    }
}

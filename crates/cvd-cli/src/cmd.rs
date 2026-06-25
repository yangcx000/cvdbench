//! 子命令 dispatch。

use cvd_proto::cvdbench as pb;
use cvd_proto::cvdbench::master_service_client::MasterServiceClient;
use std::time::Duration;
use tonic::transport::Channel;

pub mod create;
pub mod delete;
pub mod list;
pub mod query;
pub mod watch;

#[must_use]
pub fn is_terminal_status(status: i32) -> bool {
    matches!(
        pb::JobStatus::try_from(status).unwrap_or(pb::JobStatus::Unspecified),
        pb::JobStatus::Completed | pb::JobStatus::Failed | pb::JobStatus::Cancelled
    )
}

#[must_use]
pub fn query_is_terminal(query: &pb::QueryJobResponse) -> bool {
    query
        .job
        .as_ref()
        .is_some_and(|job| is_terminal_status(job.status))
}

pub async fn ensure_terminal_after_stream(
    client: &mut MasterServiceClient<Channel>,
    job_id: &str,
    first: pb::QueryJobResponse,
    stream_error: Option<String>,
) -> anyhow::Result<pb::QueryJobResponse> {
    if query_is_terminal(&first) {
        return Ok(first);
    }

    for _ in 0..10 {
        tokio::time::sleep(Duration::from_millis(200)).await;
        let next = client
            .query_job(pb::QueryJobRequest {
                job_id: job_id.to_owned(),
            })
            .await
            .map_err(|e| anyhow::anyhow!("QueryJob: {e}"))?
            .into_inner();
        if query_is_terminal(&next) {
            return Ok(next);
        }
    }

    let suffix = stream_error
        .map(|e| format!("; last stream error: {e}"))
        .unwrap_or_default();
    Err(anyhow::anyhow!(
        "WatchJob stream ended before job reached terminal state{suffix}; rerun `cvd-cli watch {job_id}` to continue"
    ))
}

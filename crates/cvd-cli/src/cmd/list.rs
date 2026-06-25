//! `cvd-cli list [--status ...] [--limit ...]`：ListJobs，简表打印。

use cvd_proto::cvdbench as pb;
use cvd_proto::cvdbench::master_service_client::MasterServiceClient;

use crate::endpoint;

/// proto §4.1：limit 0 = server 默认 100；上限 1000；负值视为 0。
const LIMIT_HARD_CAP: i32 = 1000;

pub async fn run(master: &str, status: Option<&str>, limit: i32) -> anyhow::Result<()> {
    let endpoint = endpoint::resolve(master)?;
    let mut client = MasterServiceClient::connect(endpoint)
        .await
        .map_err(|e| anyhow::anyhow!("connect master {master}: {e}"))?;

    let status_filter = match status {
        None => None,
        Some(s) => Some(parse_status(s)?),
    };

    // CLI 端 clamp 到 [0, 1000]；负数收敛为 0（=server default 100）。
    let clamped_limit = if limit < 0 {
        eprintln!("warning: --limit {limit} is negative; using server default (0)");
        0
    } else if limit > LIMIT_HARD_CAP {
        eprintln!("warning: --limit {limit} exceeds {LIMIT_HARD_CAP}; clamping");
        LIMIT_HARD_CAP
    } else {
        limit
    };

    let resp = client
        .list_jobs(pb::ListJobsRequest {
            status_filter: status_filter.map(i32::from),
            limit: clamped_limit,
        })
        .await
        .map_err(|e| anyhow::anyhow!("ListJobs: {e}"))?
        .into_inner();

    if resp.jobs.is_empty() {
        println!("(no jobs)");
        return Ok(());
    }

    println!("{:<40}  {:<10}  CREATED_AT_MS", "JOB_ID", "STATUS");
    for job in &resp.jobs {
        let status = status_name(job.status);
        println!("{:<40}  {:<10}  {}", job.job_id, status, job.created_at);
    }
    Ok(())
}

fn parse_status(s: &str) -> anyhow::Result<pb::JobStatus> {
    match s.to_ascii_lowercase().as_str() {
        "pending" => Ok(pb::JobStatus::Pending),
        "preparing" => Ok(pb::JobStatus::Preparing),
        "running" => Ok(pb::JobStatus::Running),
        "completed" => Ok(pb::JobStatus::Completed),
        "failed" => Ok(pb::JobStatus::Failed),
        "cancelled" => Ok(pb::JobStatus::Cancelled),
        other => Err(anyhow::anyhow!(
            "unknown --status {other:?}; expected pending/preparing/running/completed/failed/cancelled"
        )),
    }
}

fn status_name(s: i32) -> &'static str {
    match pb::JobStatus::try_from(s).unwrap_or(pb::JobStatus::Unspecified) {
        pb::JobStatus::Pending => "PENDING",
        pb::JobStatus::Preparing => "PREPARING",
        pb::JobStatus::Running => "RUNNING",
        pb::JobStatus::Completed => "COMPLETED",
        pb::JobStatus::Failed => "FAILED",
        pb::JobStatus::Cancelled => "CANCELLED",
        pb::JobStatus::Unspecified => "?",
    }
}

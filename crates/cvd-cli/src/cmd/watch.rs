//! `cvd-cli watch <job_id>`：连接 master，流式接收 JobEvent 并实时渲染（spec §7）。

use std::time::Duration;

use cvd_proto::cvdbench as pb;
use cvd_proto::cvdbench::master_service_client::MasterServiceClient;
use tokio_stream::StreamExt;

use crate::cmd;
use crate::display;
use crate::display::live::LiveDisplay;
use crate::endpoint;
use crate::signal::{self, CtrlCAction};

pub async fn run(master: &str, job_id: &str, verbose: bool) -> anyhow::Result<()> {
    let endpoint = endpoint::resolve(master)?;
    let mut client = MasterServiceClient::connect(endpoint)
        .await
        .map_err(|e| anyhow::anyhow!("connect master {master}: {e}"))?;
    // QueryJob 一次拿 spec 算 total duration（用于 Elapsed/Total 列）。同时保留
    // 首次查询结果，终态 job 可以直接用 Query 的完整聚合结果渲染，避免 WatchJob
    // 快照只有 latest_progress 导致 Duration/worker 明细不完整。
    let initial_query = client
        .query_job(pb::QueryJobRequest {
            job_id: job_id.to_owned(),
        })
        .await
        .ok()
        .map(tonic::Response::into_inner);

    let total_duration_ms = initial_query
        .as_ref()
        .and_then(|q| q.job.as_ref())
        .and_then(|j| j.spec.as_ref())
        .and_then(|s| {
            cvd_common::parse::duration::parse_duration(&s.duration)
                .ok()
                .flatten()
        })
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0);

    let mut display = LiveDisplay::new_with_options(job_id.to_owned(), total_duration_ms, verbose);
    if let Some(q) = &initial_query {
        if cmd::query_is_terminal(q) {
            display.apply(&event_from_query(q));
            display.render();
            println!();
            display::summary::print_with_options(q, display::summary::SummaryOptions { verbose });
            return Ok(());
        }
    }

    let mut events = client
        .watch_job(pb::WatchJobRequest {
            job_id: job_id.to_owned(),
        })
        .await
        .map_err(|e| anyhow::anyhow!("WatchJob: {e}"))?
        .into_inner();
    let mut detached = false;
    let mut stream_error = None;
    let mut ticker = tokio::time::interval(Duration::from_secs(2));
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    ticker.tick().await;

    loop {
        tokio::select! {
            biased;
            _ = tokio::signal::ctrl_c() => {
                match signal::prompt_action_async().await {
                    CtrlCAction::Detach => {
                        eprintln!("detached; job continues on master.");
                        detached = true;
                        break;
                    }
                    CtrlCAction::Cancel | CtrlCAction::DefaultCancel => {
                        let _ = signal::cancel_job(&mut client, job_id).await;
                    }
                }
            }
            evt = events.next() => match evt {
                Some(Ok(e)) => {
                    display.apply(&e);
                    if display.should_render() {
                        display.render();
                    }
                }
                Some(Err(s)) => {
                    tracing::warn!(code = ?s.code(), msg = %s.message(), "WatchJob stream error");
                    stream_error = Some(format!("{}: {}", s.code(), s.message()));
                    break;
                }
                None => break,
            },
            _ = ticker.tick() => {
                if display.should_render() {
                    display.render();
                }
            }
        }
    }
    println!();

    if detached {
        return Ok(());
    }

    let q = client
        .query_job(pb::QueryJobRequest {
            job_id: job_id.to_owned(),
        })
        .await
        .map_err(|e| anyhow::anyhow!("QueryJob: {e}"))?
        .into_inner();

    let q = cmd::ensure_terminal_after_stream(&mut client, job_id, q, stream_error).await?;

    display::summary::print_with_options(&q, display::summary::SummaryOptions { verbose });
    Ok(())
}

fn event_from_query(query: &pb::QueryJobResponse) -> pb::JobEvent {
    let job = query.job.as_ref();
    pb::JobEvent {
        job_id: job.map_or_else(String::new, |j| j.job_id.clone()),
        status: job.map_or(pb::JobStatus::Unspecified.into(), |j| j.status),
        worker_progress: Vec::new(),
        aggregated: query.aggregated.clone(),
        error: query.error.clone(),
        timestamp: 0,
        seq: 0,
        kind: pb::EventKind::StatusChange.into(),
        dirs_scanned: query.dirs_scanned,
        files_scanned: query.files_scanned,
        scan_duration_ms: query.scan_duration_ms,
    }
}

//! `cvd-cli create`：解析 job.json → CreateJob → 默认 watch 直到终态。

use std::path::{Path, PathBuf};
use std::time::Duration;

use cvd_proto::cvdbench as pb;
use cvd_proto::cvdbench::master_service_client::MasterServiceClient;
use tokio_stream::StreamExt;

use crate::cmd;
use crate::display::live::LiveDisplay;
use crate::signal::{self, CtrlCAction};
use crate::{display, endpoint, job_input, output};

pub async fn run(
    master: &str,
    config: &Path,
    output_path: Option<PathBuf>,
    verbose: bool,
) -> anyhow::Result<()> {
    let spec = job_input::load_from_path(config)?;
    let endpoint = endpoint::resolve(master)?;
    let mut client = MasterServiceClient::connect(endpoint)
        .await
        .map_err(|e| anyhow::anyhow!("connect master {master}: {e}"))?;

    // duration 用于 LiveDisplay 的 Total 列；解析失败则按 0（显示 00:00:00）。
    let total_duration_ms = cvd_common::parse::duration::parse_duration(&spec.duration)
        .ok()
        .flatten()
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0);

    let job_id = client
        .create_job(pb::CreateJobRequest { spec: Some(spec) })
        .await
        .map_err(|e| anyhow::anyhow!("CreateJob: {e}"))?
        .into_inner()
        .job_id;
    println!("created job {job_id}");
    println!("(streaming events; press Ctrl+C to detach or cancel)");

    let mut events = client
        .watch_job(pb::WatchJobRequest {
            job_id: job_id.clone(),
        })
        .await
        .map_err(|e| anyhow::anyhow!("WatchJob: {e}"))?
        .into_inner();

    let mut display = LiveDisplay::new_with_options(job_id.clone(), total_duration_ms, verbose);
    let mut detached = false;
    let mut stream_error = None;

    // tty 路径：每 2s 节流刷新；事件流 + ctrl_c + ticker 三路 select。
    let mut ticker = tokio::time::interval(Duration::from_secs(2));
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    // 立即跳过第一个 tick（至少要等到第一条事件再渲染）
    ticker.tick().await;

    loop {
        tokio::select! {
            biased;
            _ = tokio::signal::ctrl_c() => {
                // spec §7：弹出 detach / cancel 询问。
                match signal::prompt_action_async().await {
                    CtrlCAction::Detach => {
                        detached = true;
                        eprintln!("detached; job continues on master. job_id={job_id}");
                        break;
                    }
                    CtrlCAction::Cancel | CtrlCAction::DefaultCancel => {
                        let _ = signal::cancel_job(&mut client, &job_id).await;
                        // 取消请求会让 master 很快 finalize stream；继续消费事件，
                        // 若事件竞争丢失，后续会通过 QueryJob 短轮询确认终态。
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

    // 终态后查询一次最终状态并写 JSON。若 watch 在非终态前断流，明确报错，
    // 避免把 watcher overflow / 网络断开误当成 job 完成。
    let q = client
        .query_job(pb::QueryJobRequest {
            job_id: job_id.clone(),
        })
        .await
        .map_err(|e| anyhow::anyhow!("QueryJob: {e}"))?
        .into_inner();

    let q = cmd::ensure_terminal_after_stream(&mut client, &job_id, q, stream_error).await?;

    display::summary::print_with_options(&q, display::summary::SummaryOptions { verbose });

    let out_path = output_path.unwrap_or_else(|| PathBuf::from(format!("{job_id}.json")));
    output::write_report(&out_path, &q)?;
    println!("result written to {}", out_path.display());

    Ok(())
}

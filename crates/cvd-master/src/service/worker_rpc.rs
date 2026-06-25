//! Worker 端 RPC 实现。
//!
//! 5 个端点的归属：
//! - `FetchJob` → [`crate::scheduler::fetch_job`]；
//! - `ReportReady` → [`crate::scheduler::ready`]；
//! - `FetchFileBatch` → 路线 10：读场景文件队列出队；
//! - `ReportProgress` / `ReportResult` → 本模块直接实现。

use std::sync::Arc;

use cvd_proto::cvdbench as pb;
use tonic::{Response, Status};

use crate::aggregate;
use crate::events;
use crate::scheduler;
use crate::state::MasterState;

/// 校验 worker_id 字符集与长度（spec §6.1），不通过即 InvalidArgument。
fn require_valid_worker_id(worker_id: &str) -> Result<(), Status> {
    cvd_common::id::validate(worker_id)
        .map_err(|e| Status::invalid_argument(format!("worker_id: {e}")))
}

pub fn fetch_job(
    state: &Arc<MasterState>,
    req: pb::FetchJobRequest,
) -> Result<Response<pb::FetchJobResponse>, Status> {
    scheduler::fetch_job::fetch_job(state, req)
}

pub fn report_ready(
    state: &MasterState,
    req: pb::ReportReadyRequest,
) -> Result<Response<pb::ReportReadyResponse>, Status> {
    require_valid_worker_id(&req.worker_id)?;
    scheduler::ready::report_ready(state, req)
}

pub fn fetch_file_batch(
    state: &MasterState,
    req: pb::FetchFileBatchRequest,
) -> Result<Response<pb::FetchFileBatchResponse>, Status> {
    require_valid_worker_id(&req.worker_id)?;
    const DEFAULT_BATCH: i32 = 1000;
    const MAX_BATCH: i32 = 10_000;
    let batch_size = match req.batch_size {
        n if n <= 0 => DEFAULT_BATCH,
        n if n > MAX_BATCH => MAX_BATCH,
        n => n,
    } as usize;

    // 收集要返回的内容；jobs 锁仅持到 drain 完成
    let outcome = {
        let mut jobs = state.jobs.lock().expect("jobs mutex");
        let Some(record) = jobs.get_mut(&req.job_id) else {
            return Ok(Response::new(unknown_batch()));
        };
        // run_workers 包含性检查上移：spec §5.8 明确 worker_id ∉ run_workers 一律
        // unknown_job=true，包括 CANCELLED 状态。
        if !record.run_workers.contains(&req.worker_id) {
            return Ok(Response::new(unknown_batch()));
        }
        // 仅 RUNNING 接受批次取数；其它状态按 spec §5.8 处理
        match record.status {
            pb::JobStatus::Cancelled => {
                // 仍然 touch：CANCELLED 期间 worker 在退出，stale watcher 不应误判
                record.touch(&req.worker_id);
                return Ok(Response::new(pb::FetchFileBatchResponse {
                    files: vec![],
                    has_more: false,
                    cancelled: true,
                    unknown_job: false,
                }));
            }
            pb::JobStatus::Running => {}
            // PREPARING / PENDING / 终态都按 unknown_job 处理（worker 应放弃）
            _ => return Ok(Response::new(unknown_batch())),
        }
        let queue = match &record.file_queue {
            Some(q) => q.clone(),
            None => {
                // 没有 file_queue：可能是 dir_manifest 模式（v1 不支持）或 read 字段缺失。
                // 直接返回 unknown_job 让 worker 放弃。
                return Ok(Response::new(unknown_batch()));
            }
        };
        let manifest_done = record.manifest_done.clone();
        // FetchFileBatch 也是 worker 活性信号（spec §5.5），刷新 last_seen
        record.touch(&req.worker_id);
        (queue, manifest_done)
    };

    let (queue, manifest_done) = outcome;
    let files = queue.drain_up_to(batch_size);
    let has_more = if !files.is_empty() {
        true
    } else if manifest_done.load(std::sync::atomic::Ordering::SeqCst) && queue.is_empty() {
        false
    } else {
        // 队列暂空但 manifest 仍在生产 → 让 worker 短暂 sleep 后重试
        true
    };

    Ok(Response::new(pb::FetchFileBatchResponse {
        files,
        has_more,
        cancelled: false,
        unknown_job: false,
    }))
}

fn unknown_batch() -> pb::FetchFileBatchResponse {
    pb::FetchFileBatchResponse {
        files: vec![],
        has_more: false,
        cancelled: false,
        unknown_job: true,
    }
}

/// `ReportProgress` —— 活性 ping + 写入 `latest_progress` + emit PROGRESS 事件。
pub fn report_progress(
    state: &MasterState,
    req: pb::ReportProgressRequest,
) -> Result<Response<pb::ReportProgressResponse>, Status> {
    let mut progress = req
        .progress
        .ok_or_else(|| Status::invalid_argument("progress is required"))?;
    let worker_id = progress.worker_id.clone();
    require_valid_worker_id(&worker_id)?;

    // spec §5.9 step 5: phase ∈ {PREPARING, WAITING_START, CLEANUP, FINISHED}
    // 必须 per_op 为空；MEASURING 才进最终聚合。这里强制清空，避免 worker bug
    // 污染 master 终态。WARMUP 在协议上允许 per_op 用于进度展示，但当前 master
    // 把 WARMUP 也仅当展示，不进入聚合。
    let phase = pb::WorkerPhase::try_from(progress.phase).unwrap_or(pb::WorkerPhase::Unspecified);
    let phase_allows_per_op = matches!(phase, pb::WorkerPhase::Warmup | pb::WorkerPhase::Measuring);
    if !phase_allows_per_op && !progress.per_op.is_empty() {
        progress.per_op.clear();
    }

    let mut jobs = state.jobs.lock().expect("jobs mutex");
    let Some(record) = jobs.get_mut(&req.job_id) else {
        return Ok(Response::new(pb::ReportProgressResponse {
            cancelled: false,
            unknown_job: true,
        }));
    };

    if record.status == pb::JobStatus::Cancelled {
        record.touch(&worker_id);
        return Ok(Response::new(pb::ReportProgressResponse {
            cancelled: true,
            unknown_job: false,
        }));
    }
    if record.is_terminal() {
        return Ok(Response::new(pb::ReportProgressResponse {
            cancelled: false,
            unknown_job: true,
        }));
    }
    if !record.worker_assignments.contains_key(&worker_id) {
        return Ok(Response::new(pb::ReportProgressResponse {
            cancelled: false,
            unknown_job: true,
        }));
    }

    record.touch(&worker_id);
    record.latest_progress.insert(worker_id, progress);
    events::emit_for_job(state, record, pb::EventKind::Progress);

    Ok(Response::new(pb::ReportProgressResponse {
        cancelled: false,
        unknown_job: false,
    }))
}

/// `ReportResult` —— 收集每个 run worker 的最终结果。
///
/// 状态转换（spec §5.10）：
/// - 任一 `success=false` → 立即 FAILED；
/// - 全部 `success=true` → COMPLETED；
/// - CANCELLED 保持，不被 `success=true` 改写。
pub fn report_result(
    state: &MasterState,
    req: pb::ReportResultRequest,
) -> Result<Response<pb::ReportResultResponse>, Status> {
    let result = req
        .result
        .ok_or_else(|| Status::invalid_argument("result is required"))?;
    let worker_id = result.worker_id.clone();
    require_valid_worker_id(&worker_id)?;

    let became_terminal;
    let job_id = req.job_id.clone();

    {
        let mut jobs = state.jobs.lock().expect("jobs mutex");
        let Some(record) = jobs.get_mut(&job_id) else {
            // job 已 GC：active 表里如果还指着该 job 也清掉，避免 worker 被卡死
            drop(jobs);
            let mut active = state.worker_active_jobs.lock().expect("active mutex");
            if active.get(&worker_id) == Some(&job_id) {
                active.remove(&worker_id);
            }
            return Ok(Response::new(pb::ReportResultResponse {
                unknown_job: true,
            }));
        };

        if !record.run_workers.contains(&worker_id) {
            // 非 run worker：对方已经不属于本 job，清理 active 占位（防御性）
            drop(jobs);
            let mut active = state.worker_active_jobs.lock().expect("active mutex");
            if active.get(&worker_id) == Some(&job_id) {
                active.remove(&worker_id);
            }
            return Ok(Response::new(pb::ReportResultResponse {
                unknown_job: true,
            }));
        }

        // 只有 RUNNING/CANCELLED 接受 ReportResult。其它状态（PENDING/PREPARING/
        // COMPLETED/FAILED）当作 unknown_job：spec §5.10 仅在跑过的 worker 上汇总
        // 结果；preparing 阶段意外到达的 result 表示 worker 状态错乱，应被丢弃，
        // 终态本来就不该被翻动。
        match record.status {
            pb::JobStatus::Running | pb::JobStatus::Cancelled => {}
            _ => {
                // 终态：本 worker 的占位已无意义，清理 active
                drop(jobs);
                let mut active = state.worker_active_jobs.lock().expect("active mutex");
                if active.get(&worker_id) == Some(&job_id) {
                    active.remove(&worker_id);
                }
                return Ok(Response::new(pb::ReportResultResponse {
                    unknown_job: true,
                }));
            }
        }

        record.touch(&worker_id);

        if record.worker_results.contains_key(&worker_id) {
            // 幂等重复：active 占位也可以清了（worker 已上报过 result，不会再跑本 job）
            drop(jobs);
            let mut active = state.worker_active_jobs.lock().expect("active mutex");
            if active.get(&worker_id) == Some(&job_id) {
                active.remove(&worker_id);
            }
            return Ok(Response::new(pb::ReportResultResponse {
                unknown_job: false,
            }));
        }

        let success = result.success;
        let error = result.error.clone();
        let final_phase = if success {
            pb::WorkerPhase::Finished
        } else {
            pb::WorkerPhase::Unspecified
        };
        record.latest_progress.insert(
            worker_id.clone(),
            pb::WorkerProgress {
                worker_id: worker_id.clone(),
                elapsed_ms: result.effective_duration_ms,
                per_op: result.per_op.clone(),
                phase: final_phase as i32,
            },
        );
        // CANCELLED 不接受新 worker_results 写入（避免 watcher 已经 finalize 后
        // 重新触发聚合 / emit RESULT 事件；spec §5.10 step 3 提到可记录用于审计，
        // 但 master 当前 watcher 流模型下重新写入会产生 phantom 事件）。
        let cancelled_status = record.status == pb::JobStatus::Cancelled;
        if !cancelled_status {
            record.worker_results.insert(worker_id.clone(), result);
        }

        let mut transition = false;
        if !cancelled_status && !record.is_terminal() {
            if !success {
                record.status = pb::JobStatus::Failed;
                record.error =
                    error.or_else(|| Some(format!("worker {worker_id} reported failure")));
                tracing::warn!(%job_id, %worker_id, "worker failed → job FAILED");
                transition = true;
            } else if record.worker_results.len() == record.run_workers.len() {
                record.status = pb::JobStatus::Completed;
                tracing::info!(%job_id, "all workers succeeded → job COMPLETED");
                transition = true;
            }
        }
        if transition {
            // 终态：先聚合，再 cleanup（关 manifest 队列等），再 emit
            aggregate::aggregate_into_record(record);
            record.cleanup_on_terminal();
            events::emit_for_job(state, record, pb::EventKind::StatusChange);
        }
        became_terminal = transition;
    }

    if became_terminal {
        events::finalize_stream(state, &job_id);
    }

    {
        let mut active = state.worker_active_jobs.lock().expect("active mutex");
        if active.get(&worker_id) == Some(&job_id) {
            active.remove(&worker_id);
        }
    }

    Ok(Response::new(pb::ReportResultResponse {
        unknown_job: false,
    }))
}

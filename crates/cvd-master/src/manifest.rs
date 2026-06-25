//! 读 job 文件来源：file_manifest CSV 解析 + dir_manifest 并发扫描，
//! 写入有界 `file_queue`（spec §5.7）。

use std::path::PathBuf;
use std::sync::Arc;

use cvd_proto::cvdbench as pb;

use crate::aggregate;
use crate::events;
use crate::state::{JobRecord, MasterState};

pub mod dir_scan;
pub mod file_csv;
pub mod queue;

pub use dir_scan::{run_pipeline as run_dir_scan_pipeline, DirScanError};
pub use file_csv::{run as run_file_csv_reader, ReaderError as FileCsvReaderError};
pub use queue::{BoundedQueue, QueueClosed};

/// 在 PREPARING 转换瞬间为需要 file_queue 的 job 启动 manifest reader 流水线。
///
/// 二选一分发：
/// - `file_manifest` 非空 → 启 1 个 CSV reader；
/// - `dir_manifest`  非空 → 启 1 个 dir reader + N scanner 流水线；
/// - 都为空（CreateJob 校验过不会发生）→ 防御性 no-op。
pub fn start_for_file_queue_job(state: &Arc<MasterState>, record: &mut JobRecord) {
    let source = if let Some(read) = record.spec_redacted.read.clone() {
        if !read.file_manifest.is_empty() {
            Some((PathBuf::from(read.file_manifest), false, read.loop_files))
        } else if !read.dir_manifest.is_empty() {
            Some((PathBuf::from(read.dir_manifest), true, read.loop_files))
        } else {
            None
        }
    } else if let Some(meta) = record.spec_redacted.metadata.clone() {
        if !meta.dir_manifest.is_empty() {
            Some((PathBuf::from(meta.dir_manifest), true, false))
        } else {
            None
        }
    } else {
        None
    };
    let Some((path, is_dir_manifest, loop_files)) = source else {
        return;
    };

    let queue = Arc::new(BoundedQueue::new(
        state.config.scheduler.file_queue_capacity,
    ));
    record.file_queue = Some(queue.clone());

    let cancel = record.cancel_flag.clone();
    let done = record.manifest_done.clone();
    let scan_stats = record.manifest_scan_stats.clone();
    let job_id = record.job_id.clone();
    let mount_point = record.mount_point.clone();
    let state_for_task = state.clone();

    let handle = if !is_dir_manifest {
        tokio::spawn(async move {
            match run_file_csv_reader(path, queue, cancel, loop_files, done.clone()).await {
                Ok(()) => tracing::debug!(%job_id, "file_csv reader finished"),
                Err(err) => {
                    tracing::error!(%job_id, %err,
                        "file_csv reader failed → marking job FAILED");
                    fail_job_due_to_manifest(&state_for_task, &job_id, &err.to_string()).await;
                }
            }
        })
    } else {
        let scan_concurrency = state.config.scheduler.dir_scan_concurrency;
        tokio::spawn(async move {
            match run_dir_scan_pipeline(
                path,
                mount_point,
                queue,
                scan_concurrency,
                cancel,
                loop_files,
                done.clone(),
                scan_stats,
            )
            .await
            {
                Ok(()) => tracing::debug!(%job_id, "dir_scan pipeline finished"),
                Err(err) => {
                    tracing::error!(%job_id, %err,
                        "dir_scan pipeline failed → marking job FAILED");
                    fail_job_due_to_manifest(&state_for_task, &job_id, &err.to_string()).await;
                }
            }
        })
    };
    record.manifest_handle = Some(handle);
}

async fn fail_job_due_to_manifest(state: &Arc<MasterState>, job_id: &str, reason: &str) {
    let (became_terminal, assigned) = {
        let mut jobs = state.jobs.lock().expect("jobs mutex");
        let Some(record) = jobs.get_mut(job_id) else {
            return;
        };
        if record.is_terminal() {
            return;
        }
        record.status = pb::JobStatus::Failed;
        record.error = Some(format!("manifest reader: {reason}"));
        aggregate::aggregate_into_record(record);
        record.cleanup_on_terminal();
        events::emit_for_job(state, record, pb::EventKind::StatusChange);
        let assigned: Vec<String> = record.worker_assignments.keys().cloned().collect();
        (true, assigned)
    };
    if became_terminal {
        // PENDING/PREPARING 阶段 manifest 失败时，已经领到 slot 的 worker 仍然
        // 持有 worker_active_jobs 占位；不清理会让 worker daemon 不能 fetch 新 job
        // （spec §6.4 worker 单 job 串行）。
        let mut active = state.worker_active_jobs.lock().expect("active mutex");
        for w in &assigned {
            if active.get(w).map(String::as_str) == Some(job_id) {
                active.remove(w);
            }
        }
        drop(active);
        events::finalize_stream(state, job_id);
    }
}

//! WatchJob 事件分发：JobEvent 单调 seq + EventKind + subscriber 广播。
//!
//! 调用者向 [`emit_for_job`] 传入当前 [`JobRecord`] 与 [`pb::EventKind`]，
//! 函数自动生成 `seq` / `timestamp` 并广播到所有订阅该 job_id 的 watcher。

use cvd_proto::cvdbench as pb;

use crate::state::{now_ms, JobRecord, MasterState};

pub mod subscriber;

pub use subscriber::{EventSender, SubscriberRegistry};

/// 根据当前 record 构造一条 JobEvent。`seq` 由 master 单调分配。
pub fn build_event(state: &MasterState, record: &JobRecord, kind: pb::EventKind) -> pb::JobEvent {
    let (dirs_scanned, files_scanned, scan_duration_ms) = record.manifest_scan_stats.snapshot();
    pb::JobEvent {
        job_id: record.job_id.clone(),
        status: record.status.into(),
        worker_progress: record.latest_progress.values().cloned().collect(),
        aggregated: record.aggregated.clone(),
        error: record.error.clone(),
        timestamp: now_ms(),
        seq: state.next_event_seq(),
        kind: kind.into(),
        dirs_scanned,
        files_scanned,
        scan_duration_ms,
    }
}

/// 广播一条 JobEvent 到该 job 的所有 subscriber。
///
/// 不持有 `state.jobs` 锁；调用者应在 drop 锁后调用，避免阻塞 RPC 路径。
pub fn emit_for_job(state: &MasterState, record: &JobRecord, kind: pb::EventKind) {
    let event = build_event(state, record, kind);
    state.subscribers.broadcast(&record.job_id, &event);
}

/// 终态发完最后一条事件后调用：关闭所有 subscriber 流。
pub fn finalize_stream(state: &MasterState, job_id: &str) {
    state.subscribers.terminate(job_id);
}

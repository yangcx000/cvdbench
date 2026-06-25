//! 终态结果聚合：调用 [`cvd_common::metrics::aggregate`] 把 `worker_results`
//! 合并成 [`pb::AggregatedMetrics`]，存进 [`JobRecord::aggregated`]，供 QueryJob /
//! WatchJob 终态事件下发（spec §4.2）。
//!
//! 同时记录 `terminal_at_ms`，让 [`crate::gc`] 能按 retention 判断 GC 时机。

use cvd_proto::cvdbench as pb;

use crate::state::{now_ms, JobRecord};

/// 把 `record.worker_results` 收集成 `Vec<pb::WorkerResult>` 后调用 cvd-common 的
/// 聚合实现。结果直接写回 `record.aggregated`；调用方必须在持有 jobs lock 时调用。
///
/// `window_misaligned` / `missing_histogram_count` 写到独立字段，**不**污染
/// `record.error`（保留给真实终态错误：staleness / manifest fail / worker
/// reported failure 等）；CLI 通过 QueryJob 的 aggregated 透出这些诊断字段。
pub fn aggregate_into_record(record: &mut JobRecord) {
    let workers: Vec<pb::WorkerResult> = record.worker_results.values().cloned().collect();
    let summary = cvd_common::metrics::aggregate(&workers);
    record.aggregated = Some(summary.aggregated);
    record.window_misaligned = summary.window_misaligned;
    record.missing_histogram_count = summary.missing_histogram_count;
    record.success_worker_count = summary.success_worker_count;
    if record.terminal_at_ms.is_none() {
        record.terminal_at_ms = Some(now_ms());
    }
}

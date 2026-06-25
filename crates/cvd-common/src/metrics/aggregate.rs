//! 多 worker 结果聚合：counter 求和；分位数从合并 histogram 重算；
//! `effective_duration_ms` 取交集，否则标 `window_misaligned=true`（spec §4.2）。
//!
//! 聚合规则：
//! - 仅参与「success=true」的 worker；失败的 worker 在终态由 master 单独标注。
//! - `total_per_op[k]`：合并所有成功 worker 的 `per_op[k]`；counter 求和，
//!   histogram 反序列化后 `add` 合并并重新序列化。
//! - `total`：把所有 op 的合并结果再次合在一起。
//! - `effective_window_ms`：取所有贡献了 counter 的 worker
//!   `[measure_start, measure_end]` 区间交集；交集为空时回退到
//!   `min(effective_duration_ms)` 并由调用方在结果中标 `window_misaligned`
//!   （暂未在 proto 中体现，预留扩展点）。
//! - `throughput_mbps` / `iops` 用 total_bytes / total_ops 除以聚合后的 `effective_secs`
//!   重新计算，**不**取各 worker 数值的均值。

use std::collections::HashMap;

use cvd_proto::cvdbench as pb;
use hdrhistogram::Histogram;

use super::histogram;

/// 把 NaN/Inf 折回 0，避免后续 serde_json 序列化时报错。
#[inline]
fn sanitize_finite(v: f64) -> f64 {
    if v.is_finite() {
        v
    } else {
        0.0
    }
}

/// 多 worker 聚合结果，附带计算细节供调用方诊断。
#[derive(Debug, Clone)]
pub struct AggregatedSummary {
    pub aggregated: pb::AggregatedMetrics,
    /// `[measure_start, measure_end]` 交集时长；交集为空时回退值。
    pub effective_window_ms: i64,
    /// 交集为空时为 true（CLI 应在 summary 上注明）。
    pub window_misaligned: bool,
    /// 参与 histogram 合并的成功 worker 数量。
    pub success_worker_count: usize,
    /// 缺失 histogram 的 worker 数量；spec §4.2 要求 CLI 标注 latency 不完整。
    pub missing_histogram_count: usize,
}

/// 主入口：根据所有 worker 结果生成聚合视图。
///
/// 聚合规则（spec §4.2 / §6.7）：
/// - counter 部分（total_ops/total_bytes/error_count）按 **所有** worker 求和：
///   失败 worker 在 fail-fast 退出前累计的 `error_count` 也要被计入，否则用户
///   永远看不到首错前累计的 op 数；
/// - histogram 仅由 success worker 参与合并（失败 worker 中途 abort 时 hist
///   数据不完整、p99 等指标不可信）；
/// - effective window 由贡献 counter 的 worker 计算，避免 failed-only job 因
///   success worker 数为 0 而用 1ms 兜底，导致吞吐/IOPS 被异常放大。
#[must_use]
pub fn aggregate(workers: &[pb::WorkerResult]) -> AggregatedSummary {
    let successful: Vec<&pb::WorkerResult> = workers.iter().filter(|w| w.success).collect();
    let contributors: Vec<&pb::WorkerResult> = workers.iter().filter(|w| has_counters(w)).collect();
    let (effective_window_ms, window_misaligned) = compute_effective_window_ms(&contributors);
    #[allow(clippy::cast_precision_loss)]
    let effective_secs = (effective_window_ms.max(1) as f64) / 1000.0;

    let success_worker_count = successful.len();
    let mut missing_histogram_count = 0usize;
    for w in &successful {
        for m in w.per_op.values() {
            if m.latency_histogram_hdr.is_empty() && m.total_ops > 0 {
                missing_histogram_count += 1;
                break; // 一个 worker 只数一次
            }
        }
    }

    // 1) total_per_op：合并所有 worker 的同名 op
    //    - counter 来自 **所有** worker（含失败 worker 的 error_count）
    //    - histogram 仅来自 success worker
    let mut merged_per_op: HashMap<String, MergedOp> = HashMap::new();
    for w in workers {
        let include_hist = w.success;
        for (op, m) in &w.per_op {
            let entry = merged_per_op.entry(op.clone()).or_default();
            entry.merge_counters(m);
            if include_hist {
                entry.merge_histogram(m);
            }
        }
    }
    let total_per_op_pb: HashMap<String, pb::PerformanceMetrics> = merged_per_op
        .iter()
        .map(|(k, v)| (k.clone(), v.to_pb(effective_secs)))
        .collect();

    // 2) total：把所有 op 的合并结果合在一起
    let mut total = MergedOp::default();
    for v in merged_per_op.values() {
        total.merge_other(v);
    }

    let aggregated = pb::AggregatedMetrics {
        total: Some(total.to_pb(effective_secs)),
        total_per_op: total_per_op_pb,
        per_worker: workers.to_vec(),
    };
    AggregatedSummary {
        aggregated,
        effective_window_ms,
        window_misaligned,
        success_worker_count,
        missing_histogram_count,
    }
}

fn has_counters(worker: &pb::WorkerResult) -> bool {
    worker
        .per_op
        .values()
        .any(|m| m.total_ops > 0 || m.total_bytes > 0 || m.error_count > 0)
}

fn compute_effective_window_ms(workers: &[&pb::WorkerResult]) -> (i64, bool) {
    if workers.is_empty() {
        return (0, false);
    }
    if workers.len() == 1 {
        // 单 worker：直接用 effective_duration_ms，不存在跨 worker 错位（spec §4.2）。
        let w = workers[0];
        let span = (w.measure_end_ms - w.measure_start_ms).max(0);
        let dur = if span > 0 {
            span
        } else {
            w.effective_duration_ms.max(0)
        };
        return (dur, false);
    }
    let max_start = workers
        .iter()
        .map(|w| w.measure_start_ms)
        .max()
        .unwrap_or(0);
    let min_end = workers.iter().map(|w| w.measure_end_ms).min().unwrap_or(0);
    if min_end > max_start {
        (min_end - max_start, false)
    } else {
        // 交集为空：回退到 effective_duration_ms 的最小值并标注
        let fallback = workers
            .iter()
            .map(|w| w.effective_duration_ms)
            .min()
            .unwrap_or(0);
        (fallback, true)
    }
}

/// 累积单个 op 的合并状态，跨 worker 求和 + histogram 合并。
#[derive(Default)]
struct MergedOp {
    total_ops: i64,
    total_bytes: i64,
    error_count: i64,
    histogram: Option<Histogram<u64>>,
}

impl MergedOp {
    fn merge_counters(&mut self, m: &pb::PerformanceMetrics) {
        self.total_ops = self.total_ops.saturating_add(m.total_ops);
        self.total_bytes = self.total_bytes.saturating_add(m.total_bytes);
        self.error_count = self.error_count.saturating_add(m.error_count);
    }

    fn merge_histogram(&mut self, m: &pb::PerformanceMetrics) {
        if !m.latency_histogram_hdr.is_empty() {
            if let Ok(h) = histogram::decode(&m.latency_histogram_hdr) {
                self.add_histogram(h);
            }
        }
    }

    fn merge_other(&mut self, other: &Self) {
        self.total_ops = self.total_ops.saturating_add(other.total_ops);
        self.total_bytes = self.total_bytes.saturating_add(other.total_bytes);
        self.error_count = self.error_count.saturating_add(other.error_count);
        if let Some(h) = &other.histogram {
            self.add_histogram(h.clone());
        }
    }

    fn add_histogram(&mut self, h: Histogram<u64>) {
        match self.histogram.as_mut() {
            Some(existing) => {
                let _ = existing.add(h);
            }
            None => self.histogram = Some(h),
        }
    }

    fn to_pb(&self, effective_secs: f64) -> pb::PerformanceMetrics {
        let throughput_mbps = if effective_secs > 0.0 {
            #[allow(clippy::cast_precision_loss)]
            let bytes = self.total_bytes as f64;
            bytes / 1_000_000.0 / effective_secs
        } else {
            0.0
        };
        let iops = if effective_secs > 0.0 {
            #[allow(clippy::cast_precision_loss)]
            let ops = self.total_ops as f64;
            ops / effective_secs
        } else {
            0.0
        };
        let error_rate = if self.total_ops > 0 {
            #[allow(clippy::cast_precision_loss)]
            let denom = self.total_ops as f64;
            (self.error_count as f64) / denom
        } else {
            0.0
        };
        let stats = if let Some(h) = &self.histogram {
            if h.is_empty() {
                pb::LatencyStats::default()
            } else {
                pb::LatencyStats {
                    #[allow(clippy::cast_precision_loss)]
                    p50: h.value_at_quantile(0.50) as f64,
                    #[allow(clippy::cast_precision_loss)]
                    p95: h.value_at_quantile(0.95) as f64,
                    #[allow(clippy::cast_precision_loss)]
                    p99: h.value_at_quantile(0.99) as f64,
                    #[allow(clippy::cast_precision_loss)]
                    p999: h.value_at_quantile(0.999) as f64,
                    #[allow(clippy::cast_precision_loss)]
                    max: h.max() as f64,
                    avg: sanitize_finite(h.mean()),
                }
            }
        } else {
            pb::LatencyStats::default()
        };
        let serialized = self
            .histogram
            .as_ref()
            .and_then(|h| histogram::encode_v2_compressed(h).ok())
            .unwrap_or_default();
        pb::PerformanceMetrics {
            throughput_mbps,
            iops,
            latency_us: Some(stats),
            error_count: self.error_count,
            error_rate,
            total_ops: self.total_ops,
            total_bytes: self.total_bytes,
            latency_histogram_hdr: serialized,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metrics::{histogram::encode_v2_compressed, PerOpMetrics};

    fn metrics_pb(latencies_us: &[u64], bytes_per_op: u64) -> pb::PerformanceMetrics {
        let m = PerOpMetrics::new();
        for &l in latencies_us {
            m.record_op(l, bytes_per_op);
        }
        m.snapshot(1.0)
    }

    fn worker_result(
        worker_id: &str,
        per_op: HashMap<String, pb::PerformanceMetrics>,
        success: bool,
        ms_start: i64,
        ms_end: i64,
    ) -> pb::WorkerResult {
        pb::WorkerResult {
            worker_id: worker_id.into(),
            per_op,
            consistency_errors: vec![],
            success,
            error: None,
            effective_duration_ms: ms_end - ms_start,
            measure_start_ms: ms_start,
            measure_end_ms: ms_end,
        }
    }

    #[test]
    fn aggregate_two_workers_sum_counters_and_merge_histograms() {
        let mut a_per_op = HashMap::new();
        a_per_op.insert("write".into(), metrics_pb(&[100, 200, 300, 400, 500], 4096));
        let mut b_per_op = HashMap::new();
        b_per_op.insert("write".into(), metrics_pb(&[110, 210, 310, 410, 510], 4096));

        let workers = vec![
            worker_result("w1", a_per_op, true, 1_000, 2_000),
            worker_result("w2", b_per_op, true, 1_000, 2_000),
        ];
        let summary = aggregate(&workers);

        let total = summary.aggregated.total.unwrap();
        assert_eq!(total.total_ops, 10);
        assert_eq!(total.total_bytes, 10 * 4096);
        // window 1s 内 10 ops → 10 iops
        assert!((total.iops - 10.0).abs() < 1e-6);
        // total per_op 也要存在
        assert!(summary.aggregated.total_per_op.contains_key("write"));
        assert!(!summary.window_misaligned);
        assert_eq!(summary.effective_window_ms, 1_000);
    }

    #[test]
    fn aggregate_skips_failed_workers() {
        let mut succ_per_op = HashMap::new();
        succ_per_op.insert("write".into(), metrics_pb(&[100, 200], 1000));

        let workers = vec![
            worker_result("ok", succ_per_op, true, 0, 1000),
            worker_result("fail", HashMap::new(), false, 0, 1000),
        ];
        let summary = aggregate(&workers);
        let total = summary.aggregated.total.unwrap();
        assert_eq!(total.total_ops, 2);
    }

    #[test]
    fn aggregate_marks_window_misaligned_for_disjoint_windows() {
        let mut a_per_op = HashMap::new();
        a_per_op.insert("write".into(), metrics_pb(&[100], 1));
        let mut b_per_op = HashMap::new();
        b_per_op.insert("write".into(), metrics_pb(&[200], 1));
        let workers = vec![
            worker_result("a", a_per_op, true, 0, 1000),
            worker_result("b", b_per_op, true, 5000, 6000),
        ];
        let summary = aggregate(&workers);
        assert!(summary.window_misaligned);
        // 回退到 effective_duration_ms 的最小值（都是 1000）
        assert_eq!(summary.effective_window_ms, 1000);
    }

    #[test]
    fn merge_op_histograms_double_count() {
        // 单元测试：构造两份相同 hist，merge 后 count 翻倍，p50 不变
        let mut a_per_op = HashMap::new();
        a_per_op.insert("write".into(), metrics_pb(&[100; 1000], 0));
        let mut b_per_op = HashMap::new();
        b_per_op.insert("write".into(), metrics_pb(&[100; 1000], 0));
        let workers = vec![
            worker_result("a", a_per_op, true, 0, 1000),
            worker_result("b", b_per_op, true, 0, 1000),
        ];
        let summary = aggregate(&workers);
        let merged_pb = &summary.aggregated.total_per_op["write"];
        assert_eq!(merged_pb.total_ops, 2000);
        let merged_hist = super::histogram::decode(&merged_pb.latency_histogram_hdr).unwrap();
        assert_eq!(merged_hist.len(), 2000);
        assert_eq!(merged_hist.value_at_quantile(0.5), 100);
    }

    #[test]
    fn empty_workers_yields_zero_aggregate() {
        let summary = aggregate(&[]);
        let total = summary.aggregated.total.unwrap();
        assert_eq!(total.total_ops, 0);
        assert_eq!(total.total_bytes, 0);
        assert_eq!(summary.effective_window_ms, 0);
        let _ = encode_v2_compressed; // ensure import is reachable
    }

    /// 回归（M10）：失败 worker 的 error_count 必须计入 total，否则 fail-fast
    /// 场景下用户看不到首错前累计的 op 数（spec §6.7）。
    #[test]
    fn aggregate_includes_error_count_from_failed_workers() {
        // 成功 worker：5 ops、0 errors
        let mut succ_per_op = HashMap::new();
        succ_per_op.insert("read".into(), metrics_pb(&[100, 100, 100, 100, 100], 4096));

        // 失败 worker：在 fail-fast 前累计了 3 个 ops + 1 个 error。
        let m_fail = PerOpMetrics::new();
        m_fail.record_op(50, 100);
        m_fail.record_op(60, 100);
        m_fail.record_error();
        let fail_pb = m_fail.snapshot(1.0);
        let mut fail_per_op = HashMap::new();
        fail_per_op.insert("read".into(), fail_pb);

        let workers = vec![
            worker_result("ok", succ_per_op, true, 0, 1000),
            worker_result("fail", fail_per_op, false, 0, 1000),
        ];
        let summary = aggregate(&workers);
        let total = summary.aggregated.total.unwrap();
        // counter 部分包含失败 worker 的累计
        assert_eq!(total.total_ops, 5 + 3); // 5 success + 3 fail-fast prefix
        assert_eq!(total.error_count, 1);
    }

    /// 回归：failed-only job 仍可能携带 fail-fast 前累计的 counters。
    /// 聚合吞吐/IOPS 的分母必须使用失败 worker 的实际 measure 窗口，不能因
    /// success worker 数为 0 退化为 1ms，否则会显示离谱的大吞吐。
    #[test]
    fn failed_only_aggregate_uses_failed_worker_duration() {
        let m_fail = PerOpMetrics::new();
        m_fail.record_op(50, 1_000_000);
        m_fail.record_op(60, 1_000_000);
        m_fail.record_error();
        let mut fail_per_op = HashMap::new();
        fail_per_op.insert("read".into(), m_fail.snapshot(1.0));

        let workers = vec![worker_result("fail", fail_per_op, false, 10_000, 12_000)];
        let summary = aggregate(&workers);
        let total = summary.aggregated.total.unwrap();

        assert_eq!(summary.effective_window_ms, 2_000);
        assert!(!summary.window_misaligned);
        assert_eq!(total.total_ops, 3);
        assert_eq!(total.total_bytes, 2_000_000);
        assert_eq!(total.error_count, 1);
        assert!((total.throughput_mbps - 1.0).abs() < 1e-6);
        assert!((total.iops - 1.5).abs() < 1e-6);
    }

    /// 回归（M11）：单 worker 不应被判 window_misaligned。
    #[test]
    fn single_worker_does_not_mark_misaligned() {
        let mut per_op = HashMap::new();
        per_op.insert("write".into(), metrics_pb(&[100, 200], 4096));
        let workers = vec![worker_result("solo", per_op, true, 1_000, 2_000)];
        let summary = aggregate(&workers);
        assert!(!summary.window_misaligned);
        assert_eq!(summary.effective_window_ms, 1_000);
    }
}

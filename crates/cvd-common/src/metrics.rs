//! HDR histogram + per-op counter，多 worker 聚合（spec §4.2）。
//!
//! 三层结构：
//! - [`PerOpMetrics`]：单个 op 的 counter（total_ops/bytes/errors）+ 一份共享
//!   `Mutex<Histogram>`。worker 内部多个 IO task 共享一份 op metric 时通过 Arc
//!   分发；记录调用走 `Mutex` 串行化（写场景每秒几千次 IO 内可承受）。
//! - [`MetricsRegistry`]：`HashMap<op_key, Arc<PerOpMetrics>>`，按 op 名索引。
//! - [`aggregate`]：master 终态把多 worker 的 `pb::WorkerResult.per_op` 合并。

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use cvd_proto::cvdbench as pb;
use hdrhistogram::Histogram;

pub mod aggregate;
pub mod histogram;

pub use aggregate::aggregate;

/// 直方图统计的最大延迟，单位微秒。1h 上限对单 op 已绰绰有余。
const HIST_MAX_LATENCY_US: u64 = 60 * 60 * 1_000_000;
/// HDR histogram 有效精度位数。3 是 hdrhistogram 文档推荐的常见值。
const HIST_SIGNIFICANT_FIGURES: u8 = 3;

/// 用于 serialize 的安全浮点：把 NaN/Inf 折回 0，避免被 serde_json 拒绝。
#[inline]
fn sanitize_finite(v: f64) -> f64 {
    if v.is_finite() {
        v
    } else {
        0.0
    }
}

/// 单个 op 的 counter + histogram。线程安全，可被多个 IO task 共享。
pub struct PerOpMetrics {
    total_ops: AtomicU64,
    total_bytes: AtomicU64,
    error_count: AtomicU64,
    histogram: Mutex<Histogram<u64>>,
}

impl Default for PerOpMetrics {
    fn default() -> Self {
        Self::new()
    }
}

impl PerOpMetrics {
    pub fn new() -> Self {
        let hist = Histogram::<u64>::new_with_max(HIST_MAX_LATENCY_US, HIST_SIGNIFICANT_FIGURES)
            .expect("HDR histogram dimensions are valid");
        Self {
            total_ops: AtomicU64::new(0),
            total_bytes: AtomicU64::new(0),
            error_count: AtomicU64::new(0),
            histogram: Mutex::new(hist),
        }
    }

    /// 记一次成功 op：增加 ops 计数（+1）、bytes（按需）、并把延迟塞进 histogram。
    pub fn record_op(&self, latency_us: u64, bytes: u64) {
        self.total_ops.fetch_add(1, Ordering::Relaxed);
        if bytes > 0 {
            self.total_bytes.fetch_add(bytes, Ordering::Relaxed);
        }
        // 锁失败不会导致 panic（worker 当前不会 poison 该 mutex），
        // 但保险起见使用 try_lock 进入快路径。
        if let Ok(mut h) = self.histogram.lock() {
            // 超过上限时 saturate 到 high()，避免 record_correct 失败丢样本
            let v = latency_us.min(h.high());
            let _ = h.record(v);
        }
    }

    /// 记一次错误 op；fail-fast 语义下整个 worker 即将退出，不再统计延迟分布。
    pub fn record_error(&self) {
        self.error_count.fetch_add(1, Ordering::Relaxed);
        self.total_ops.fetch_add(1, Ordering::Relaxed);
    }

    /// 直接读取计数器（用于测试与诊断）。
    pub fn totals(&self) -> (u64, u64, u64) {
        (
            self.total_ops.load(Ordering::Relaxed),
            self.total_bytes.load(Ordering::Relaxed),
            self.error_count.load(Ordering::Relaxed),
        )
    }

    /// 把当前 metric 序列化为 protobuf。`effective_secs` 用于换算 throughput / IOPS；
    /// 调用方负责传入合适的窗口时长（measure window 或 progress 窗口）。
    pub fn snapshot(&self, effective_secs: f64) -> pb::PerformanceMetrics {
        let total_ops = self.total_ops.load(Ordering::Relaxed);
        let total_bytes = self.total_bytes.load(Ordering::Relaxed);
        let errors = self.error_count.load(Ordering::Relaxed);
        let hist = self.histogram.lock().expect("histogram mutex");
        // 空 histogram（仅 record_error 没 record_op）下 mean()/value_at_quantile
        // 返回 NaN，会让 serde_json 拒绝写出结果文件。返回零分位避免 NaN 污染。
        let stats = if hist.is_empty() {
            pb::LatencyStats::default()
        } else {
            pb::LatencyStats {
                #[allow(clippy::cast_precision_loss)]
                p50: hist.value_at_quantile(0.50) as f64,
                #[allow(clippy::cast_precision_loss)]
                p95: hist.value_at_quantile(0.95) as f64,
                #[allow(clippy::cast_precision_loss)]
                p99: hist.value_at_quantile(0.99) as f64,
                #[allow(clippy::cast_precision_loss)]
                p999: hist.value_at_quantile(0.999) as f64,
                #[allow(clippy::cast_precision_loss)]
                max: hist.max() as f64,
                avg: sanitize_finite(hist.mean()),
            }
        };
        let throughput_mbps = if effective_secs > 0.0 {
            #[allow(clippy::cast_precision_loss)]
            let bytes = total_bytes as f64;
            bytes / 1_000_000.0 / effective_secs
        } else {
            0.0
        };
        let iops = if effective_secs > 0.0 {
            #[allow(clippy::cast_precision_loss)]
            let ops = total_ops as f64;
            ops / effective_secs
        } else {
            0.0
        };
        let error_rate = if total_ops > 0 {
            #[allow(clippy::cast_precision_loss)]
            let denom = total_ops as f64;
            (errors as f64) / denom
        } else {
            0.0
        };
        let serialized = histogram::encode_v2_compressed(&hist).unwrap_or_default();
        pb::PerformanceMetrics {
            throughput_mbps,
            iops,
            latency_us: Some(stats),
            error_count: i64::try_from(errors).unwrap_or(i64::MAX),
            error_rate,
            total_ops: i64::try_from(total_ops).unwrap_or(i64::MAX),
            total_bytes: i64::try_from(total_bytes).unwrap_or(i64::MAX),
            latency_histogram_hdr: serialized,
        }
    }
}

/// 多个 op 的注册表；worker runner 用 [`MetricsRegistry::op`] 拿到 Arc 共享句柄。
pub struct MetricsRegistry {
    ops: Mutex<HashMap<String, Arc<PerOpMetrics>>>,
}

impl Default for MetricsRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl MetricsRegistry {
    pub fn new() -> Self {
        Self {
            ops: Mutex::new(HashMap::new()),
        }
    }

    /// 取或新建 op 句柄。
    pub fn op(&self, name: &str) -> Arc<PerOpMetrics> {
        let mut map = self.ops.lock().expect("metrics map");
        map.entry(name.to_owned())
            .or_insert_with(|| Arc::new(PerOpMetrics::new()))
            .clone()
    }

    /// 把所有 op 序列化为 `HashMap<op_key, PerformanceMetrics>`，可直接塞进
    /// `pb::WorkerProgress.per_op` / `pb::WorkerResult.per_op`。
    pub fn snapshot_pb(&self, effective_secs: f64) -> HashMap<String, pb::PerformanceMetrics> {
        let map = self.ops.lock().expect("metrics map");
        map.iter()
            .map(|(k, v)| (k.clone(), v.snapshot(effective_secs)))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn record_and_snapshot_round_trip() {
        let m = PerOpMetrics::new();
        m.record_op(100, 4096);
        m.record_op(200, 4096);
        m.record_op(500, 4096);
        let pb_metrics = m.snapshot(1.0);
        assert_eq!(pb_metrics.total_ops, 3);
        assert_eq!(pb_metrics.total_bytes, 12288);
        let stats = pb_metrics.latency_us.as_ref().unwrap();
        assert!(stats.p50 >= 100.0);
        assert!(stats.max >= 500.0);
        assert!(!pb_metrics.latency_histogram_hdr.is_empty());
    }

    #[test]
    fn registry_returns_same_arc_for_same_op() {
        let reg = MetricsRegistry::new();
        let a = reg.op("write");
        let b = reg.op("write");
        a.record_op(50, 1);
        assert_eq!(b.totals(), (1, 1, 0));
    }

    #[test]
    fn snapshot_pb_includes_all_ops() {
        let reg = MetricsRegistry::new();
        reg.op("write").record_op(10, 100);
        reg.op("write_verify").record_op(20, 100);
        let snap = reg.snapshot_pb(1.0);
        assert!(snap.contains_key("write"));
        assert!(snap.contains_key("write_verify"));
    }

    #[test]
    fn record_error_advances_total_ops_and_error_count() {
        let m = PerOpMetrics::new();
        m.record_op(10, 0);
        m.record_error();
        let (ops, _, errs) = m.totals();
        assert_eq!(ops, 2);
        assert_eq!(errs, 1);
    }

    #[test]
    fn snapshot_with_only_errors_does_not_emit_nan() {
        // 仅 record_error 的 op：histogram 是空的，mean() 返回 NaN，
        // 守护逻辑应该把分位数全部置 0，避免 serde_json 拒绝。
        let m = PerOpMetrics::new();
        m.record_error();
        m.record_error();
        let pb = m.snapshot(1.0);
        let stats = pb.latency_us.unwrap();
        for v in [
            stats.p50, stats.p95, stats.p99, stats.p999, stats.max, stats.avg,
        ] {
            assert!(v.is_finite(), "expected finite, got {v}");
            assert_eq!(v, 0.0);
        }
        // serde_json 必须能写出
        let _ = serde_json::to_string(&pb).expect("must serialize");
    }
}

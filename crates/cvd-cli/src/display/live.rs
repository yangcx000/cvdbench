//! 实时进度展示（spec §7）：
//! - tty：每 2s 追加打印 per-worker 表格 + Aggregate 行，保留历史采样；
//! - 非 tty（管道 / CI）：fallback 到逐条 `println!` 不影响日志解析。
//!
//! 内部维护 `HashMap<worker_id, WorkerProgress>` 状态机，由 `apply_event`
//! 推动；外部 (cmd/watch、cmd/create) 调用 `LiveDisplay::run_stream` 把
//! tonic stream 喂进来，可选附带 Ctrl+C 信号实现 detach/cancel 行为。

use std::collections::{BTreeMap, HashMap};
use std::time::{Duration, Instant};

use cvd_proto::cvdbench as pb;

/// 渲染表格的最小刷新周期（spec §7：每 2s）。
const REFRESH_INTERVAL: Duration = Duration::from_secs(2);
const TTY_SEP: &str = "─────────────┼───────────────┼───────────┼────────────┼─────────┼──────────┼────────┼────────┼────────┼────────┼────────┼────────┼────────\n";

#[derive(Debug)]
pub struct LiveDisplay {
    job_id: String,
    /// 来自 spec.duration（毫秒），用于计算 Elapsed / Total 列；0 表示未知。
    total_duration_ms: i64,
    verbose: bool,
    started_at: Instant,
    last_status: pb::JobStatus,
    last_seq: i64,
    dirs_scanned: i64,
    files_scanned: i64,
    scan_duration_ms: i64,
    workers: HashMap<String, pb::WorkerProgress>,
    final_workers: HashMap<String, pb::WorkerResult>,
    final_per_op: BTreeMap<String, OpMetrics>,
    last_render: Option<Instant>,
    rendered_once: bool,
}

impl LiveDisplay {
    #[must_use]
    pub fn new(job_id: String, total_duration_ms: i64) -> Self {
        Self::new_with_options(job_id, total_duration_ms, false)
    }

    #[must_use]
    pub fn new_with_options(job_id: String, total_duration_ms: i64, verbose: bool) -> Self {
        Self {
            job_id,
            total_duration_ms,
            verbose,
            started_at: Instant::now(),
            last_status: pb::JobStatus::Unspecified,
            last_seq: 0,
            dirs_scanned: 0,
            files_scanned: 0,
            scan_duration_ms: 0,
            workers: HashMap::new(),
            final_workers: HashMap::new(),
            final_per_op: BTreeMap::new(),
            last_render: None,
            rendered_once: false,
        }
    }

    /// 更新状态机；不直接渲染（让上层决定刷新节奏）。
    pub fn apply(&mut self, evt: &pb::JobEvent) {
        if evt.seq > self.last_seq {
            self.last_seq = evt.seq;
        }
        self.last_status =
            pb::JobStatus::try_from(evt.status).unwrap_or(pb::JobStatus::Unspecified);
        self.dirs_scanned = evt.dirs_scanned;
        self.files_scanned = evt.files_scanned;
        self.scan_duration_ms = evt.scan_duration_ms;
        for wp in &evt.worker_progress {
            self.workers.insert(wp.worker_id.clone(), wp.clone());
        }
        if let Some(agg) = &evt.aggregated {
            self.final_workers = agg
                .per_worker
                .iter()
                .map(|w| (w.worker_id.clone(), w.clone()))
                .collect();
            self.final_per_op = agg
                .total_per_op
                .iter()
                .map(|(op, metric)| (op.clone(), metrics_from_metric(metric)))
                .collect();
        }
    }

    /// 是否到 2s 节流窗口；用于控制追加打印频率。
    pub fn should_render(&self) -> bool {
        match self.last_render {
            None => true,
            Some(t) => t.elapsed() >= REFRESH_INTERVAL,
        }
    }

    /// 渲染当前状态：追加打印本次采样，保留历史输出。
    pub fn render(&mut self) {
        self.render_sample();
        self.last_render = Some(Instant::now());
        self.rendered_once = true;
    }

    fn render_sample(&self) {
        let mut out = String::new();
        if !self.rendered_once {
            out.push_str(&self.header_block());
            out.push('\n');
        }
        out.push_str(&format_table_header());
        out.push_str(TTY_SEP);
        for row in self.worker_rows() {
            out.push_str(&format_metric_row(&row));
        }
        out.push_str(TTY_SEP);
        for row in self.aggregate_rows() {
            out.push_str(&format_metric_row(&row));
        }
        out.push('\n');

        use std::io::Write;
        let stdout = std::io::stdout();
        let mut handle = stdout.lock();
        let _ = handle.write_all(out.as_bytes());
        let _ = handle.flush();
    }

    fn has_scan_stats(&self) -> bool {
        self.dirs_scanned > 0 || self.files_scanned > 0 || self.scan_duration_ms > 0
    }

    fn header_block(&self) -> String {
        let mut header = format!("Job ID: {} | ", self.job_id);
        if self.total_duration_ms > 0 {
            header.push_str(&format!(
                "Elapsed: {} / {} | ",
                format_duration(self.display_elapsed_ms()),
                format_duration(self.total_duration_ms)
            ));
        } else {
            header.push_str(&format!(
                "Elapsed: {} | ",
                format_duration(self.display_elapsed_ms())
            ));
        }
        header.push_str(&format!("Status: {}\n", status_short(self.last_status)));
        if self.verbose && self.has_scan_stats() {
            header.push_str(&format!(
                "Scan: dirs={} files={} duration={}\n",
                format_count(self.dirs_scanned),
                format_count(self.files_scanned),
                format_duration(self.scan_duration_ms)
            ));
        }
        if !self.is_terminal() {
            header.push_str("Tip: Press Ctrl+C to detach or cancel job\n");
        }
        header
    }

    fn is_terminal(&self) -> bool {
        matches!(
            self.last_status,
            pb::JobStatus::Completed | pb::JobStatus::Failed | pb::JobStatus::Cancelled
        )
    }

    fn display_elapsed_ms(&self) -> i64 {
        if !self.final_workers.is_empty() {
            self.final_workers
                .values()
                .map(|w| w.effective_duration_ms)
                .max()
                .unwrap_or(0)
        } else {
            self.workers
                .values()
                .map(|w| w.elapsed_ms)
                .max()
                .unwrap_or_else(|| self.started_at.elapsed().as_secs() as i64 * 1000)
        }
    }

    fn worker_rows(&self) -> Vec<DisplayRow> {
        let mut rows = Vec::new();
        if self.final_workers.is_empty() {
            let mut ids: Vec<&String> = self.workers.keys().collect();
            ids.sort();
            for id in ids {
                let wp = &self.workers[id];
                let mut ops: Vec<_> = wp.per_op.iter().collect();
                ops.sort_by(|a, b| a.0.cmp(b.0));
                rows.extend(ops.into_iter().map(|(op, metric)| DisplayRow {
                    worker: id.clone(),
                    op: op.clone(),
                    elapsed_ms: wp.elapsed_ms,
                    metrics: metrics_from_metric(metric),
                }));
            }
        } else {
            let mut ids: Vec<&String> = self.final_workers.keys().collect();
            ids.sort();
            for id in ids {
                let wr = &self.final_workers[id];
                let mut ops: Vec<_> = wr.per_op.iter().collect();
                ops.sort_by(|a, b| a.0.cmp(b.0));
                rows.extend(ops.into_iter().map(|(op, metric)| DisplayRow {
                    worker: id.clone(),
                    op: op.clone(),
                    elapsed_ms: wr.effective_duration_ms,
                    metrics: metrics_from_metric(metric),
                }));
            }
        }
        rows
    }

    fn aggregate_rows(&self) -> Vec<DisplayRow> {
        let elapsed_ms = self.display_elapsed_ms();
        let per_op = if self.final_per_op.is_empty() {
            aggregate_per_op(&self.workers)
        } else {
            self.final_per_op.clone()
        };
        per_op
            .into_iter()
            .map(|(op, metrics)| DisplayRow {
                worker: "Aggregate".to_owned(),
                op,
                elapsed_ms,
                metrics,
            })
            .collect()
    }
}

/// 直接打印一条事件（兼容旧 watch.rs 调用，保留作为非 tty fallback）。
pub fn print_event(evt: &pb::JobEvent) {
    let status =
        status_short(pb::JobStatus::try_from(evt.status).unwrap_or(pb::JobStatus::Unspecified));
    let kind = kind_name(evt.kind);
    let workers = evt.worker_progress.len();
    let err_suffix = evt
        .error
        .as_deref()
        .map(|e| format!(" error={e:?}"))
        .unwrap_or_default();
    println!(
        "[seq={:>4}] {:<8} status={:<10} workers={}{}",
        evt.seq, kind, status, workers, err_suffix
    );
}

#[derive(Clone, Debug, Default)]
struct OpMetrics {
    throughput_mbps: f64,
    ops_per_sec: f64,
    total_ops: i64,
    error_count: i64,
    p50_us: f64,
    p95_us: f64,
    p99_us: f64,
    p999_us: f64,
    max_us: f64,
    avg_us: f64,
}

#[derive(Clone, Debug)]
struct DisplayRow {
    worker: String,
    op: String,
    elapsed_ms: i64,
    metrics: OpMetrics,
}

fn metrics_from_metric(metric: &pb::PerformanceMetrics) -> OpMetrics {
    let stats = metric.latency_us.as_ref();
    OpMetrics {
        throughput_mbps: metric.throughput_mbps,
        ops_per_sec: metric.iops,
        total_ops: metric.total_ops,
        error_count: metric.error_count,
        p50_us: stats.map_or(0.0, |s| s.p50),
        p95_us: stats.map_or(0.0, |s| s.p95),
        p99_us: stats.map_or(0.0, |s| s.p99),
        p999_us: stats.map_or(0.0, |s| s.p999),
        max_us: stats.map_or(0.0, |s| s.max),
        avg_us: stats.map_or(0.0, |s| s.avg),
    }
}

fn aggregate_per_op(workers: &HashMap<String, pb::WorkerProgress>) -> BTreeMap<String, OpMetrics> {
    let mut per_op: BTreeMap<String, OpMetrics> = BTreeMap::new();
    for worker in workers.values() {
        for (op, metric) in &worker.per_op {
            merge_metric(per_op.entry(op.clone()).or_default(), metric);
        }
    }
    per_op
}

fn merge_metric(target: &mut OpMetrics, metric: &pb::PerformanceMetrics) {
    let previous_ops = target.total_ops;
    target.throughput_mbps += metric.throughput_mbps;
    target.ops_per_sec += metric.iops;
    target.total_ops = target.total_ops.saturating_add(metric.total_ops);
    target.error_count = target.error_count.saturating_add(metric.error_count);
    if let Some(stats) = &metric.latency_us {
        target.p50_us = target.p50_us.max(stats.p50);
        target.p95_us = target.p95_us.max(stats.p95);
        target.p99_us = target.p99_us.max(stats.p99);
        target.p999_us = target.p999_us.max(stats.p999);
        target.max_us = target.max_us.max(stats.max);
        let previous_success_ops =
            previous_ops.saturating_sub(target.error_count - metric.error_count);
        let metric_success_ops = metric.total_ops.saturating_sub(metric.error_count);
        let combined_success_ops = previous_success_ops.saturating_add(metric_success_ops);
        if combined_success_ops > 0 {
            target.avg_us = (target.avg_us * previous_success_ops as f64
                + stats.avg * metric_success_ops as f64)
                / combined_success_ops as f64;
        }
    }
}

fn format_metric_row(row: &DisplayRow) -> String {
    format!(
        " {} │ {} │ {} │ {} │ {} │ {} │ {} │ {} │ {} │ {} │ {} │ {} │ {}\n",
        fit_center(&worker_label(&row.worker), 11),
        fit_center(&op_label(&row.op), 13),
        fit_center(&format_elapsed_compact(row.elapsed_ms, 0), 9),
        fit_center(
            &format_throughput_for_op(&row.op, row.metrics.throughput_mbps),
            10
        ),
        fit_center(&format_iops(row.metrics.ops_per_sec), 7),
        fit_center(&format_count(row.metrics.total_ops), 8),
        fit_center(&format_count(row.metrics.error_count), 6),
        fit_center(&format_us_compact(row.metrics.p50_us), 6),
        fit_center(&format_us_compact(row.metrics.p95_us), 6),
        fit_center(&format_us_compact(row.metrics.p99_us), 6),
        fit_center(&format_us_compact(row.metrics.p999_us), 6),
        fit_center(&format_us_compact(row.metrics.max_us), 6),
        fit_center(&format_us_compact(row.metrics.avg_us), 6)
    )
}

fn format_table_header() -> String {
    format!(
        " {} │ {} │ {} │ {} │ {} │ {} │ {} │ {} │ {} │ {} │ {} │ {} │ {}\n",
        fit_center("Worker", 11),
        fit_center("Op", 13),
        fit_center("Elapsed", 9),
        fit_center("Throughput", 10),
        fit_center("Ops/s", 7),
        fit_center("Ops", 8),
        fit_center("Errors", 6),
        fit_center("p50", 6),
        fit_center("p95", 6),
        fit_center("p99", 6),
        fit_center("p999", 6),
        fit_center("max", 6),
        fit_center("avg", 6)
    )
}

fn op_label(op: &str) -> String {
    op.strip_prefix("metadata.").unwrap_or(op).to_owned()
}

fn worker_label(worker_id: &str) -> String {
    if worker_id == "Aggregate" {
        return worker_id.to_owned();
    }
    let mut parts = worker_id.rsplitn(3, '-');
    let Some(uuid) = parts.next() else {
        return suffix_chars(worker_id, 11);
    };
    let Some(pid) = parts.next() else {
        return suffix_chars(worker_id, 11);
    };
    let uuid_prefix: String = uuid.chars().take(5).collect();
    format!("{pid}-{uuid_prefix}")
}

fn suffix_chars(value: &str, width: usize) -> String {
    let chars: Vec<char> = value.chars().collect();
    if chars.len() <= width {
        value.to_owned()
    } else {
        chars[chars.len() - width..].iter().collect()
    }
}

fn fit_center(value: &str, width: usize) -> String {
    let mut s: String = value.chars().take(width).collect();
    let len = s.chars().count();
    if len < width {
        let total_padding = width - len;
        let left = total_padding / 2;
        let right = total_padding - left;
        s = format!("{}{}{}", " ".repeat(left), s, " ".repeat(right));
    }
    s
}

fn format_elapsed_compact(elapsed_ms: i64, total_ms: i64) -> String {
    if total_ms > 0 {
        let pct = ((elapsed_ms.max(0) as f64 / total_ms as f64) * 100.0).min(999.0);
        format!("{} {:>3.0}%", format_duration_short(elapsed_ms), pct)
    } else {
        format_duration_short(elapsed_ms)
    }
}

fn format_iops(iops: f64) -> String {
    if !iops.is_finite() || iops <= 0.0 {
        "0".to_owned()
    } else if iops >= 1_000_000.0 {
        format!("{:.1}M", iops / 1_000_000.0)
    } else if iops >= 10_000.0 {
        format!("{:.1}K", iops / 1_000.0)
    } else {
        format!("{iops:.0}")
    }
}

fn format_count(n: i64) -> String {
    if n >= 1_000_000_000 {
        format!("{:.1}B", n as f64 / 1_000_000_000.0)
    } else if n >= 1_000_000 {
        format!("{:.1}M", n as f64 / 1_000_000.0)
    } else if n >= 10_000 {
        format!("{:.1}K", n as f64 / 1_000.0)
    } else {
        n.to_string()
    }
}

#[allow(clippy::cast_precision_loss)]
fn format_throughput_for_op(op: &str, mbps: f64) -> String {
    if op.starts_with("metadata.") {
        return "—".to_owned();
    }
    if !mbps.is_finite() || mbps <= 0.0 {
        "0B/s".to_owned()
    } else if mbps >= 1_000.0 {
        format!("{:.2}G/s", mbps / 1_000.0)
    } else if mbps >= 1.0 {
        format!("{mbps:.2}M/s")
    } else {
        format!("{:.1}K/s", mbps * 1_000.0)
    }
}

#[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
fn format_us_compact(v: f64) -> String {
    if !v.is_finite() || v <= 0.0 {
        return "—".to_owned();
    }
    if v >= 10_000.0 {
        format!("{:.0}ms", v / 1000.0)
    } else if v >= 1000.0 {
        format!("{:.1}ms", v / 1000.0)
    } else {
        format!("{}us", v as u64)
    }
}

fn format_duration_short(ms: i64) -> String {
    if ms <= 0 {
        return "0s".to_owned();
    }
    let secs = ms / 1000;
    let h = secs / 3600;
    let m = (secs % 3600) / 60;
    let s = secs % 60;
    if h > 0 {
        format!("{h}h{m:02}")
    } else if m > 0 {
        format!("{m}m{s:02}")
    } else {
        format!("{s}s")
    }
}

/// 把毫秒数格式化为 `HH:MM:SS`；负值或 0 显示 `00:00:00`。
fn format_duration(ms: i64) -> String {
    if ms <= 0 {
        return "00:00:00".to_owned();
    }
    let secs = ms / 1000;
    let h = secs / 3600;
    let m = (secs % 3600) / 60;
    let s = secs % 60;
    format!("{h:02}:{m:02}:{s:02}")
}

fn status_short(s: pb::JobStatus) -> &'static str {
    match s {
        pb::JobStatus::Pending => "PENDING",
        pb::JobStatus::Preparing => "PREPARING",
        pb::JobStatus::Running => "RUNNING",
        pb::JobStatus::Completed => "COMPLETED",
        pb::JobStatus::Failed => "FAILED",
        pb::JobStatus::Cancelled => "CANCELLED",
        pb::JobStatus::Unspecified => "UNSPECIFIED",
    }
}

fn kind_name(k: i32) -> &'static str {
    match pb::EventKind::try_from(k).unwrap_or(pb::EventKind::Unspecified) {
        pb::EventKind::StatusChange => "STATUS",
        pb::EventKind::Progress => "PROGRESS",
        pb::EventKind::Result => "RESULT",
        pb::EventKind::Error => "ERROR",
        pb::EventKind::Unspecified => "?",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn worker_label_uses_pid_and_uuid_prefix() {
        assert_eq!(
            worker_label("iv-yd974fr4sgcva4fcgd0j-27180-49b2e70d"),
            "27180-49b2e"
        );
        assert_eq!(
            worker_label("iv-yd974fr4sgcva4fcgd0j-27292-77c19f68"),
            "27292-77c19"
        );
    }

    #[test]
    fn format_elapsed_shows_progress_percent() {
        assert_eq!(format_elapsed_compact(183_000, 86_400_000), "3m03   0%");
        assert_eq!(format_elapsed_compact(43_200_000, 86_400_000), "12h00  50%");
    }
}

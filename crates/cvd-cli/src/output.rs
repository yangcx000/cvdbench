//! Spec §9.3 结果输出：默认 JSON，扩展名 `.csv` 时输出 CSV。
//!
//! JSON 字段命名严格对齐 spec §9.3 示例：
//! - `status` 为小写字符串（`completed` / `failed` / `cancelled` 等）；
//! - `duration_secs` / `effective_duration_secs` 浮点秒；
//! - `target_workers` / `run_workers` 顶层；
//! - `window_misaligned` 由 worker_results 重新计算；
//! - `aggregated` / `workers` 平铺为业务字段，不暴露 prost 内部 i32 / 直方图 bytes；
//! - `spec.read.s3_consistency_check.access_key/secret_key/session_token` 写入前
//!   再次 enforce 为 `"***"`，防御性兜底（master 已脱敏，此处冗余但安全）。
//!
//! CSV：每行对应「(worker_id, op)」一对。`worker_id="*"` 表示聚合（所有成功 worker
//! 该 op 合并）。列固定：
//! ```text
//! worker_id, op, success, total_ops, total_bytes, errors,
//! throughput_mbps, iops, p50_us, p95_us, p99_us, p999_us, max_us, avg_us,
//! effective_duration_ms
//! ```

use std::collections::BTreeMap;
use std::path::Path;

use cvd_common::spec::redact::REDACTED;
use cvd_proto::cvdbench as pb;
use serde::Serialize;

/// 顶层入口：根据 `path` 后缀选择 JSON / CSV，已脱敏。
pub fn write_report(path: &Path, query: &pb::QueryJobResponse) -> anyhow::Result<()> {
    let report = build_report(query);
    ensure_parent_dir(path)?;
    if path
        .extension()
        .is_some_and(|e| e.eq_ignore_ascii_case("csv"))
    {
        write_csv(path, &report)
    } else {
        write_json(path, &report)
    }
}

fn ensure_parent_dir(path: &Path) -> anyhow::Result<()> {
    let Some(parent) = path.parent() else {
        return Ok(());
    };
    if parent.as_os_str().is_empty() {
        return Ok(());
    }
    std::fs::create_dir_all(parent)
        .map_err(|e| anyhow::anyhow!("create output directory {}: {e}", parent.display()))
}

fn write_json(path: &Path, report: &JobReport) -> anyhow::Result<()> {
    let body = serde_json::to_string_pretty(report)
        .map_err(|e| anyhow::anyhow!("serialize result JSON: {e}"))?;
    std::fs::write(path, body).map_err(|e| anyhow::anyhow!("write {}: {e}", path.display()))?;
    Ok(())
}

fn write_csv(path: &Path, report: &JobReport) -> anyhow::Result<()> {
    let mut wtr = csv::WriterBuilder::new()
        .has_headers(true)
        .from_path(path)
        .map_err(|e| anyhow::anyhow!("open csv {}: {e}", path.display()))?;

    // 写入 header（serde 自动从 CsvRow 字段推导）
    for row in report.csv_rows() {
        wtr.serialize(row)
            .map_err(|e| anyhow::anyhow!("write csv row: {e}"))?;
    }
    wtr.flush().map_err(|e| anyhow::anyhow!("flush csv: {e}"))?;
    Ok(())
}

// ─── 报告结构（spec §9.3） ─────────────────────────────────────────────────

#[derive(Debug, Serialize)]
pub struct JobReport {
    pub job_id: String,
    pub status: String,
    pub created_at_ms: i64,
    /// job 级 error；若 master 未提供则回退到第一条失败 worker 的 error。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    pub duration_secs: f64,
    pub effective_duration_secs: f64,
    pub target_workers: i32,
    pub run_workers: usize,
    pub dirs_scanned: i64,
    pub files_scanned: i64,
    pub scan_duration_ms: i64,
    pub window_misaligned: bool,
    pub spec: pb::BenchSpec,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub aggregated: Option<AggregatedReport>,
    pub workers: Vec<WorkerReport>,
}

#[derive(Debug, Default, Clone, Serialize)]
pub struct AggregatedReport {
    pub throughput_mbps: f64,
    pub iops: f64,
    pub total_ops: i64,
    pub total_bytes: i64,
    pub errors: i64,
    pub error_rate: f64,
    pub latency_us: LatencyReport,
    pub per_op: BTreeMap<String, OpReport>,
}

#[derive(Debug, Default, Clone, Serialize)]
pub struct OpReport {
    pub throughput_mbps: f64,
    pub iops: f64,
    pub total_ops: i64,
    pub total_bytes: i64,
    pub errors: i64,
    pub error_rate: f64,
    pub latency_us: LatencyReport,
}

#[derive(Debug, Default, Clone, Serialize)]
pub struct LatencyReport {
    pub p50: f64,
    pub p95: f64,
    pub p99: f64,
    pub p999: f64,
    pub max: f64,
    pub avg: f64,
}

#[derive(Debug, Serialize)]
pub struct WorkerReport {
    pub worker_id: String,
    pub success: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    pub effective_duration_ms: i64,
    pub measure_start_ms: i64,
    pub measure_end_ms: i64,
    pub consistency_errors: Vec<pb::ConsistencyError>,
    pub per_op: BTreeMap<String, OpReport>,
}

#[derive(Debug, Serialize)]
struct CsvRow<'a> {
    worker_id: &'a str,
    op: &'a str,
    success: bool,
    total_ops: i64,
    total_bytes: i64,
    errors: i64,
    throughput_mbps: f64,
    iops: f64,
    p50_us: f64,
    p95_us: f64,
    p99_us: f64,
    p999_us: f64,
    max_us: f64,
    avg_us: f64,
    effective_duration_ms: i64,
}

// ─── Construction ─────────────────────────────────────────────────────────

pub fn build_report(query: &pb::QueryJobResponse) -> JobReport {
    let job = query.job.clone().unwrap_or_default();
    let mut spec = job.spec.unwrap_or_default();
    enforce_redaction(&mut spec);

    let target_workers = spec.target_workers;
    let run_workers = if query.run_workers > 0 {
        usize::try_from(query.run_workers).unwrap_or(usize::MAX)
    } else {
        query.worker_results.len()
    };
    let duration_secs = parse_duration_secs(&spec.duration);
    let (effective_duration_secs, window_misaligned) =
        compute_effective_window(&query.worker_results);

    let aggregated = query.aggregated.as_ref().map(build_aggregated_report);
    let workers: Vec<WorkerReport> = query
        .worker_results
        .iter()
        .map(build_worker_report)
        .collect();

    JobReport {
        job_id: job.job_id,
        status: status_to_str(job.status).to_owned(),
        created_at_ms: job.created_at,
        error: extract_error(query),
        duration_secs,
        effective_duration_secs,
        target_workers,
        run_workers,
        dirs_scanned: query.dirs_scanned,
        files_scanned: query.files_scanned,
        scan_duration_ms: query.scan_duration_ms,
        window_misaligned,
        spec,
        aggregated,
        workers,
    }
}

impl JobReport {
    fn csv_rows(&self) -> Vec<CsvRow<'_>> {
        let mut rows = Vec::new();
        let aggregate_success = self.status == "completed" && self.error.is_none();
        // 1) 聚合：worker_id="*", op="*" 总行 + 每个 op 一行
        if let Some(agg) = &self.aggregated {
            // 总行
            rows.push(CsvRow {
                worker_id: "*",
                op: "*",
                success: aggregate_success,
                total_ops: agg.total_ops,
                total_bytes: agg.total_bytes,
                errors: agg.errors,
                throughput_mbps: agg.throughput_mbps,
                iops: agg.iops,
                p50_us: agg.latency_us.p50,
                p95_us: agg.latency_us.p95,
                p99_us: agg.latency_us.p99,
                p999_us: agg.latency_us.p999,
                max_us: agg.latency_us.max,
                avg_us: agg.latency_us.avg,
                effective_duration_ms: (self.effective_duration_secs * 1000.0) as i64,
            });
            for (op, m) in &agg.per_op {
                rows.push(CsvRow {
                    worker_id: "*",
                    op,
                    success: aggregate_success,
                    total_ops: m.total_ops,
                    total_bytes: m.total_bytes,
                    errors: m.errors,
                    throughput_mbps: m.throughput_mbps,
                    iops: m.iops,
                    p50_us: m.latency_us.p50,
                    p95_us: m.latency_us.p95,
                    p99_us: m.latency_us.p99,
                    p999_us: m.latency_us.p999,
                    max_us: m.latency_us.max,
                    avg_us: m.latency_us.avg,
                    effective_duration_ms: (self.effective_duration_secs * 1000.0) as i64,
                });
            }
        }
        // 2) 每个 worker × op 一行
        for w in &self.workers {
            for (op, m) in &w.per_op {
                rows.push(CsvRow {
                    worker_id: &w.worker_id,
                    op,
                    success: w.success,
                    total_ops: m.total_ops,
                    total_bytes: m.total_bytes,
                    errors: m.errors,
                    throughput_mbps: m.throughput_mbps,
                    iops: m.iops,
                    p50_us: m.latency_us.p50,
                    p95_us: m.latency_us.p95,
                    p99_us: m.latency_us.p99,
                    p999_us: m.latency_us.p999,
                    max_us: m.latency_us.max,
                    avg_us: m.latency_us.avg,
                    effective_duration_ms: w.effective_duration_ms,
                });
            }
        }
        rows
    }
}

// ─── helpers ───────────────────────────────────────────────────────────────

fn status_to_str(i: i32) -> &'static str {
    match pb::JobStatus::try_from(i).unwrap_or(pb::JobStatus::Unspecified) {
        pb::JobStatus::Pending => "pending",
        pb::JobStatus::Preparing => "preparing",
        pb::JobStatus::Running => "running",
        pb::JobStatus::Completed => "completed",
        pb::JobStatus::Failed => "failed",
        pb::JobStatus::Cancelled => "cancelled",
        pb::JobStatus::Unspecified => "unspecified",
    }
}

fn parse_duration_secs(s: &str) -> f64 {
    cvd_common::parse::parse_duration(s)
        .ok()
        .flatten()
        .map_or(0.0, |d| d.as_secs_f64())
}

#[allow(clippy::cast_precision_loss)]
fn compute_effective_window(workers: &[pb::WorkerResult]) -> (f64, bool) {
    let succ: Vec<&pb::WorkerResult> = workers.iter().filter(|w| w.success).collect();
    if succ.is_empty() {
        return (0.0, false);
    }
    let max_start = succ.iter().map(|w| w.measure_start_ms).max().unwrap_or(0);
    let min_end = succ.iter().map(|w| w.measure_end_ms).min().unwrap_or(0);
    if min_end > max_start {
        ((min_end - max_start) as f64 / 1000.0, false)
    } else {
        let fallback = succ
            .iter()
            .map(|w| w.effective_duration_ms)
            .min()
            .unwrap_or(0);
        (fallback as f64 / 1000.0, true)
    }
}

fn build_aggregated_report(agg: &pb::AggregatedMetrics) -> AggregatedReport {
    let total = agg
        .total
        .as_ref()
        .map(perf_to_op_report)
        .unwrap_or_default();
    let per_op: BTreeMap<String, OpReport> = agg
        .total_per_op
        .iter()
        .map(|(k, m)| (k.clone(), perf_to_op_report(m)))
        .collect();
    AggregatedReport {
        throughput_mbps: total.throughput_mbps,
        iops: total.iops,
        total_ops: total.total_ops,
        total_bytes: total.total_bytes,
        errors: total.errors,
        error_rate: total.error_rate,
        latency_us: total.latency_us,
        per_op,
    }
}

fn perf_to_op_report(m: &pb::PerformanceMetrics) -> OpReport {
    let lat = m
        .latency_us
        .as_ref()
        .map(|l| LatencyReport {
            p50: l.p50,
            p95: l.p95,
            p99: l.p99,
            p999: l.p999,
            max: l.max,
            avg: l.avg,
        })
        .unwrap_or_default();
    OpReport {
        throughput_mbps: m.throughput_mbps,
        iops: m.iops,
        total_ops: m.total_ops,
        total_bytes: m.total_bytes,
        errors: m.error_count,
        error_rate: m.error_rate,
        latency_us: lat,
    }
}

fn build_worker_report(w: &pb::WorkerResult) -> WorkerReport {
    let per_op: BTreeMap<String, OpReport> = w
        .per_op
        .iter()
        .map(|(k, m)| (k.clone(), perf_to_op_report(m)))
        .collect();
    WorkerReport {
        worker_id: w.worker_id.clone(),
        success: w.success,
        error: w.error.clone(),
        effective_duration_ms: w.effective_duration_ms,
        measure_start_ms: w.measure_start_ms,
        measure_end_ms: w.measure_end_ms,
        consistency_errors: w.consistency_errors.clone(),
        per_op,
    }
}

fn enforce_redaction(spec: &mut pb::BenchSpec) {
    if let Some(read) = spec.read.as_mut() {
        if let Some(s3) = read.s3_consistency_check.as_mut() {
            if s3.access_key != REDACTED {
                s3.access_key = REDACTED.to_owned();
            }
            if s3.secret_key != REDACTED {
                s3.secret_key = REDACTED.to_owned();
            }
            if s3.session_token != REDACTED {
                s3.session_token = REDACTED.to_owned();
            }
        }
    }
}

/// 优先使用 job 级 error；兼容旧响应时回退到首个失败 worker 的 error。
fn extract_error(query: &pb::QueryJobResponse) -> Option<String> {
    if query.error.as_ref().is_some_and(|e| !e.is_empty()) {
        return query.error.clone();
    }
    query
        .worker_results
        .iter()
        .find(|w| !w.success)
        .and_then(|w| w.error.clone())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn metric(total_ops: i64, total_bytes: i64, errs: i64) -> pb::PerformanceMetrics {
        pb::PerformanceMetrics {
            throughput_mbps: 100.0,
            iops: 1000.0,
            latency_us: Some(pb::LatencyStats {
                p50: 10.0,
                p95: 20.0,
                p99: 30.0,
                p999: 40.0,
                max: 50.0,
                avg: 15.0,
            }),
            error_count: errs,
            error_rate: if total_ops > 0 {
                errs as f64 / total_ops as f64
            } else {
                0.0
            },
            total_ops,
            total_bytes,
            latency_histogram_hdr: vec![],
        }
    }

    fn worker_result(id: &str, success: bool, ms_start: i64, ms_end: i64) -> pb::WorkerResult {
        let mut per_op = HashMap::new();
        per_op.insert("read".to_owned(), metric(100, 4096 * 100, 0));
        pb::WorkerResult {
            worker_id: id.into(),
            per_op,
            consistency_errors: vec![],
            success,
            error: if success { None } else { Some("oops".into()) },
            effective_duration_ms: ms_end - ms_start,
            measure_start_ms: ms_start,
            measure_end_ms: ms_end,
        }
    }

    fn sample_query(status: pb::JobStatus, workers: &[pb::WorkerResult]) -> pb::QueryJobResponse {
        let spec = pb::BenchSpec {
            fs_name: "tmpfs".into(),
            io_mode: "seq".into(),
            io_aligned: true,
            direct_io: false,
            block_size: "4Ki".into(),
            duration: "1s".into(),
            warmup: String::new(),
            target_workers: 2,
            read: Some(pb::ReadConfig {
                concurrency: 1,
                file_manifest: "m.csv".into(),
                ..Default::default()
            }),
            write: None,
            metadata: None,
        };
        let mut total_per_op = HashMap::new();
        total_per_op.insert("read".to_owned(), metric(200, 8192 * 100, 0));
        let agg = pb::AggregatedMetrics {
            total: Some(metric(200, 8192 * 100, 0)),
            total_per_op,
            per_worker: workers.to_vec(),
        };
        pb::QueryJobResponse {
            job: Some(pb::Job {
                job_id: "j1".into(),
                spec: Some(spec),
                status: status.into(),
                created_at: 0,
            }),
            worker_results: workers.to_vec(),
            aggregated: Some(agg),
            error: None,
            run_workers: i32::try_from(workers.len()).unwrap(),
            dirs_scanned: 0,
            files_scanned: 0,
            scan_duration_ms: 0,
        }
    }

    #[test]
    fn build_report_uses_lowercase_status_and_seconds() {
        let workers = vec![
            worker_result("w1", true, 1000, 2000),
            worker_result("w2", true, 1000, 2000),
        ];
        let q = sample_query(pb::JobStatus::Completed, &workers);
        let r = build_report(&q);
        assert_eq!(r.status, "completed");
        assert!((r.duration_secs - 1.0).abs() < 1e-6);
        assert!((r.effective_duration_secs - 1.0).abs() < 1e-6);
        assert!(!r.window_misaligned);
        assert_eq!(r.target_workers, 2);
        assert_eq!(r.run_workers, 2);
    }

    #[test]
    fn build_report_prefers_query_run_workers() {
        let workers = vec![worker_result("w1", true, 1000, 2000)];
        let mut q = sample_query(pb::JobStatus::Failed, &workers);
        q.run_workers = 2;

        let r = build_report(&q);
        assert_eq!(r.workers.len(), 1);
        assert_eq!(r.run_workers, 2);
    }

    #[test]
    fn build_report_marks_window_misaligned() {
        let workers = vec![
            worker_result("w1", true, 0, 1000),
            worker_result("w2", true, 5000, 6000),
        ];
        let q = sample_query(pb::JobStatus::Completed, &workers);
        let r = build_report(&q);
        assert!(r.window_misaligned);
        // fallback 到 effective_duration_ms 最小值（都是 1000）→ 1.0s
        assert!((r.effective_duration_secs - 1.0).abs() < 1e-6);
    }

    #[test]
    fn build_report_extracts_error_from_failed_worker() {
        let workers = vec![
            worker_result("w1", true, 0, 1000),
            worker_result("w2", false, 0, 500),
        ];
        let q = sample_query(pb::JobStatus::Failed, &workers);
        let r = build_report(&q);
        assert_eq!(r.error.as_deref(), Some("oops"));
        assert_eq!(r.status, "failed");
    }

    #[test]
    fn build_report_prefers_job_level_error() {
        let workers = vec![worker_result("w1", false, 0, 500)];
        let mut q = sample_query(pb::JobStatus::Failed, &workers);
        q.error = Some("manifest reader: bad path".into());
        let r = build_report(&q);
        assert_eq!(r.error.as_deref(), Some("manifest reader: bad path"));
    }

    #[test]
    fn enforce_redaction_replaces_credentials_when_master_missed() {
        // 模拟一个未脱敏的 spec（理论上不会出现，但 CLI 端做防御）
        let workers = vec![worker_result("w1", true, 0, 1000)];
        let mut q = sample_query(pb::JobStatus::Completed, &workers);
        if let Some(read) = q.job.as_mut().unwrap().spec.as_mut().unwrap().read.as_mut() {
            read.s3_consistency_check = Some(pb::ConsistencyConfig {
                bucket_name: "b".into(),
                bucket_url: "http://s3".into(),
                access_key: "AK_LEAKED".into(),
                secret_key: "SK_LEAKED".into(),
                region: "us-east-1".into(),
                prefix: String::new(),
                session_token: "TOK_LEAKED".into(),
            });
        }
        let r = build_report(&q);
        let s3 = r
            .spec
            .read
            .as_ref()
            .unwrap()
            .s3_consistency_check
            .as_ref()
            .unwrap();
        assert_eq!(s3.access_key, REDACTED);
        assert_eq!(s3.secret_key, REDACTED);
        assert_eq!(s3.session_token, REDACTED);
    }

    #[test]
    fn csv_rows_include_aggregate_total_and_per_worker_per_op() {
        let workers = vec![
            worker_result("w1", true, 1000, 2000),
            worker_result("w2", true, 1000, 2000),
        ];
        let q = sample_query(pb::JobStatus::Completed, &workers);
        let r = build_report(&q);
        let rows = r.csv_rows();
        // 期望：总行（worker_id="*", op="*") + 1 个聚合 op 行 + 2 个 worker × 1 op = 4 行
        assert_eq!(rows.len(), 4);
        assert_eq!(rows[0].worker_id, "*");
        assert_eq!(rows[0].op, "*");
        assert_eq!(rows[1].worker_id, "*");
        assert_eq!(rows[1].op, "read");
        assert!(rows[0].success);
    }

    #[test]
    fn csv_aggregate_rows_mark_failed_job_unsuccessful() {
        let workers = vec![worker_result("w1", false, 0, 500)];
        let q = sample_query(pb::JobStatus::Failed, &workers);
        let r = build_report(&q);
        let rows = r.csv_rows();
        assert!(!rows[0].success);
        assert!(!rows[1].success);
    }

    #[test]
    fn write_json_then_round_trip_status() {
        let workers = vec![worker_result("w1", true, 0, 1000)];
        let q = sample_query(pb::JobStatus::Completed, &workers);
        let r = build_report(&q);
        let json = serde_json::to_string(&r).unwrap();
        // status 出现为字符串，不是数字
        assert!(json.contains("\"status\":\"completed\""));
        // window_misaligned 字段存在且为 false
        assert!(json.contains("\"window_misaligned\":false"));
    }

    #[test]
    fn write_report_creates_parent_dirs_and_accepts_uppercase_csv_extension() {
        let workers = vec![worker_result("w1", true, 0, 1000)];
        let q = sample_query(pb::JobStatus::Completed, &workers);
        let root = std::env::temp_dir().join(format!("cvd-cli-output-test-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        let path = root.join("nested").join("RESULT.CSV");

        write_report(&path, &q).unwrap();

        let body = std::fs::read_to_string(&path).unwrap();
        assert!(body.starts_with("worker_id,op,success,"));
        let _ = std::fs::remove_dir_all(root);
    }
}

//! Prometheus text endpoint for latest per-worker job metrics.

use std::fmt::Write as _;
use std::net::SocketAddr;
use std::sync::Arc;

use cvd_proto::cvdbench as pb;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

use crate::state::MasterState;

pub fn spawn_endpoint(state: Arc<MasterState>, listen: SocketAddr) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        if let Err(err) = serve(state, listen).await {
            tracing::error!(%listen, %err, "metrics endpoint stopped");
        }
    })
}

async fn serve(state: Arc<MasterState>, listen: SocketAddr) -> std::io::Result<()> {
    let listener = TcpListener::bind(listen).await?;
    tracing::info!(%listen, "cvd-master metrics endpoint listening");
    loop {
        let (mut socket, peer) = listener.accept().await?;
        let state = state.clone();
        tokio::spawn(async move {
            let mut buf = [0u8; 1024];
            let n = match socket.read(&mut buf).await {
                Ok(n) => n,
                Err(err) => {
                    tracing::debug!(%peer, %err, "read metrics request failed");
                    return;
                }
            };
            let request = String::from_utf8_lossy(&buf[..n]);
            let path = parse_path(&request);
            let (status, content_type, body) = if path == "/metrics" {
                (
                    "200 OK",
                    "text/plain; version=0.0.4; charset=utf-8",
                    render(&state),
                )
            } else {
                (
                    "404 Not Found",
                    "text/plain; charset=utf-8",
                    "not found\n".to_owned(),
                )
            };
            let response = format!(
                "HTTP/1.1 {status}\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            );
            let _ = socket.write_all(response.as_bytes()).await;
        });
    }
}

fn parse_path(request: &str) -> &str {
    request
        .lines()
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
        .unwrap_or("/")
}

pub fn render(state: &MasterState) -> String {
    let mut out = String::new();
    out.push_str("# HELP cvdbench_job_info cvdbench job metadata.\n");
    out.push_str("# TYPE cvdbench_job_info gauge\n");
    out.push_str("# HELP cvdbench_worker_phase Worker phase enum value for latest progress.\n");
    out.push_str("# TYPE cvdbench_worker_phase gauge\n");
    out.push_str("# HELP cvdbench_worker_elapsed_ms Latest worker elapsed time in milliseconds.\n");
    out.push_str("# TYPE cvdbench_worker_elapsed_ms gauge\n");
    out.push_str(
        "# HELP cvdbench_worker_op_throughput_mbps Latest worker operation throughput in MB/s.\n",
    );
    out.push_str("# TYPE cvdbench_worker_op_throughput_mbps gauge\n");
    out.push_str("# HELP cvdbench_worker_op_iops Latest worker operation IOPS.\n");
    out.push_str("# TYPE cvdbench_worker_op_iops gauge\n");
    out.push_str(
        "# HELP cvdbench_worker_op_total_ops Latest worker operation total ops within the job.\n",
    );
    out.push_str("# TYPE cvdbench_worker_op_total_ops gauge\n");
    out.push_str("# HELP cvdbench_worker_op_total_bytes Latest worker operation total bytes within the job.\n");
    out.push_str("# TYPE cvdbench_worker_op_total_bytes gauge\n");
    out.push_str(
        "# HELP cvdbench_worker_op_errors Latest worker operation error count within the job.\n",
    );
    out.push_str("# TYPE cvdbench_worker_op_errors gauge\n");
    out.push_str("# HELP cvdbench_worker_op_error_rate Latest worker operation error rate.\n");
    out.push_str("# TYPE cvdbench_worker_op_error_rate gauge\n");
    out.push_str("# HELP cvdbench_worker_op_latency_us Latest worker operation latency in microseconds by quantile/stat.\n");
    out.push_str("# TYPE cvdbench_worker_op_latency_us gauge\n");

    let jobs = state.jobs.lock().expect("jobs mutex");
    let mut job_ids: Vec<_> = jobs.keys().cloned().collect();
    job_ids.sort();
    for job_id in job_ids {
        let record = &jobs[&job_id];
        let status = status_label(record.status);
        let fs_name = &record.spec_redacted.fs_name;
        let job_labels = format!(
            "job_id=\"{}\",status=\"{}\",fs_name=\"{}\"",
            escape_label(&job_id),
            escape_label(status),
            escape_label(fs_name)
        );
        let _ = writeln!(out, "cvdbench_job_info{{{job_labels}}} 1");

        let mut worker_ids: Vec<_> = record.latest_progress.keys().cloned().collect();
        worker_ids.sort();
        for worker_id in worker_ids {
            let progress = &record.latest_progress[&worker_id];
            let worker_labels = format!(
                "job_id=\"{}\",worker_id=\"{}\"",
                escape_label(&job_id),
                escape_label(&worker_id)
            );
            let _ = writeln!(
                out,
                "cvdbench_worker_phase{{{worker_labels}}} {}",
                progress.phase
            );
            let _ = writeln!(
                out,
                "cvdbench_worker_elapsed_ms{{{worker_labels}}} {}",
                progress.elapsed_ms
            );

            let mut ops: Vec<_> = progress.per_op.iter().collect();
            ops.sort_by(|a, b| a.0.cmp(b.0));
            for (op, metric) in ops {
                write_metric_lines(&mut out, &job_id, &worker_id, op, metric);
            }
        }
    }
    out
}

fn write_metric_lines(
    out: &mut String,
    job_id: &str,
    worker_id: &str,
    op: &str,
    metric: &pb::PerformanceMetrics,
) {
    let labels = format!(
        "job_id=\"{}\",worker_id=\"{}\",op=\"{}\"",
        escape_label(job_id),
        escape_label(worker_id),
        escape_label(op)
    );
    let _ = writeln!(
        out,
        "cvdbench_worker_op_throughput_mbps{{{labels}}} {}",
        finite(metric.throughput_mbps)
    );
    let _ = writeln!(
        out,
        "cvdbench_worker_op_iops{{{labels}}} {}",
        finite(metric.iops)
    );
    let _ = writeln!(
        out,
        "cvdbench_worker_op_total_ops{{{labels}}} {}",
        metric.total_ops
    );
    let _ = writeln!(
        out,
        "cvdbench_worker_op_total_bytes{{{labels}}} {}",
        metric.total_bytes
    );
    let _ = writeln!(
        out,
        "cvdbench_worker_op_errors{{{labels}}} {}",
        metric.error_count
    );
    let _ = writeln!(
        out,
        "cvdbench_worker_op_error_rate{{{labels}}} {}",
        finite(metric.error_rate)
    );
    let latency = metric.latency_us.clone().unwrap_or_default();
    for (stat, value) in [
        ("p50", latency.p50),
        ("p95", latency.p95),
        ("p99", latency.p99),
        ("p999", latency.p999),
        ("max", latency.max),
        ("avg", latency.avg),
    ] {
        let _ = writeln!(
            out,
            "cvdbench_worker_op_latency_us{{{labels},stat=\"{stat}\"}} {}",
            finite(value)
        );
    }
}

fn status_label(status: pb::JobStatus) -> &'static str {
    match status {
        pb::JobStatus::Unspecified => "unspecified",
        pb::JobStatus::Pending => "pending",
        pb::JobStatus::Preparing => "preparing",
        pb::JobStatus::Running => "running",
        pb::JobStatus::Completed => "completed",
        pb::JobStatus::Failed => "failed",
        pb::JobStatus::Cancelled => "cancelled",
    }
}

fn escape_label(value: &str) -> String {
    value
        .chars()
        .flat_map(|c| match c {
            '\\' => "\\\\".chars().collect::<Vec<_>>(),
            '"' => "\\\"".chars().collect::<Vec<_>>(),
            '\n' => "\\n".chars().collect::<Vec<_>>(),
            _ => vec![c],
        })
        .collect()
}

fn finite(value: f64) -> f64 {
    if value.is_finite() {
        value
    } else {
        0.0
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::path::PathBuf;

    use cvd_proto::cvdbench as pb;

    use super::*;
    use crate::config::{MasterConfig, SchedulerConfig};
    use crate::state::JobRecord;

    #[test]
    fn render_worker_progress_metrics() {
        let mut filesystems = HashMap::new();
        filesystems.insert("tmpfs".to_owned(), PathBuf::from("/mnt/tmpfs"));
        let state = MasterState::new(MasterConfig {
            listen: "127.0.0.1:0".parse().unwrap(),
            metrics_listen: None,
            scheduler: SchedulerConfig::default(),
            filesystems,
        });

        let mut record = JobRecord::new(
            "job-1".to_owned(),
            pb::BenchSpec {
                fs_name: "tmpfs".to_owned(),
                ..pb::BenchSpec::default()
            },
            None,
            PathBuf::from("/mnt/tmpfs"),
            1,
            1,
        );
        record.status = pb::JobStatus::Running;
        let mut per_op = HashMap::new();
        per_op.insert(
            "read.open".to_owned(),
            pb::PerformanceMetrics {
                throughput_mbps: 0.0,
                iops: 10.0,
                latency_us: Some(pb::LatencyStats {
                    p50: 10.0,
                    p95: 20.0,
                    p99: 30.0,
                    p999: 40.0,
                    max: 50.0,
                    avg: 15.0,
                }),
                error_count: 0,
                error_rate: 0.0,
                total_ops: 10,
                total_bytes: 0,
                latency_histogram_hdr: vec![],
            },
        );
        record.latest_progress.insert(
            "worker-1".to_owned(),
            pb::WorkerProgress {
                worker_id: "worker-1".to_owned(),
                elapsed_ms: 1234,
                per_op,
                phase: pb::WorkerPhase::Measuring as i32,
            },
        );
        state
            .jobs
            .lock()
            .expect("jobs")
            .insert(record.job_id.clone(), record);

        let body = render(&state);
        assert!(body
            .contains("cvdbench_job_info{job_id=\"job-1\",status=\"running\",fs_name=\"tmpfs\"} 1"));
        assert!(body.contains(
            "cvdbench_worker_op_iops{job_id=\"job-1\",worker_id=\"worker-1\",op=\"read.open\"} 10"
        ));
        assert!(body.contains("cvdbench_worker_op_latency_us{job_id=\"job-1\",worker_id=\"worker-1\",op=\"read.open\",stat=\"p99\"} 30"));
    }

    #[test]
    fn render_terminal_worker_result_metrics() {
        let mut filesystems = HashMap::new();
        filesystems.insert("tmpfs".to_owned(), PathBuf::from("/mnt/tmpfs"));
        let state = MasterState::new(MasterConfig {
            listen: "127.0.0.1:0".parse().unwrap(),
            metrics_listen: None,
            scheduler: SchedulerConfig::default(),
            filesystems,
        });

        let mut record = JobRecord::new(
            "job-2".to_owned(),
            pb::BenchSpec {
                fs_name: "tmpfs".to_owned(),
                ..pb::BenchSpec::default()
            },
            None,
            PathBuf::from("/mnt/tmpfs"),
            1,
            1,
        );
        record.status = pb::JobStatus::Completed;
        let mut per_op = HashMap::new();
        per_op.insert(
            "read".to_owned(),
            pb::PerformanceMetrics {
                throughput_mbps: 123.0,
                iops: 456.0,
                latency_us: None,
                error_count: 0,
                error_rate: 0.0,
                total_ops: 789,
                total_bytes: 1024,
                latency_histogram_hdr: vec![],
            },
        );
        record.latest_progress.insert(
            "worker-2".to_owned(),
            pb::WorkerProgress {
                worker_id: "worker-2".to_owned(),
                elapsed_ms: 60000,
                per_op,
                phase: pb::WorkerPhase::Finished as i32,
            },
        );
        state
            .jobs
            .lock()
            .expect("jobs")
            .insert(record.job_id.clone(), record);

        let body = render(&state);
        assert!(body.contains(
            "cvdbench_job_info{job_id=\"job-2\",status=\"completed\",fs_name=\"tmpfs\"} 1"
        ));
        assert!(body.contains(
            "cvdbench_worker_phase{job_id=\"job-2\",worker_id=\"worker-2\"} 6"
        ));
        assert!(body.contains(
            "cvdbench_worker_op_iops{job_id=\"job-2\",worker_id=\"worker-2\",op=\"read\"} 456"
        ));
    }
}

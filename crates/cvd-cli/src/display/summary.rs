//! Job 完成后的汇总报告渲染（spec §7 "Job Completed" 模板）。

use cvd_proto::cvdbench as pb;

#[derive(Debug, Clone, Copy, Default)]
pub struct SummaryOptions {
    pub verbose: bool,
}

pub fn print(query: &pb::QueryJobResponse) {
    print_with_options(query, SummaryOptions::default());
}

pub fn print_with_options(query: &pb::QueryJobResponse, options: SummaryOptions) {
    let Some(job) = &query.job else {
        println!("(empty QueryJobResponse — job not found?)");
        return;
    };

    println!("Job Summary");
    println!("───────────");
    println!("Job ID  : {}", job.job_id);
    println!("Status  : {}", status_name(job.status));
    println!("Workers : {}", worker_summary(query));
    println!("Progress: {}", progress_summary(query));

    if options.verbose {
        if let Some(spec) = &job.spec {
            println!();
            println!("Workload");
            println!("────────");
            println!("Duration : {}", spec.duration);
            println!("Block    : {}", spec.block_size);
            println!(
                "Workers  : target={} run={}",
                spec.target_workers, query.run_workers
            );
        }
    }

    if query.dirs_scanned > 0 || query.files_scanned > 0 || query.scan_duration_ms > 0 {
        println!();
        println!("Manifest Scan");
        println!("─────────────");
        print_table(
            &["Dirs", "Files", "Duration"],
            &[vec![
                format_count(query.dirs_scanned),
                format_count(query.files_scanned),
                format_duration_ms(query.scan_duration_ms),
            ]],
        );
    }

    if let Some(agg) = &query.aggregated {
        if let Some(total) = &agg.total {
            let mut rows = vec![metric_row("aggregate", total)];
            if options.verbose {
                let mut ops: Vec<_> = agg.total_per_op.iter().collect();
                ops.sort_by(|a, b| a.0.cmp(b.0));
                rows.extend(ops.into_iter().map(|(op, m)| metric_row(op, m)));
            }

            println!();
            println!("Performance");
            println!("───────────");
            print_table(
                &["Scope", "Throughput", "Ops/s", "Ops", "Errors", "Bytes"],
                &rows,
            );

            let mut latency_rows = vec![latency_row("aggregate", total)];
            if options.verbose {
                let mut ops: Vec<_> = agg.total_per_op.iter().collect();
                ops.sort_by(|a, b| a.0.cmp(b.0));
                latency_rows.extend(ops.into_iter().map(|(op, m)| latency_row(op, m)));
            }
            println!();
            println!("Latency");
            println!("───────");
            print_table(
                &["Scope", "p50", "p95", "p99", "p999", "max", "avg"],
                &latency_rows,
            );
        }

        let (misaligned, missing_hist) = compute_diagnostics(&agg.per_worker);
        let mut warnings = Vec::new();
        if misaligned {
            warnings.push(
                "Worker timing is misaligned; aggregate metrics may be less reliable".to_owned(),
            );
        }
        if missing_hist > 0 {
            warnings.push(format!(
                "Latency histograms incomplete: {missing_hist} worker(s) missing histogram"
            ));
        }
        if !warnings.is_empty() {
            println!();
            println!("Warnings");
            println!("────────");
            for warning in warnings {
                println!("- {warning}");
            }
        }
    }

    if let Some(err) = &query.error {
        println!();
        println!("Error");
        println!("─────");
        println!("{err}");
    }
}

fn worker_summary(query: &pb::QueryJobResponse) -> String {
    if query.worker_results.is_empty() {
        let run_workers = query.run_workers.max(0);
        return format!("0 reported, {run_workers} running");
    }
    let success = query.worker_results.iter().filter(|r| r.success).count();
    let failed = query.worker_results.len().saturating_sub(success);
    format!(
        "{} reported, {success} success, {failed} failed",
        query.worker_results.len()
    )
}

fn progress_summary(query: &pb::QueryJobResponse) -> String {
    let Some(job) = &query.job else {
        return "unknown".to_owned();
    };
    let status = pb::JobStatus::try_from(job.status).unwrap_or(pb::JobStatus::Unspecified);
    if matches!(
        status,
        pb::JobStatus::Completed | pb::JobStatus::Failed | pb::JobStatus::Cancelled
    ) {
        return "100%".to_owned();
    }
    let Some(spec) = &job.spec else {
        return "running".to_owned();
    };
    let Some(duration_ms) = parse_duration_ms(&spec.duration) else {
        return "running".to_owned();
    };
    let elapsed_ms = query
        .worker_results
        .iter()
        .map(|w| w.effective_duration_ms)
        .max()
        .unwrap_or(0);
    if duration_ms <= 0 {
        "running".to_owned()
    } else {
        let pct = ((elapsed_ms.max(0) as f64 / duration_ms as f64) * 100.0).min(100.0);
        format!(
            "{pct:.1}% ({}/{})",
            format_duration_ms(elapsed_ms),
            format_duration_ms(duration_ms)
        )
    }
}

fn parse_duration_ms(raw: &str) -> Option<i64> {
    cvd_common::parse::duration::parse_duration(raw)
        .ok()
        .flatten()
        .and_then(|duration| i64::try_from(duration.as_millis()).ok())
}

fn metric_row(name: &str, metric: &pb::PerformanceMetrics) -> Vec<String> {
    vec![
        name.to_owned(),
        format_throughput(metric.throughput_mbps),
        format_iops(metric.iops),
        format_count(metric.total_ops),
        format!(
            "{} ({:.2}%)",
            format_count(metric.error_count),
            metric.error_rate * 100.0
        ),
        format_count(metric.total_bytes),
    ]
}

fn latency_row(name: &str, metric: &pb::PerformanceMetrics) -> Vec<String> {
    let stats = metric.latency_us.as_ref();
    vec![
        name.to_owned(),
        format_latency_us(stats.map_or(0.0, |s| s.p50)),
        format_latency_us(stats.map_or(0.0, |s| s.p95)),
        format_latency_us(stats.map_or(0.0, |s| s.p99)),
        format_latency_us(stats.map_or(0.0, |s| s.p999)),
        format_latency_us(stats.map_or(0.0, |s| s.max)),
        format_latency_us(stats.map_or(0.0, |s| s.avg)),
    ]
}

fn print_table(headers: &[&str], rows: &[Vec<String>]) {
    let widths: Vec<usize> = headers
        .iter()
        .enumerate()
        .map(|(idx, header)| {
            rows.iter()
                .filter_map(|row| row.get(idx))
                .map(|cell| display_width(cell))
                .max()
                .unwrap_or(0)
                .max(display_width(header))
        })
        .collect();

    print_table_border('┌', '┬', '┐', &widths);
    print_table_row(headers.iter().copied(), &widths);
    print_table_border('├', '┼', '┤', &widths);
    for row in rows {
        print_table_row(row.iter().map(String::as_str), &widths);
    }
    print_table_border('└', '┴', '┘', &widths);
}

fn print_table_border(left: char, middle: char, right: char, widths: &[usize]) {
    print!("{left}");
    for (idx, width) in widths.iter().enumerate() {
        if idx > 0 {
            print!("{middle}");
        }
        print!("{}", "─".repeat(width + 2));
    }
    println!("{right}");
}

fn print_table_row<'a>(cells: impl Iterator<Item = &'a str>, widths: &[usize]) {
    print!("│");
    for (cell, width) in cells.zip(widths.iter()) {
        let padding = width.saturating_sub(display_width(cell));
        print!(" {cell}{} │", " ".repeat(padding));
    }
    println!();
}

fn display_width(s: &str) -> usize {
    s.chars().count()
}

/// 从 per_worker 重算 window_misaligned + 缺失 histogram 的 worker 数。
fn compute_diagnostics(workers: &[pb::WorkerResult]) -> (bool, usize) {
    let succ: Vec<&pb::WorkerResult> = workers.iter().filter(|w| w.success).collect();
    let misaligned = if succ.len() < 2 {
        false
    } else {
        let max_start = succ.iter().map(|w| w.measure_start_ms).max().unwrap_or(0);
        let min_end = succ.iter().map(|w| w.measure_end_ms).min().unwrap_or(0);
        min_end <= max_start
    };
    let missing = succ
        .iter()
        .filter(|w| {
            w.per_op
                .values()
                .any(|m| m.latency_histogram_hdr.is_empty() && m.total_ops > 0)
        })
        .count();
    (misaligned, missing)
}

#[allow(clippy::cast_precision_loss)]
fn format_throughput(mbps: f64) -> String {
    if mbps >= 1_000.0 {
        format!("{:.2} GB/s", mbps / 1_000.0)
    } else if mbps >= 1.0 {
        format!("{mbps:.2} MB/s")
    } else if mbps > 0.0 {
        format!("{:.2} KB/s", mbps * 1_000.0)
    } else {
        "0 B/s".to_owned()
    }
}

fn format_iops(iops: f64) -> String {
    if !iops.is_finite() || iops <= 0.0 {
        "0".to_owned()
    } else if iops >= 1_000_000.0 {
        format!("{:.2}M", iops / 1_000_000.0)
    } else if iops >= 10_000.0 {
        format!("{:.1}K", iops / 1_000.0)
    } else if iops >= 1.0 {
        format!("{iops:.0}")
    } else {
        format!("{iops:.3}")
    }
}

#[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
fn format_latency_us(v: f64) -> String {
    if !v.is_finite() || v <= 0.0 {
        "0 µs".to_owned()
    } else if v >= 1000.0 {
        format!("{:.2} ms", v / 1000.0)
    } else {
        format!("{} µs", v as u64)
    }
}

fn format_count(n: i64) -> String {
    let sign = if n < 0 { "-" } else { "" };
    let digits = n.abs().to_string();
    let mut out = String::new();
    for (idx, ch) in digits.chars().rev().enumerate() {
        if idx > 0 && idx % 3 == 0 {
            out.push(',');
        }
        out.push(ch);
    }
    let grouped: String = out.chars().rev().collect();
    format!("{sign}{grouped}")
}

fn format_duration_ms(ms: i64) -> String {
    if ms <= 0 {
        return "0ms".to_owned();
    }
    let total_secs = ms / 1000;
    let millis = ms % 1000;
    let hours = total_secs / 3600;
    let minutes = (total_secs % 3600) / 60;
    let seconds = total_secs % 60;
    if hours > 0 {
        format!("{hours}h {minutes}m {seconds}s")
    } else if minutes > 0 {
        format!("{minutes}m {seconds}s")
    } else if seconds > 0 {
        format!("{seconds}.{millis:03}s")
    } else {
        format!("{ms}ms")
    }
}

fn status_name(s: i32) -> &'static str {
    match pb::JobStatus::try_from(s).unwrap_or(pb::JobStatus::Unspecified) {
        pb::JobStatus::Pending => "PENDING",
        pb::JobStatus::Preparing => "PREPARING",
        pb::JobStatus::Running => "RUNNING",
        pb::JobStatus::Completed => "COMPLETED",
        pb::JobStatus::Failed => "FAILED",
        pb::JobStatus::Cancelled => "CANCELLED",
        pb::JobStatus::Unspecified => "UNSPECIFIED",
    }
}

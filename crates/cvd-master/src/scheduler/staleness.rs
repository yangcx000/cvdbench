//! `worker_last_seen` 巡检（spec §5.5）。
//!
//! 每个 tick 扫描所有非终态 job，触发以下 → FAILED：
//! 1. **staleness**：`run_workers` 中任一 worker 超过 `worker_staleness_secs`
//!    未上报任何 RPC；
//! 2. **prepare timeout**：`preparing_since` 距今超过 `prepare_timeout_secs`
//!    仍未集齐全员 ReportReady。
//!
//! 与状态转换的约定保持一致：
//! - 持锁内 `aggregate::aggregate_into_record` + `events::emit_for_job`；
//! - 锁释放后 `finalize_stream` + 清理 `worker_active_jobs`。

use std::sync::Arc;
use std::time::{Duration, Instant};

use cvd_proto::cvdbench as pb;
use tokio::task::JoinHandle;
use tokio::time::MissedTickBehavior;

use crate::aggregate;
use crate::config::SchedulerConfig;
use crate::events;
use crate::state::{JobRecord, MasterState};

/// 巡检间隔策略：默认取 `worker_staleness / 3`，下限 100ms（避免 CPU 烧空），
/// 上限 5s（默认 60s staleness 时仍能 5s 一巡）。短 staleness 配置（测试 / demo）
/// 自动得到亚秒级 tick；生产默认配置得到 5s tick。
const STALENESS_TICK_MIN: Duration = Duration::from_millis(100);
const STALENESS_TICK_MAX: Duration = Duration::from_secs(5);

fn tick_interval(cfg: &SchedulerConfig) -> Duration {
    let candidate = cfg.worker_staleness / 3;
    candidate.clamp(STALENESS_TICK_MIN, STALENESS_TICK_MAX)
}

/// 启动 staleness watcher。返回的 JoinHandle 由调用方保管/abort。
pub fn spawn_watcher(state: Arc<MasterState>) -> JoinHandle<()> {
    let interval_dur = tick_interval(&state.config.scheduler);
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(interval_dur);
        ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
        ticker.tick().await; // 跳过立即触发的首次 tick
        loop {
            ticker.tick().await;
            tick_once(&state);
        }
    })
}

/// 单次巡检。可独立调用，便于单元测试。
pub fn tick_once(state: &MasterState) {
    let staleness = state.config.scheduler.worker_staleness;
    let prepare_timeout = state.config.scheduler.prepare_timeout;
    let now = Instant::now();

    let mut to_clean: Vec<(String, Vec<String>)> = Vec::new();
    {
        let mut jobs = state.jobs.lock().expect("jobs mutex");
        for (id, record) in jobs.iter_mut() {
            if !matches!(
                record.status,
                pb::JobStatus::Preparing | pb::JobStatus::Running
            ) {
                continue;
            }

            // 1) staleness：扫描 run_workers
            let stale = find_stale_worker(record, now, staleness);

            // 2) prepare_timeout
            let prepare_overdue = record.status == pb::JobStatus::Preparing
                && record
                    .preparing_since
                    .is_some_and(|t| now.duration_since(t) > prepare_timeout);

            let reason = if let Some((w, age)) = stale {
                Some(format!(
                    "worker {w} stale ({age_ms} ms > worker_staleness {staleness_ms} ms)",
                    age_ms = age.as_millis(),
                    staleness_ms = staleness.as_millis(),
                ))
            } else if prepare_overdue {
                Some(format!(
                    "PREPARING exceeded prepare_timeout ({} ms) without all ReportReady",
                    prepare_timeout.as_millis()
                ))
            } else {
                None
            };

            if let Some(reason) = reason {
                tracing::warn!(job_id = %id, %reason, "watcher: job → FAILED");
                record.status = pb::JobStatus::Failed;
                record.error = Some(reason);
                let assigned: Vec<String> = record.worker_assignments.keys().cloned().collect();
                aggregate::aggregate_into_record(record);
                record.cleanup_on_terminal();
                events::emit_for_job(state, record, pb::EventKind::StatusChange);
                to_clean.push((id.clone(), assigned));
            }
        }
    }

    if to_clean.is_empty() {
        return;
    }

    // 释放 jobs 锁后再做 finalize_stream + active 清理，避免持锁时间过长。
    {
        let mut active = state.worker_active_jobs.lock().expect("active mutex");
        for (id, workers) in &to_clean {
            for w in workers {
                if active.get(w) == Some(id) {
                    active.remove(w);
                }
            }
        }
    }
    for (id, _) in &to_clean {
        events::finalize_stream(state, id);
    }
}

fn find_stale_worker(
    record: &JobRecord,
    now: Instant,
    threshold: Duration,
) -> Option<(String, Duration)> {
    for w in &record.run_workers {
        let last_seen = record
            .worker_last_seen
            .get(w)
            .copied()
            .or(record.preparing_since)
            .unwrap_or(now);
        let age = now.duration_since(last_seen);
        if age > threshold {
            return Some((w.clone(), age));
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::path::PathBuf;
    use std::sync::Arc;

    use cvd_proto::cvdbench as pb;

    use super::*;
    use crate::config::{MasterConfig, SchedulerConfig};
    use crate::state::{JobRecord, MasterState};

    fn test_state(staleness: Duration, prepare_timeout: Duration) -> Arc<MasterState> {
        let mut filesystems = HashMap::new();
        filesystems.insert("examplefs".into(), PathBuf::from("/mnt/examplefs"));
        let cfg = MasterConfig {
            listen: "127.0.0.1:0".parse().unwrap(),
            metrics_listen: None,
            scheduler: SchedulerConfig {
                worker_staleness: staleness,
                prepare_timeout,
                ..SchedulerConfig::default()
            },
            filesystems,
        };
        Arc::new(MasterState::new(cfg))
    }

    fn redacted_spec() -> pb::BenchSpec {
        pb::BenchSpec {
            fs_name: "examplefs".into(),
            io_mode: "seq".into(),
            io_aligned: true,
            direct_io: false,
            block_size: "1Mi".into(),
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
        }
    }

    fn install_running_job(
        state: &MasterState,
        id: &str,
        run_workers: &[&str],
        last_seen_age: Duration,
    ) {
        let mut record = JobRecord::new(
            id.into(),
            redacted_spec(),
            None,
            PathBuf::from("/mnt/examplefs"),
            run_workers.len() as u32,
            0,
        );
        record.status = pb::JobStatus::Running;
        let baseline = Instant::now() - last_seen_age;
        for w in run_workers {
            record.worker_assignments.insert(
                (*w).into(),
                crate::state::WorkerAssignment { worker_index: 0 },
            );
            record.run_workers.insert((*w).to_string());
            record.worker_last_seen.insert((*w).into(), baseline);
        }
        let mut jobs = state.jobs.lock().unwrap();
        jobs.insert(id.into(), record);
    }

    #[test]
    fn fails_job_when_worker_stale() {
        let state = test_state(Duration::from_millis(100), Duration::from_secs(60));
        install_running_job(&state, "j1", &["w1", "w2"], Duration::from_millis(500));
        // 注册一个 active 映射，验证 finalize 时清理
        state
            .worker_active_jobs
            .lock()
            .unwrap()
            .insert("w1".into(), "j1".into());

        tick_once(&state);

        let jobs = state.jobs.lock().unwrap();
        let rec = jobs.get("j1").unwrap();
        assert_eq!(rec.status, pb::JobStatus::Failed);
        assert!(rec.error.as_deref().unwrap().contains("stale"));
        assert!(rec.aggregated.is_some());
        drop(jobs);

        let active = state.worker_active_jobs.lock().unwrap();
        assert!(!active.contains_key("w1"), "active map should be cleared");
    }

    #[test]
    fn prepare_timeout_makes_job_failed() {
        let state = test_state(Duration::from_secs(60), Duration::from_millis(100));
        // PREPARING 超时但没有 worker stale
        let mut record = JobRecord::new(
            "j1".into(),
            redacted_spec(),
            None,
            PathBuf::from("/mnt/examplefs"),
            2,
            0,
        );
        record.status = pb::JobStatus::Preparing;
        // preparing_since 在过去，但 worker_last_seen 在当下 → 仅 prepare_timeout 触发
        record.preparing_since = Some(Instant::now() - Duration::from_millis(500));
        for w in ["w1", "w2"] {
            record
                .worker_assignments
                .insert(w.into(), crate::state::WorkerAssignment { worker_index: 0 });
            record.run_workers.insert(w.into());
            record.worker_last_seen.insert(w.into(), Instant::now());
        }
        state.jobs.lock().unwrap().insert("j1".into(), record);

        tick_once(&state);

        let jobs = state.jobs.lock().unwrap();
        let rec = jobs.get("j1").unwrap();
        assert_eq!(rec.status, pb::JobStatus::Failed);
        assert!(rec.error.as_deref().unwrap().contains("prepare_timeout"));
    }

    #[test]
    fn does_not_touch_already_terminal_jobs() {
        let state = test_state(Duration::from_millis(10), Duration::from_secs(1));
        let mut record = JobRecord::new(
            "j1".into(),
            redacted_spec(),
            None,
            PathBuf::from("/mnt/examplefs"),
            1,
            0,
        );
        record.status = pb::JobStatus::Completed;
        state.jobs.lock().unwrap().insert("j1".into(), record);

        tick_once(&state);

        let jobs = state.jobs.lock().unwrap();
        assert_eq!(jobs.get("j1").unwrap().status, pb::JobStatus::Completed);
    }

    #[test]
    fn does_not_fail_when_within_threshold() {
        let state = test_state(Duration::from_secs(60), Duration::from_secs(60));
        install_running_job(&state, "j1", &["w1"], Duration::from_millis(50));
        tick_once(&state);
        let jobs = state.jobs.lock().unwrap();
        assert_eq!(jobs.get("j1").unwrap().status, pb::JobStatus::Running);
    }
}

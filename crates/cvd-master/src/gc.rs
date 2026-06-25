//! 终态 job GC（spec §2 / §5.1）。
//!
//! 进入终态（COMPLETED / FAILED / CANCELLED）后保留 `job_retention_secs` 时间，
//! 超过则从 `MasterState.jobs` 删除。判定基准：[`JobRecord::terminal_at_ms`]，
//! 由 [`crate::aggregate::aggregate_into_record`] 在终态转换瞬间设置；若该字段为空
//! （理论上不会发生），fall back 到 `created_at_ms`。
//!
//! Subscriber 在状态进终态时已 `finalize_stream`，删除 record 不影响 watcher；
//! 但删除后 `WatchJob` / `QueryJob` 会返回 NotFound，新的 `FetchFileBatch` /
//! `ReportProgress` / `ReportResult` 会以 `unknown_job=true` 返回。

use std::sync::Arc;
use std::time::Duration;

use tokio::task::JoinHandle;
use tokio::time::MissedTickBehavior;

use crate::config::SchedulerConfig;
use crate::state::{now_ms, MasterState};

/// 巡检间隔策略：取 `job_retention / 10`，下限 500ms，上限 60s。短 retention
/// 配置（测试 / demo）自动得到亚秒级 tick；生产默认 3 天 retention 得到 60s tick。
const GC_TICK_MIN: Duration = Duration::from_millis(500);
const GC_TICK_MAX: Duration = Duration::from_secs(60);

fn tick_interval(cfg: &SchedulerConfig) -> Duration {
    let candidate = cfg.job_retention / 10;
    candidate.clamp(GC_TICK_MIN, GC_TICK_MAX)
}

/// 启动 GC watcher。
pub fn spawn_watcher(state: Arc<MasterState>) -> JoinHandle<()> {
    let interval_dur = tick_interval(&state.config.scheduler);
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(interval_dur);
        ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
        ticker.tick().await; // 跳过立刻触发的首次 tick
        loop {
            ticker.tick().await;
            tick_once(&state);
        }
    })
}

/// 单次 GC 巡检。可独立调用，便于单元测试。
pub fn tick_once(state: &MasterState) {
    let retention_ms =
        i64::try_from(state.config.scheduler.job_retention.as_millis()).unwrap_or(i64::MAX);
    let now_ms = now_ms();
    let cutoff_ms = now_ms.saturating_sub(retention_ms);

    let mut to_remove: Vec<String> = Vec::new();
    {
        let jobs = state.jobs.lock().expect("jobs mutex");
        for (id, record) in jobs.iter() {
            if !record.is_terminal() {
                continue;
            }
            // 优先用 terminal_at_ms；缺失时 fall back 到 created_at_ms（保守）
            let baseline = record.terminal_at_ms.unwrap_or(record.created_at_ms);
            if baseline <= cutoff_ms {
                to_remove.push(id.clone());
            }
        }
    }

    if to_remove.is_empty() {
        return;
    }

    {
        let mut jobs = state.jobs.lock().expect("jobs mutex");
        for id in &to_remove {
            jobs.remove(id);
        }
    }
    tracing::info!(
        removed = to_remove.len(),
        retention_secs = state.config.scheduler.job_retention.as_secs(),
        "GC: dropped terminal jobs past retention"
    );
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::path::PathBuf;
    use std::sync::Arc;
    use std::time::Duration;

    use cvd_proto::cvdbench as pb;

    use crate::config::{MasterConfig, SchedulerConfig};
    use crate::state::{JobRecord, MasterState};

    fn test_state(retention: Duration) -> Arc<MasterState> {
        let mut filesystems = HashMap::new();
        filesystems.insert("examplefs".into(), PathBuf::from("/mnt/examplefs"));
        let cfg = MasterConfig {
            listen: "127.0.0.1:0".parse().unwrap(),
            metrics_listen: None,
            scheduler: SchedulerConfig {
                job_retention: retention,
                ..SchedulerConfig::default()
            },
            filesystems,
        };
        Arc::new(MasterState::new(cfg))
    }

    fn install_terminal(state: &MasterState, id: &str, status: pb::JobStatus, terminal_at_ms: i64) {
        let mut record = JobRecord::new(
            id.into(),
            pb::BenchSpec::default(),
            None,
            PathBuf::from("/mnt/examplefs"),
            1,
            terminal_at_ms - 1_000,
        );
        record.status = status;
        record.terminal_at_ms = Some(terminal_at_ms);
        state.jobs.lock().unwrap().insert(id.into(), record);
    }

    #[test]
    fn removes_jobs_past_retention() {
        let state = test_state(Duration::from_millis(100));
        let now = crate::state::now_ms();
        // terminal 1s 前；retention 100ms → 应该删
        install_terminal(&state, "old", pb::JobStatus::Completed, now - 1_000);
        // terminal 10ms 前；retention 100ms → 应该保留
        install_terminal(&state, "recent", pb::JobStatus::Completed, now - 10);
        super::tick_once(&state);
        let jobs = state.jobs.lock().unwrap();
        assert!(!jobs.contains_key("old"));
        assert!(jobs.contains_key("recent"));
    }

    #[test]
    fn skips_non_terminal_jobs() {
        let state = test_state(Duration::from_millis(0));
        let now = crate::state::now_ms();
        let mut record = JobRecord::new(
            "running".into(),
            pb::BenchSpec::default(),
            None,
            PathBuf::from("/mnt/examplefs"),
            1,
            now - 86_400_000, // 1 天前
        );
        record.status = pb::JobStatus::Running;
        state.jobs.lock().unwrap().insert("running".into(), record);
        super::tick_once(&state);
        let jobs = state.jobs.lock().unwrap();
        assert!(jobs.contains_key("running"));
    }

    #[test]
    fn falls_back_to_created_at_when_terminal_at_missing() {
        let state = test_state(Duration::from_millis(100));
        let now = crate::state::now_ms();
        let mut record = JobRecord::new(
            "old".into(),
            pb::BenchSpec::default(),
            None,
            PathBuf::from("/mnt/examplefs"),
            1,
            now - 1_000, // 1s 前 created
        );
        record.status = pb::JobStatus::Completed;
        record.terminal_at_ms = None; // 缺失字段
        state.jobs.lock().unwrap().insert("old".into(), record);
        super::tick_once(&state);
        let jobs = state.jobs.lock().unwrap();
        assert!(!jobs.contains_key("old"));
    }
}

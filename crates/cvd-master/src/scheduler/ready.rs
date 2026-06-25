//! ReportReady 屏障：等所有 `run_workers` ready 后下发统一 `start_at_ms`；
//! `prepare_timeout` 到期或任一 ready(error) 即转 FAILED（spec §5.3 / §5.5）。

use cvd_proto::cvdbench as pb;
use tonic::{Response, Status};

use crate::aggregate;
use crate::events;
use crate::state::{now_ms, MasterState};

pub fn report_ready(
    state: &MasterState,
    req: pb::ReportReadyRequest,
) -> Result<Response<pb::ReportReadyResponse>, Status> {
    let mut jobs = state.jobs.lock().expect("jobs mutex");
    let Some(record) = jobs.get_mut(&req.job_id) else {
        return Ok(Response::new(unknown()));
    };

    // CANCELLED：让 worker 收到 cancelled=true，干净退出。
    if record.status == pb::JobStatus::Cancelled {
        return Ok(Response::new(pb::ReportReadyResponse {
            cancelled: true,
            unknown_job: false,
            start_at_ms: 0,
            master_now_ms: now_ms(),
        }));
    }
    // 终态（COMPLETED/FAILED）：worker 应放弃。
    if record.is_terminal() {
        return Ok(Response::new(unknown()));
    }
    // worker 不在 assignments 内：迟到 / unknown_job。
    if !record.worker_assignments.contains_key(&req.worker_id) {
        return Ok(Response::new(unknown()));
    }

    record.touch(&req.worker_id);

    // 本地准备失败 → job FAILED，并把错误原因登记。
    if let Some(reason) = req.error.as_ref() {
        // ReportReady(error) 仅在 PREPARING 期间能驱动 PREPARING→FAILED；其它状态
        // 下若收到 error，要么 job 已经 RUNNING 了（worker preflight 应当成功），
        // 要么已经是终态。两种情况都不再翻动状态，直接返回 unknown_job。
        if record.status != pb::JobStatus::Preparing {
            return Ok(Response::new(unknown()));
        }
        record.status = pb::JobStatus::Failed;
        record.error = Some(format!(
            "worker {} reported ready failure: {}",
            req.worker_id, reason
        ));
        tracing::warn!(job_id = %req.job_id, worker_id = %req.worker_id, %reason,
            "ReportReady error → job FAILED");
        // FAILED 也跑一次聚合（worker_results 此刻一般为空，但保持终态一致）
        aggregate::aggregate_into_record(record);
        record.cleanup_on_terminal();
        events::emit_for_job(state, record, pb::EventKind::StatusChange);
        // 终态时把 PREPARING 阶段的 worker 占位释放，避免 worker daemon 卡死在
        // 旧 job_id 上不能去 fetch 新 job（spec §6.4 worker 单 job 串行）
        let assigned: Vec<String> = record.worker_assignments.keys().cloned().collect();
        drop(jobs);
        {
            let mut active = state.worker_active_jobs.lock().expect("active mutex");
            for w in &assigned {
                if active.get(w) == Some(&req.job_id) {
                    active.remove(w);
                }
            }
        }
        events::finalize_stream(state, &req.job_id);
        return Ok(Response::new(pb::ReportReadyResponse {
            cancelled: false,
            unknown_job: false,
            start_at_ms: 0,
            master_now_ms: now_ms(),
        }));
    }

    // 屏障状态推进：只要 worker 已经在 worker_assignments 里就把 ReportReady 计入
    // ready_workers；status 在最后一个 FetchJob 才翻 PREPARING，先到的 ReportReady
    // 不应被丢弃（spec §5.3）。屏障开门条件仍要求 status==PREPARING && ready==run。
    let just_started =
        if record.status == pb::JobStatus::Preparing || record.status == pb::JobStatus::Pending {
            record.ready_workers.insert(req.worker_id.clone());
            if record.status == pb::JobStatus::Preparing
                && record.start_at_ms == 0
                && !record.run_workers.is_empty()
                && record.ready_workers.len() == record.run_workers.len()
            {
                let delay_ms =
                    i64::try_from(state.config.scheduler.start_delay.as_millis()).unwrap_or(5_000);
                record.start_at_ms = now_ms() + delay_ms;
                record.status = pb::JobStatus::Running;
                tracing::info!(job_id = %req.job_id,
                run_workers = record.run_workers.len(),
                start_at_ms = record.start_at_ms,
                "all workers ready, job RUNNING");
                true
            } else {
                false
            }
        } else {
            false
        };

    let response = pb::ReportReadyResponse {
        cancelled: false,
        unknown_job: false,
        start_at_ms: record.start_at_ms,
        master_now_ms: now_ms(),
    };
    if just_started {
        events::emit_for_job(state, record, pb::EventKind::StatusChange);
    }

    Ok(Response::new(response))
}

fn unknown() -> pb::ReportReadyResponse {
    pb::ReportReadyResponse {
        cancelled: false,
        unknown_job: true,
        start_at_ms: 0,
        master_now_ms: now_ms(),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use crate::config::{MasterConfig, SchedulerConfig};
    use crate::scheduler::fetch_job::fetch_job;
    use crate::service::cli_rpc;
    use cvd_proto::cvdbench as pb;

    use super::*;

    fn test_state() -> Arc<MasterState> {
        let mut filesystems = HashMap::new();
        filesystems.insert("examplefs".into(), std::path::PathBuf::from("/mnt/examplefs"));
        let cfg = MasterConfig {
            listen: "127.0.0.1:0".parse().unwrap(),
            metrics_listen: None,
            scheduler: SchedulerConfig::default(),
            filesystems,
        };
        Arc::new(MasterState::new(cfg))
    }

    fn good_proto_spec(target_workers: i32) -> pb::BenchSpec {
        pb::BenchSpec {
            fs_name: "examplefs".into(),
            io_mode: "seq".into(),
            io_aligned: true,
            direct_io: false,
            block_size: "1Mi".into(),
            duration: "1h".into(),
            warmup: String::new(),
            target_workers,
            // write spec：测试只关心 ready 屏障行为，不需要 manifest 文件
            read: None,
            write: Some(pb::WriteConfig {
                concurrency: 1,
                dir: "bench/write".into(),
                file_size: "4Ki".into(),
                file_size_range: None,
                fsync: false,
                cleanup: false,
                think_time: String::new(),
                rate_limit: String::new(),
                verify_after_write: false,
            }),
            metadata: None,
        }
    }

    fn create_and_fill_slots(state: &Arc<MasterState>, workers: &[&str]) -> String {
        let job_id = cli_rpc::create_job(
            state,
            pb::CreateJobRequest {
                spec: Some(good_proto_spec(workers.len() as i32)),
            },
        )
        .unwrap()
        .into_inner()
        .job_id;
        for w in workers {
            fetch_job(
                state,
                pb::FetchJobRequest {
                    worker_id: (*w).into(),
                },
            )
            .unwrap();
        }
        job_id
    }

    fn ready(state: &MasterState, job_id: &str, worker: &str) -> pb::ReportReadyResponse {
        report_ready(
            state,
            pb::ReportReadyRequest {
                job_id: job_id.into(),
                worker_id: worker.into(),
                error: None,
            },
        )
        .unwrap()
        .into_inner()
    }

    #[test]
    fn ready_advances_to_running_when_all_report() {
        let state = test_state();
        let workers = ["host1-1-aaaaaaaa", "host1-2-bbbbbbbb"];
        let job_id = create_and_fill_slots(&state, &workers);

        let r1 = ready(&state, &job_id, workers[0]);
        assert!(!r1.unknown_job);
        assert_eq!(r1.start_at_ms, 0, "屏障未开");

        let r2 = ready(&state, &job_id, workers[1]);
        assert!(r2.start_at_ms > 0, "全员 ready → 起跑时间下发");

        let jobs = state.jobs.lock().unwrap();
        assert_eq!(jobs.get(&job_id).unwrap().status, pb::JobStatus::Running);
    }

    #[test]
    fn error_in_ready_makes_job_fail() {
        let state = test_state();
        let workers = ["host1-1-aaaaaaaa", "host1-2-bbbbbbbb"];
        let job_id = create_and_fill_slots(&state, &workers);

        let resp = report_ready(
            &state,
            pb::ReportReadyRequest {
                job_id: job_id.clone(),
                worker_id: workers[0].into(),
                error: Some("alignment unsupported".into()),
            },
        )
        .unwrap()
        .into_inner();
        assert_eq!(resp.start_at_ms, 0);

        let jobs = state.jobs.lock().unwrap();
        let rec = jobs.get(&job_id).unwrap();
        assert_eq!(rec.status, pb::JobStatus::Failed);
        assert!(rec
            .error
            .as_deref()
            .unwrap()
            .contains("alignment unsupported"));
    }

    #[test]
    fn ready_from_non_run_worker_is_unknown() {
        let state = test_state();
        let workers = ["host1-1-aaaaaaaa", "host1-2-bbbbbbbb"];
        let job_id = create_and_fill_slots(&state, &workers);

        let resp = ready(&state, &job_id, "stranger-9-99999999");
        assert!(resp.unknown_job);
    }

    #[test]
    fn ready_after_cancel_returns_cancelled() {
        let state = test_state();
        let workers = ["host1-1-aaaaaaaa", "host1-2-bbbbbbbb"];
        let job_id = create_and_fill_slots(&state, &workers);

        cli_rpc::delete_job(
            &state,
            pb::DeleteJobRequest {
                job_id: job_id.clone(),
            },
        )
        .unwrap();

        let resp = ready(&state, &job_id, workers[0]);
        assert!(resp.cancelled);
        assert!(!resp.unknown_job);
    }

    #[test]
    fn second_ready_after_running_does_not_change_start_at() {
        let state = test_state();
        let workers = ["host1-1-aaaaaaaa", "host1-2-bbbbbbbb"];
        let job_id = create_and_fill_slots(&state, &workers);

        let r1 = ready(&state, &job_id, workers[0]);
        let r2 = ready(&state, &job_id, workers[1]);
        let r3 = ready(&state, &job_id, workers[1]); // 重复
        assert_eq!(r3.start_at_ms, r2.start_at_ms);
        let _ = r1;
    }

    /// 回归（spec §5.3 fix M2）：worker A 已 FetchJob 入队 worker_assignments 但 status
    /// 还是 PENDING（其它 slot 未占满），此时 ReportReady 不能被静默丢弃。
    #[test]
    fn ready_during_pending_is_recorded() {
        let state = test_state();
        // target_workers=2，但只有 1 个 worker 先 FetchJob 入队 → status 仍 PENDING
        let job_id = cli_rpc::create_job(
            &state,
            pb::CreateJobRequest {
                spec: Some(good_proto_spec(2)),
            },
        )
        .unwrap()
        .into_inner()
        .job_id;
        fetch_job(
            &state,
            pb::FetchJobRequest {
                worker_id: "host1-1-aaaaaaaa".into(),
            },
        )
        .unwrap();
        // 此时 status==PENDING；ReportReady 不应当丢弃
        let r = ready(&state, &job_id, "host1-1-aaaaaaaa");
        assert!(!r.unknown_job);
        assert_eq!(r.start_at_ms, 0); // 屏障未开
                                      // ready_workers 应已包含本 worker
        let jobs = state.jobs.lock().unwrap();
        let rec = jobs.get(&job_id).unwrap();
        assert!(rec.ready_workers.contains("host1-1-aaaaaaaa"));
        assert_eq!(rec.status, pb::JobStatus::Pending);
    }
}

//! FetchJob：FIFO 队首占 slot；`worker_active_jobs` 幂等重放（spec §5.4）。
//!
//! 锁顺序约定：jobs → pending_queue → worker_active_jobs。所有调度路径必须按
//! 这个顺序加锁，避免与 [`crate::service::cli_rpc`] 形成跨 RPC 死锁。

use std::sync::Arc;

use cvd_proto::cvdbench as pb;
use tonic::{Response, Status};

use crate::events;
use crate::manifest;
use crate::state::{now_ms, pending, MasterState};

/// `FetchJob` RPC 实现。
///
/// 三种返回路径：
/// 1. **幂等重放**：worker_active_jobs 已记录该 worker → 把原 job 再发一次；
/// 2. **新分配**：pending_queue 队首 job 占 slot；填满则转 PREPARING 并出队；
/// 3. **空闲**：所有 PENDING job 已停止报名（被 cancel 或恰好为空）→ 返回空。
pub fn fetch_job(
    state: &Arc<MasterState>,
    req: pb::FetchJobRequest,
) -> Result<Response<pb::FetchJobResponse>, Status> {
    let worker_id = req.worker_id;
    cvd_common::id::validate(&worker_id)
        .map_err(|e| Status::invalid_argument(format!("worker_id: {e}")))?;

    let mut jobs = state.jobs.lock().expect("jobs mutex");
    let mut queue = state.pending_queue.lock().expect("pending mutex");
    let mut active = state.worker_active_jobs.lock().expect("active mutex");

    // 1) 幂等重放
    if let Some(active_job_id) = active.get(&worker_id).cloned() {
        if let Some(record) = jobs.get_mut(&active_job_id) {
            if !record.is_terminal() {
                if let Some(assignment) = record.worker_assignments.get(&worker_id).cloned() {
                    record.touch(&worker_id);
                    return Ok(Response::new(pb::FetchJobResponse {
                        job_id: Some(active_job_id),
                        spec: Some(record.spec_redacted.clone()),
                        mount_point: Some(record.mount_point.to_string_lossy().into_owned()),
                        worker_index: Some(i32::try_from(assignment.worker_index).unwrap_or(0)),
                        s3_credentials: record.credentials.clone(),
                        master_now_ms: now_ms(),
                    }));
                }
            }
        }
        // 关联 job 不存在 / 已终态 / 没有 assignment（理论上不应发生）→ 清理重放表
        active.remove(&worker_id);
    }

    // 2) 取 pending 队首；防御性地跳过状态不符或已 cancel 的项。
    while let Some(top_id) = pending::peek_front(&queue).cloned() {
        let Some(record) = jobs.get_mut(&top_id) else {
            queue.pop_front();
            continue;
        };
        if record.status != pb::JobStatus::Pending {
            queue.pop_front();
            continue;
        }

        let assignment = match record.try_assign(&worker_id) {
            Some(a) => a,
            // slot 已满但状态仍是 PENDING 不应发生；防御性地推进
            None => {
                record.enter_preparing();
                queue.pop_front();
                continue;
            }
        };
        active.insert(worker_id.clone(), top_id.clone());

        let response = pb::FetchJobResponse {
            job_id: Some(top_id.clone()),
            spec: Some(record.spec_redacted.clone()),
            mount_point: Some(record.mount_point.to_string_lossy().into_owned()),
            worker_index: Some(i32::try_from(assignment.worker_index).unwrap_or(0)),
            s3_credentials: record.credentials.clone(),
            master_now_ms: now_ms(),
        };

        // 3) slot 占满 → PREPARING + 出队 + 启动 manifest reader（read job）+ 广播 STATUS_CHANGE
        if record.slots_remaining() == 0 {
            record.enter_preparing();
            tracing::info!(job_id = %top_id, run_workers = record.run_workers.len(),
                "job slots filled, entering PREPARING");
            manifest::start_for_file_queue_job(state, record);
            events::emit_for_job(state, record, pb::EventKind::StatusChange);
            pending::pop_if_front(&mut queue, &top_id);
        }

        return Ok(Response::new(response));
    }

    // 4) 没有可分配 → 空 job 让 worker 继续轮询
    Ok(Response::new(pb::FetchJobResponse {
        job_id: None,
        spec: None,
        mount_point: None,
        worker_index: None,
        s3_credentials: None,
        master_now_ms: now_ms(),
    }))
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use crate::config::{MasterConfig, SchedulerConfig};
    use crate::service::cli_rpc;
    use cvd_proto::cvdbench as pb;

    use super::*;

    fn test_state(target_workers: i32) -> Arc<MasterState> {
        let mut filesystems = HashMap::new();
        filesystems.insert("examplefs".into(), std::path::PathBuf::from("/mnt/examplefs"));
        let cfg = MasterConfig {
            listen: "127.0.0.1:0".parse().unwrap(),
            metrics_listen: None,
            scheduler: SchedulerConfig::default(),
            filesystems,
        };
        let state = Arc::new(MasterState::new(cfg));

        let _ = target_workers;
        state
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
            // 用 write spec 避免触发 master 的 manifest 文件存在性校验，
            // 这些测试只关心 slot 调度，不关心 workload 类型。
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

    fn create(state: &Arc<MasterState>, target_workers: i32) -> String {
        cli_rpc::create_job(
            state,
            pb::CreateJobRequest {
                spec: Some(good_proto_spec(target_workers)),
            },
        )
        .unwrap()
        .into_inner()
        .job_id
    }

    fn fetch(state: &Arc<MasterState>, worker_id: &str) -> pb::FetchJobResponse {
        fetch_job(
            state,
            pb::FetchJobRequest {
                worker_id: worker_id.into(),
            },
        )
        .unwrap()
        .into_inner()
    }

    #[test]
    fn fills_slots_in_fifo_and_transitions_to_preparing() {
        let state = test_state(3);
        let job_id = create(&state, 3);

        let r1 = fetch(&state, "host1-1-aaaaaaaa");
        assert_eq!(r1.job_id.as_deref(), Some(job_id.as_str()));
        assert_eq!(r1.worker_index, Some(0));
        let r2 = fetch(&state, "host1-2-bbbbbbbb");
        assert_eq!(r2.worker_index, Some(1));
        let r3 = fetch(&state, "host1-3-cccccccc");
        assert_eq!(r3.worker_index, Some(2));

        // slot 满 → PREPARING + 队列空
        let jobs = state.jobs.lock().unwrap();
        let rec = jobs.get(&job_id).unwrap();
        assert_eq!(rec.status, pb::JobStatus::Preparing);
        assert_eq!(rec.run_workers.len(), 3);
        drop(jobs);
        assert!(state.pending_queue.lock().unwrap().is_empty());
    }

    #[test]
    fn idempotent_replay_returns_same_job_and_index() {
        let state = test_state(3);
        let job_id = create(&state, 3);
        let r1 = fetch(&state, "host1-1-aaaaaaaa");
        let r1b = fetch(&state, "host1-1-aaaaaaaa"); // 同一 worker 重试
        assert_eq!(r1.job_id, r1b.job_id);
        assert_eq!(r1.worker_index, r1b.worker_index);
        // 第二次不应该再扣 slot
        let _ = job_id;
        let jobs = state.jobs.lock().unwrap();
        let rec = jobs.values().next().unwrap();
        assert_eq!(rec.slots_filled(), 1);
    }

    #[test]
    fn empty_response_when_no_pending() {
        let state = test_state(1);
        let resp = fetch(&state, "host1-1-aaaaaaaa");
        assert!(resp.job_id.is_none());
        assert!(resp.master_now_ms > 0);
    }

    #[test]
    fn second_job_picks_up_after_first_filled() {
        let state = test_state(1);
        let job_a = create(&state, 1);
        let job_b = create(&state, 1);

        let ra = fetch(&state, "host1-1-aaaaaaaa");
        assert_eq!(ra.job_id.as_deref(), Some(job_a.as_str()));

        let rb = fetch(&state, "host2-2-bbbbbbbb");
        assert_eq!(rb.job_id.as_deref(), Some(job_b.as_str()));
    }

    #[test]
    fn rejects_invalid_worker_id() {
        let state = test_state(1);
        let err = fetch_job(
            &state,
            pb::FetchJobRequest {
                worker_id: "bad worker id".into(),
            },
        )
        .unwrap_err();
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
    }
}

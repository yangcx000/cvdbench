//! CLI 端 RPC 实现：CreateJob / WatchJob / QueryJob / DeleteJob / ListJobs。
//!
//! v0 状态机最小化版本：仅支持 PENDING → CANCELLED；slot 占用、PREPARING、
//! RUNNING、终态聚合等留给后续路线分阶段实现。

use cvd_common::spec::{
    extract_credentials, redact_in_place,
    validate::{validate, ValidationContext},
    BenchSpec, ReadSource,
};
use cvd_proto::cvdbench as pb;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Response, Status};
use uuid::Uuid;

use crate::aggregate;
use crate::events;
use crate::state::{now_ms, pending, JobRecord, MasterState};

/// CLI: `CreateJob`。
///
/// 流程：
/// 1. 解析 + 校验 BenchSpec（cvd-common::spec）；
/// 2. master 配置层校验：`fs_name` 必须在 `[[filesystems]]` 中；
/// 3. 抽出明文凭据并脱敏 spec；
/// 4. 入 `jobs` map + `pending_queue` 队尾，状态 PENDING；
/// 5. 返回 `job_id`。
pub fn create_job(
    state: &MasterState,
    req: pb::CreateJobRequest,
) -> Result<Response<pb::CreateJobResponse>, Status> {
    let proto_spec = req
        .spec
        .ok_or_else(|| Status::invalid_argument("CreateJobRequest.spec is required"))?;

    // 业务侧解析（拿一份强类型副本做校验，proto_spec 本身随后脱敏入库）
    let business_spec = BenchSpec::try_from_proto(proto_spec.clone())
        .map_err(|e| Status::invalid_argument(format!("spec parse: {e}")))?;

    validate(&business_spec, &ValidationContext::default())
        .map_err(|report| Status::failed_precondition(format!("spec validate: {report}")))?;

    if !state
        .config
        .filesystems
        .contains_key(&business_spec.fs_name)
    {
        return Err(Status::failed_precondition(format!(
            "fs_name {:?} not registered in cvd-master.toml",
            business_spec.fs_name
        )));
    }
    let mount_point = state.config.filesystems[&business_spec.fs_name].clone();

    // Spec §5.7：CreateJob 同步校验 file_manifest / dir_manifest 路径存在且可读。
    if let Some(read) = business_spec.read.as_ref() {
        match &read.source {
            ReadSource::FileManifest { path } => {
                ensure_readable_manifest("file_manifest", path)?;
            }
            ReadSource::DirManifest { path } => {
                ensure_readable_manifest("dir_manifest", path)?;
            }
        }
    }
    if let Some(meta) = business_spec.metadata.as_ref() {
        if let Some(path) = &meta.dir_manifest {
            ensure_readable_manifest("metadata.dir_manifest", path)?;
        }
    }

    let credentials = extract_credentials(&proto_spec);
    let mut spec_for_storage = proto_spec;
    redact_in_place(&mut spec_for_storage);
    // 与 cvd-common 业务侧一致：target_workers <= 0 在存储态也归一化为 1。
    if spec_for_storage.target_workers <= 0 {
        spec_for_storage.target_workers = 1;
    }

    let job_id = Uuid::new_v4().to_string();
    let record = JobRecord::new(
        job_id.clone(),
        spec_for_storage,
        credentials,
        mount_point,
        business_spec.target_workers,
        now_ms(),
    );

    {
        let mut jobs = state.jobs.lock().expect("jobs mutex");
        jobs.insert(job_id.clone(), record);
        // 持锁内 emit，保证 PENDING 事件 seq 早于后续任何 emit
        let r = jobs.get(&job_id).expect("just inserted");
        events::emit_for_job(state, r, pb::EventKind::StatusChange);
    }
    {
        let mut q = state.pending_queue.lock().expect("pending mutex");
        pending::push_back(&mut q, job_id.clone());
    }

    tracing::info!(%job_id, fs_name = %business_spec.fs_name,
        target_workers = business_spec.target_workers, "job created");
    Ok(Response::new(pb::CreateJobResponse { job_id }))
}

fn ensure_readable_manifest(kind: &str, path: &str) -> Result<(), Status> {
    let p = std::path::Path::new(path);
    if !p.is_file() {
        return Err(Status::failed_precondition(format!(
            "{kind} {path:?} does not exist or is not a regular file"
        )));
    }
    std::fs::File::open(p).map_err(|e| {
        Status::failed_precondition(format!("{kind} {path:?} is not readable: {e}"))
    })?;
    Ok(())
}

/// CLI: `WatchJob`。
///
/// 流程：
/// 1. 在 `jobs` 锁内构造一条 `STATUS_CHANGE` 快照事件；
/// 2. 终态 job：单条 channel 发出快照后立即关流；
/// 3. 非终态：调用 `subscribe_with_snapshot` 注册 sender 并把快照排在首位，
///    后续 emit 通过同一 channel 按 `seq` 单调下发；终态时
///    `events::finalize_stream` drop 全部 sender 让 stream 自然结束。
pub async fn watch_job(
    state: &MasterState,
    req: pb::WatchJobRequest,
) -> Result<Response<ReceiverStream<Result<pb::JobEvent, Status>>>, Status> {
    let jobs = state.jobs.lock().expect("jobs mutex");
    let record = jobs
        .get(&req.job_id)
        .ok_or_else(|| Status::not_found(format!("job {:?} not found", req.job_id)))?;
    let snapshot = events::build_event(state, record, pb::EventKind::StatusChange);
    let is_terminal = record.is_terminal();

    let rx = if is_terminal {
        let (tx, rx) = tokio::sync::mpsc::channel(1);
        let _ = tx.try_send(Ok(snapshot));
        drop(tx);
        rx
    } else {
        state
            .subscribers
            .subscribe_with_snapshot(&req.job_id, snapshot)
    };
    drop(jobs);

    Ok(Response::new(ReceiverStream::new(rx)))
}

/// CLI: `QueryJob`。
pub fn query_job(
    state: &MasterState,
    req: pb::QueryJobRequest,
) -> Result<Response<pb::QueryJobResponse>, Status> {
    let jobs = state.jobs.lock().expect("jobs mutex");
    let record = jobs
        .get(&req.job_id)
        .ok_or_else(|| Status::not_found(format!("job {:?} not found", req.job_id)))?;
    // worker_results 来自 record.worker_results；CLI summary / aggregate 都要用。
    let worker_results: Vec<pb::WorkerResult> = record.worker_results.values().cloned().collect();
    let (dirs_scanned, files_scanned, scan_duration_ms) = record.manifest_scan_stats.snapshot();
    Ok(Response::new(pb::QueryJobResponse {
        job: Some(record.to_pb_job()),
        worker_results,
        aggregated: record.aggregated.clone(),
        error: record.error.clone(),
        run_workers: i32::try_from(record.run_worker_count()).unwrap_or(i32::MAX),
        dirs_scanned,
        files_scanned,
        scan_duration_ms,
    }))
}

/// CLI: `DeleteJob` —— 只在非终态生效。
///
/// 副作用：从 pending_queue 移除（PENDING）、清理 worker_active_jobs（PREPARING/RUNNING），
/// 让仍在运行的 worker 通过下一次 RPC 响应中的 `cancelled=true` 感知（spec §5.5）。
pub fn delete_job(
    state: &MasterState,
    req: pb::DeleteJobRequest,
) -> Result<Response<pb::DeleteJobResponse>, Status> {
    let prev_status;
    let active_workers: Vec<String>;
    {
        let mut jobs = state.jobs.lock().expect("jobs mutex");
        let record = jobs
            .get_mut(&req.job_id)
            .ok_or_else(|| Status::not_found(format!("job {:?} not found", req.job_id)))?;
        if record.is_terminal() {
            return Ok(Response::new(pb::DeleteJobResponse {}));
        }

        prev_status = record.status;
        record.status = pb::JobStatus::Cancelled;
        active_workers = record.worker_assignments.keys().cloned().collect();

        // CANCELLED 也聚合已收集的 results（可能为空）+ 终态清理（关 manifest 队列等）
        aggregate::aggregate_into_record(record);
        record.cleanup_on_terminal();
        // 持锁内 emit CANCELLED 终态事件
        events::emit_for_job(state, record, pb::EventKind::StatusChange);
    }
    // 终态：关闭所有订阅
    events::finalize_stream(state, &req.job_id);

    if prev_status == pb::JobStatus::Pending {
        let mut q = state.pending_queue.lock().expect("pending mutex");
        pending::remove(&mut q, &req.job_id);
    }
    if !active_workers.is_empty() {
        let mut active = state.worker_active_jobs.lock().expect("active mutex");
        for w in &active_workers {
            if active.get(w) == Some(&req.job_id) {
                active.remove(w);
            }
        }
    }

    tracing::info!(job_id = %req.job_id, ?prev_status, "job cancelled by CLI");
    Ok(Response::new(pb::DeleteJobResponse {}))
}

/// CLI: `ListJobs`。Spec §4 规则：`limit=0` → server 默认 100；上限 1000。
pub fn list_jobs(
    state: &MasterState,
    req: pb::ListJobsRequest,
) -> Result<Response<pb::ListJobsResponse>, Status> {
    const DEFAULT_LIMIT: i32 = 100;
    const MAX_LIMIT: i32 = 1_000;
    let limit = match req.limit {
        n if n <= 0 => DEFAULT_LIMIT,
        n if n > MAX_LIMIT => MAX_LIMIT,
        n => n,
    } as usize;

    let filter = req.status_filter;
    let jobs = state.jobs.lock().expect("jobs mutex");
    let mut items: Vec<pb::Job> = jobs
        .values()
        .filter(|r| filter.map_or(true, |f| f == i32::from(r.status)))
        .map(JobRecord::to_pb_job)
        .collect();
    drop(jobs);
    // 按创建时间倒序，便于 CLI 显示最新的 job 在前。
    items.sort_by_key(|j| std::cmp::Reverse(j.created_at));
    items.truncate(limit);
    Ok(Response::new(pb::ListJobsResponse { jobs: items }))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::config::{MasterConfig, SchedulerConfig};
    use cvd_proto::cvdbench as pb;

    use super::*;

    fn test_state() -> Arc<MasterState> {
        let mut filesystems = std::collections::HashMap::new();
        filesystems.insert("examplefs".into(), std::path::PathBuf::from("/mnt/examplefs"));
        let cfg = MasterConfig {
            listen: "127.0.0.1:0".parse().unwrap(),
            metrics_listen: None,
            scheduler: SchedulerConfig::default(),
            filesystems,
        };
        Arc::new(MasterState::new(cfg))
    }

    fn good_proto_spec() -> pb::BenchSpec {
        pb::BenchSpec {
            fs_name: "examplefs".into(),
            io_mode: "seq".into(),
            io_aligned: true,
            direct_io: false,
            block_size: "1Mi".into(),
            duration: "1h".into(),
            warmup: "5m".into(),
            target_workers: 4,
            // 默认 fixture 用 write spec：避免触发 master 的 file_manifest 路径校验。
            // 对 read 行为有特定断言的测试在自己的 case 里覆盖 read 字段并提供
            // 真实 manifest 文件。
            read: None,
            write: Some(pb::WriteConfig {
                concurrency: 4,
                dir: "bench/write".into(),
                file_size: "4Ki".into(),
                file_size_range: None,
                fsync: false,
                cleanup: false,
                think_time: String::new(),
                rate_limit: "1GB/s".into(),
                verify_after_write: false,
            }),
            metadata: None,
        }
    }

    #[test]
    fn create_then_query_and_list() {
        let state = test_state();
        let resp = create_job(
            &state,
            pb::CreateJobRequest {
                spec: Some(good_proto_spec()),
            },
        )
        .unwrap();
        let job_id = resp.into_inner().job_id;

        let q = query_job(
            &state,
            pb::QueryJobRequest {
                job_id: job_id.clone(),
            },
        )
        .unwrap()
        .into_inner();
        let job = q.job.unwrap();
        assert_eq!(job.job_id, job_id);
        assert_eq!(job.status, pb::JobStatus::Pending as i32);

        let l = list_jobs(
            &state,
            pb::ListJobsRequest {
                status_filter: Some(pb::JobStatus::Pending.into()),
                limit: 0,
            },
        )
        .unwrap()
        .into_inner();
        assert_eq!(l.jobs.len(), 1);
        assert_eq!(l.jobs[0].job_id, job_id);
    }

    #[test]
    fn query_includes_job_level_error() {
        let state = test_state();
        let resp = create_job(
            &state,
            pb::CreateJobRequest {
                spec: Some(good_proto_spec()),
            },
        )
        .unwrap();
        let job_id = resp.into_inner().job_id;
        {
            let mut jobs = state.jobs.lock().unwrap();
            let rec = jobs.get_mut(&job_id).unwrap();
            rec.status = pb::JobStatus::Failed;
            rec.error = Some("manifest reader: bad path".into());
        }

        let q = query_job(&state, pb::QueryJobRequest { job_id })
            .unwrap()
            .into_inner();
        assert_eq!(q.error.as_deref(), Some("manifest reader: bad path"));
    }

    #[test]
    fn query_includes_master_run_worker_count() {
        let state = test_state();
        let resp = create_job(
            &state,
            pb::CreateJobRequest {
                spec: Some(good_proto_spec()),
            },
        )
        .unwrap();
        let job_id = resp.into_inner().job_id;
        {
            let mut jobs = state.jobs.lock().unwrap();
            let rec = jobs.get_mut(&job_id).unwrap();
            rec.run_workers.insert("worker-a".into());
            rec.run_workers.insert("worker-b".into());
        }

        let q = query_job(&state, pb::QueryJobRequest { job_id })
            .unwrap()
            .into_inner();
        assert_eq!(q.worker_results.len(), 0);
        assert_eq!(q.run_workers, 2);
    }

    #[test]
    fn create_rejects_unknown_fs_name() {
        let state = test_state();
        let mut s = good_proto_spec();
        s.fs_name = "nonexistent".into();
        let err = create_job(&state, pb::CreateJobRequest { spec: Some(s) }).unwrap_err();
        assert_eq!(err.code(), tonic::Code::FailedPrecondition);
        assert!(err.message().contains("nonexistent"));
    }

    #[test]
    fn create_rejects_invalid_spec() {
        let state = test_state();
        let mut s = good_proto_spec();
        s.duration = "0s".into();
        let err = create_job(&state, pb::CreateJobRequest { spec: Some(s) }).unwrap_err();
        assert_eq!(err.code(), tonic::Code::FailedPrecondition);
    }

    #[test]
    fn create_redacts_credentials_in_storage() {
        let state = test_state();
        // 此测试明确要 read+s3 凭据，需要真实存在的 manifest 文件
        let mf = tempfile::NamedTempFile::new().unwrap();
        let mut s = good_proto_spec();
        s.write = None;
        s.read = Some(pb::ReadConfig {
            concurrency: 4,
            file_manifest: mf.path().to_string_lossy().into_owned(),
            dir_manifest: String::new(),
            think_time: String::new(),
            rate_limit: "1GB/s".into(),
            s3_consistency_check: Some(pb::ConsistencyConfig {
                bucket_name: "b".into(),
                bucket_url: "http://s3".into(),
                access_key: "AK".into(),
                secret_key: "SK".into(),
                region: "us-east-1".into(),
                prefix: String::new(),
                session_token: "TOK".into(),
            }),
            loop_files: false,
        });
        let resp = create_job(&state, pb::CreateJobRequest { spec: Some(s) }).unwrap();
        let job_id = resp.into_inner().job_id;

        // 存储态 spec 必须已脱敏；明文凭据另存于 record.credentials
        let jobs = state.jobs.lock().unwrap();
        let rec = jobs.get(&job_id).unwrap();
        let stored = rec
            .spec_redacted
            .read
            .as_ref()
            .unwrap()
            .s3_consistency_check
            .as_ref()
            .unwrap();
        assert_eq!(stored.access_key, "***");
        assert_eq!(stored.secret_key, "***");
        assert_eq!(stored.session_token, "***");
        let creds = rec.credentials.as_ref().unwrap();
        assert_eq!(creds.access_key, "AK");
        assert_eq!(creds.secret_key, "SK");
        assert_eq!(creds.session_token, "TOK");
    }

    #[test]
    fn delete_marks_pending_as_cancelled() {
        let state = test_state();
        let resp = create_job(
            &state,
            pb::CreateJobRequest {
                spec: Some(good_proto_spec()),
            },
        )
        .unwrap();
        let job_id = resp.into_inner().job_id;

        delete_job(
            &state,
            pb::DeleteJobRequest {
                job_id: job_id.clone(),
            },
        )
        .unwrap();

        let jobs = state.jobs.lock().unwrap();
        assert_eq!(jobs.get(&job_id).unwrap().status, pb::JobStatus::Cancelled);
    }

    #[test]
    fn list_jobs_caps_at_max_limit() {
        let state = test_state();
        for _ in 0..5 {
            create_job(
                &state,
                pb::CreateJobRequest {
                    spec: Some(good_proto_spec()),
                },
            )
            .unwrap();
        }
        let l = list_jobs(
            &state,
            pb::ListJobsRequest {
                status_filter: None,
                limit: 2,
            },
        )
        .unwrap()
        .into_inner();
        assert_eq!(l.jobs.len(), 2);
    }
}

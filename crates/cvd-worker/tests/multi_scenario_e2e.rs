//! 多场景调度 coverage：
//! - fail-fast：一个 worker `ReportReady(error)`，其他 worker 通过 `unknown_job` 干净退出；
//! - cancel during PREPARING：worker 在屏障等待时收到 `cancelled=true` 干净 ReportResult；
//! - 多 job FIFO：jobs 按创建顺序占满 slot；下一个 job 等队首占满后才接收报名；
//! - cross-job worker 复用：同一 daemon 完成 job1 后无缝接 job2；
//! - target_workers 不足时持续 PENDING。

use std::collections::HashMap;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::time::Duration;

use cvd_master::config::{MasterConfig, SchedulerConfig};
use cvd_master::service::MasterServiceImpl;
use cvd_master::state::MasterState;
use cvd_proto::cvdbench as pb;
use cvd_proto::cvdbench::master_service_client::MasterServiceClient;
use cvd_proto::cvdbench::master_service_server::MasterServiceServer;
use cvd_worker::lifecycle;
use tokio::net::TcpListener;
use tonic::transport::Server;

async fn start_master() -> (String, tokio::task::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let local = listener.local_addr().unwrap();
    let stream = tokio_stream::wrappers::TcpListenerStream::new(listener);
    let mut filesystems = HashMap::new();
    filesystems.insert("examplefs".into(), std::path::PathBuf::from("/mnt/examplefs"));
    let cfg = MasterConfig {
        listen: local,
        metrics_listen: None,
        scheduler: SchedulerConfig {
            start_delay: Duration::from_millis(50),
            ..SchedulerConfig::default()
        },
        filesystems,
    };
    let state = Arc::new(MasterState::new(cfg));
    let service = MasterServiceImpl::new(state);
    let handle = tokio::spawn(async move {
        Server::builder()
            .add_service(MasterServiceServer::new(service))
            .serve_with_incoming(stream)
            .await
            .unwrap();
    });
    let endpoint = format!("http://{local}");
    for _ in 0..40 {
        if MasterServiceClient::connect(endpoint.clone()).await.is_ok() {
            return (endpoint, handle);
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("master not ready");
}

async fn connect(endpoint: &str) -> MasterServiceClient<tonic::transport::Channel> {
    MasterServiceClient::connect(endpoint.to_owned())
        .await
        .unwrap()
}

fn empty_manifest() -> tempfile::NamedTempFile {
    tempfile::NamedTempFile::new().unwrap()
}

fn read_spec(target_workers: i32, duration: &str, manifest_path: &str) -> pb::BenchSpec {
    pb::BenchSpec {
        fs_name: "examplefs".into(),
        io_mode: "seq".into(),
        io_aligned: true,
        direct_io: false,
        block_size: "1Mi".into(),
        duration: duration.into(),
        warmup: String::new(),
        target_workers,
        read: Some(pb::ReadConfig {
            concurrency: 1,
            file_manifest: manifest_path.to_owned(),
            dir_manifest: String::new(),
            think_time: String::new(),
            rate_limit: String::new(),
            s3_consistency_check: None,
            loop_files: false,
        }),
        write: None,
        metadata: None,
    }
}

async fn create_job(
    endpoint: &str,
    target_workers: i32,
    duration: &str,
    manifest_path: &str,
) -> String {
    connect(endpoint)
        .await
        .create_job(pb::CreateJobRequest {
            spec: Some(read_spec(target_workers, duration, manifest_path)),
        })
        .await
        .unwrap()
        .into_inner()
        .job_id
}

async fn query_status(endpoint: &str, job_id: &str) -> i32 {
    connect(endpoint)
        .await
        .query_job(pb::QueryJobRequest {
            job_id: job_id.to_owned(),
        })
        .await
        .unwrap()
        .into_inner()
        .job
        .unwrap()
        .status
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fail_fast_aborts_other_workers_via_unknown_job() {
    let (endpoint, handle) = start_master().await;
    let mf = empty_manifest();
    let job_id = create_job(&endpoint, 3, "1s", &mf.path().to_string_lossy()).await;

    let workers = ["hostf-1-aaaaaaaa", "hostf-2-bbbbbbbb", "hostf-3-cccccccc"];

    // 三个 worker 都 fetch_job，保存各自的 FetchJobResponse；
    // 第三次 fetch 触发 PREPARING 转换。
    let mut fetch_resps = Vec::new();
    for w in &workers {
        let r = connect(&endpoint)
            .await
            .fetch_job(pb::FetchJobRequest {
                worker_id: (*w).into(),
            })
            .await
            .unwrap()
            .into_inner();
        assert_eq!(r.job_id.as_deref(), Some(job_id.as_str()));
        fetch_resps.push(r);
    }

    // worker0 直接 ReportReady(error) → master 转 FAILED + finalize_stream
    connect(&endpoint)
        .await
        .report_ready(pb::ReportReadyRequest {
            job_id: job_id.clone(),
            worker_id: workers[0].into(),
            error: Some("simulated local prepare failure".into()),
        })
        .await
        .unwrap();

    // worker1/2 复用各自的 fetch_resp 进 lifecycle；wait_for_start_barrier 调
    // ReportReady → master 已 FAILED → 返 unknown_job=true → BarrierOutcome::UnknownJob
    // → run_assigned_job 立即 return（不 ReportResult）
    let mut tasks = Vec::new();
    for (i, &w) in workers[1..].iter().enumerate() {
        let endpoint = endpoint.clone();
        let w = w.to_owned();
        let fetch_resp = fetch_resps[i + 1].clone();
        tasks.push(tokio::spawn(async move {
            let mut c = connect(&endpoint).await;
            let cancelled = Arc::new(AtomicBool::new(false));
            lifecycle::run_assigned_job(&mut c, &w, fetch_resp, &cancelled).await;
        }));
    }

    // 严格 timeout：必须很快退出（看到 unknown_job 立即 abandon）
    for t in tasks {
        tokio::time::timeout(Duration::from_secs(3), t)
            .await
            .expect("worker should abandon quickly on unknown_job")
            .unwrap();
    }

    assert_eq!(
        query_status(&endpoint, &job_id).await,
        pb::JobStatus::Failed as i32
    );
    handle.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cancel_during_preparing_propagates_through_barrier() {
    let (endpoint, handle) = start_master().await;
    let mf = empty_manifest();
    let job_id = create_job(&endpoint, 2, "1s", &mf.path().to_string_lossy()).await;

    let workers = ["hostc-1-aaaaaaaa", "hostc-2-bbbbbbbb"];
    let mut tasks = Vec::new();

    for &w in &workers {
        let endpoint = endpoint.clone();
        let job_id = job_id.clone();
        let w = w.to_owned();
        tasks.push(tokio::spawn(async move {
            let mut c = connect(&endpoint).await;
            let fetch = c
                .fetch_job(pb::FetchJobRequest {
                    worker_id: w.clone(),
                })
                .await
                .unwrap()
                .into_inner();
            assert_eq!(fetch.job_id.as_deref(), Some(job_id.as_str()));
            let cancelled = Arc::new(AtomicBool::new(false));
            // worker 进入 ReportReady barrier 等 start_at_ms；只要其它 worker 也 ready 才会拿到时间
            lifecycle::run_assigned_job(&mut c, &w, fetch, &cancelled).await;
        }));
    }

    // 等 worker 进入 PREPARING + ReportReady barrier
    tokio::time::sleep(Duration::from_millis(100)).await;

    // 此时 cancel：master 转 CANCELLED；worker 下一次 ReportReady 收到 cancelled=true，
    // 走 BarrierOutcome::Cancelled 路径 ReportResult(success=true, error="cancelled")
    connect(&endpoint)
        .await
        .delete_job(pb::DeleteJobRequest {
            job_id: job_id.clone(),
        })
        .await
        .unwrap();

    for t in tasks {
        tokio::time::timeout(Duration::from_secs(3), t)
            .await
            .expect("worker should exit quickly after cancel")
            .unwrap();
    }

    assert_eq!(
        query_status(&endpoint, &job_id).await,
        pb::JobStatus::Cancelled as i32
    );
    handle.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn multi_job_fifo_assigns_in_creation_order() {
    let (endpoint, handle) = start_master().await;
    let mf = empty_manifest();
    let path = mf.path().to_string_lossy().into_owned();

    // 先创建 job_a，再创建 job_b（FIFO 顺序）
    let job_a = create_job(&endpoint, 1, "1h", &path).await;
    let job_b = create_job(&endpoint, 1, "1h", &path).await;

    // worker1 fetch → 应当拿到 job_a（队首）
    let r1 = connect(&endpoint)
        .await
        .fetch_job(pb::FetchJobRequest {
            worker_id: "hosti-1-aaaaaaaa".into(),
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(r1.job_id.as_deref(), Some(job_a.as_str()));

    // worker2 fetch → job_a 已 PREPARING（slot 满），队首切到 job_b
    let r2 = connect(&endpoint)
        .await
        .fetch_job(pb::FetchJobRequest {
            worker_id: "hosti-2-bbbbbbbb".into(),
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(r2.job_id.as_deref(), Some(job_b.as_str()));

    // worker3 fetch → 没有 PENDING 了
    let r3 = connect(&endpoint)
        .await
        .fetch_job(pb::FetchJobRequest {
            worker_id: "hosti-3-cccccccc".into(),
        })
        .await
        .unwrap()
        .into_inner();
    assert!(r3.job_id.is_none());

    handle.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn worker_can_take_consecutive_jobs() {
    let (endpoint, handle) = start_master().await;
    let mf1 = empty_manifest();
    let job_id_1 = create_job(&endpoint, 1, "100ms", &mf1.path().to_string_lossy()).await;

    let worker_id = "hostr-1-aaaaaaaa".to_owned();
    let mut c = connect(&endpoint).await;

    // 第一个 job
    let fetch1 = c
        .fetch_job(pb::FetchJobRequest {
            worker_id: worker_id.clone(),
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(fetch1.job_id.as_deref(), Some(job_id_1.as_str()));
    let cancelled1 = Arc::new(AtomicBool::new(false));
    lifecycle::run_assigned_job(&mut c, &worker_id, fetch1, &cancelled1).await;
    assert_eq!(
        query_status(&endpoint, &job_id_1).await,
        pb::JobStatus::Completed as i32
    );

    // 完成 job1 后 worker_active_jobs 已清，worker daemon 直接接 job2
    let mf2 = empty_manifest();
    let job_id_2 = create_job(&endpoint, 1, "100ms", &mf2.path().to_string_lossy()).await;
    let fetch2 = c
        .fetch_job(pb::FetchJobRequest {
            worker_id: worker_id.clone(),
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(fetch2.job_id.as_deref(), Some(job_id_2.as_str()));
    let cancelled2 = Arc::new(AtomicBool::new(false));
    lifecycle::run_assigned_job(&mut c, &worker_id, fetch2, &cancelled2).await;
    assert_eq!(
        query_status(&endpoint, &job_id_2).await,
        pb::JobStatus::Completed as i32
    );

    handle.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn job_stays_pending_until_target_workers_reached() {
    let (endpoint, handle) = start_master().await;
    let mf = empty_manifest();
    let job_id = create_job(&endpoint, 3, "1h", &mf.path().to_string_lossy()).await;

    // 2 个 worker fetch — slot 还差一个
    for w in ["hostp-1-aaaaaaaa", "hostp-2-bbbbbbbb"] {
        let r = connect(&endpoint)
            .await
            .fetch_job(pb::FetchJobRequest {
                worker_id: w.into(),
            })
            .await
            .unwrap()
            .into_inner();
        assert_eq!(r.job_id.as_deref(), Some(job_id.as_str()));
    }
    // 仍 PENDING（slots_remaining=1）
    assert_eq!(
        query_status(&endpoint, &job_id).await,
        pb::JobStatus::Pending as i32
    );

    // 第 3 个 worker fetch → slot 占满 → PREPARING
    let r = connect(&endpoint)
        .await
        .fetch_job(pb::FetchJobRequest {
            worker_id: "hostp-3-cccccccc".into(),
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(r.job_id.as_deref(), Some(job_id.as_str()));
    assert_eq!(
        query_status(&endpoint, &job_id).await,
        pb::JobStatus::Preparing as i32
    );

    handle.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancel_pending_job_removes_from_queue() {
    let (endpoint, handle) = start_master().await;
    let mf = empty_manifest();
    let path = mf.path().to_string_lossy().into_owned();

    // 创建 job_a (PENDING)，没人 fetch
    let job_a = create_job(&endpoint, 5, "1h", &path).await;
    assert_eq!(
        query_status(&endpoint, &job_a).await,
        pb::JobStatus::Pending as i32
    );

    // 创建 job_b
    let job_b = create_job(&endpoint, 1, "1h", &path).await;

    // cancel job_a
    connect(&endpoint)
        .await
        .delete_job(pb::DeleteJobRequest {
            job_id: job_a.clone(),
        })
        .await
        .unwrap();
    assert_eq!(
        query_status(&endpoint, &job_a).await,
        pb::JobStatus::Cancelled as i32
    );

    // worker fetch → job_b（job_a 被 cancel 后队首切到 job_b）
    let r = connect(&endpoint)
        .await
        .fetch_job(pb::FetchJobRequest {
            worker_id: "hostq-1-aaaaaaaa".into(),
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(r.job_id.as_deref(), Some(job_b.as_str()));

    handle.abort();
}

//! 端到端联通：起一个 in-process master，用 tonic client 走完
//! Create → Watch → Query → List → Delete → Query 的最小 CLI 流程。

use std::sync::Arc;
use std::time::Duration;

use cvd_master::config::{MasterConfig, SchedulerConfig};
use cvd_master::service::MasterServiceImpl;
use cvd_master::state::MasterState;
use cvd_proto::cvdbench as pb;
use cvd_proto::cvdbench::master_service_client::MasterServiceClient;
use cvd_proto::cvdbench::master_service_server::MasterServiceServer;
use tokio::net::TcpListener;
use tokio_stream::StreamExt;
use tonic::transport::Server;

async fn start_master() -> (
    MasterServiceClient<tonic::transport::Channel>,
    tokio::task::JoinHandle<()>,
) {
    // 让 OS 分配空闲端口，避免测试间撞港。
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let local = listener.local_addr().unwrap();
    let stream = tokio_stream::wrappers::TcpListenerStream::new(listener);

    let mut filesystems = std::collections::HashMap::new();
    filesystems.insert("examplefs".into(), std::path::PathBuf::from("/mnt/examplefs"));
    let cfg = MasterConfig {
        listen: local,
        metrics_listen: None,
        scheduler: SchedulerConfig::default(),
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

    // 等 server 真正能 accept 再连接（tonic 的异步启动有几毫秒延迟）
    let endpoint = format!("http://{local}");
    let mut last_err = None;
    for _ in 0..20 {
        match MasterServiceClient::connect(endpoint.clone()).await {
            Ok(client) => return (client, handle),
            Err(e) => {
                last_err = Some(e);
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        }
    }
    panic!("connect master failed: {last_err:?}");
}

fn good_spec() -> pb::BenchSpec {
    pb::BenchSpec {
        fs_name: "examplefs".into(),
        io_mode: "seq".into(),
        io_aligned: true,
        direct_io: false,
        block_size: "1Mi".into(),
        duration: "1h".into(),
        warmup: "5m".into(),
        target_workers: 4,
        // 用 write spec：避开 master file_manifest 路径校验
        read: None,
        write: Some(pb::WriteConfig {
            concurrency: 4,
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

#[tokio::test]
async fn cli_full_lifecycle() {
    let (mut client, handle) = start_master().await;

    // 1) Create
    let create = client
        .create_job(pb::CreateJobRequest {
            spec: Some(good_spec()),
        })
        .await
        .unwrap()
        .into_inner();
    let job_id = create.job_id;
    assert!(!job_id.is_empty());

    // 2) Watch（路线 6：非终态时 stream 持续；终态自动关流）
    let mut events = client
        .watch_job(pb::WatchJobRequest {
            job_id: job_id.clone(),
        })
        .await
        .unwrap()
        .into_inner();
    let first = events.next().await.unwrap().unwrap();
    assert_eq!(first.job_id, job_id);
    assert_eq!(first.status, pb::JobStatus::Pending as i32);
    assert_eq!(first.kind, pb::EventKind::StatusChange as i32);
    drop(events); // 取消订阅；非终态 job 不会自动关流

    // 3) Query
    let q = client
        .query_job(pb::QueryJobRequest {
            job_id: job_id.clone(),
        })
        .await
        .unwrap()
        .into_inner();
    let job = q.job.unwrap();
    assert_eq!(job.job_id, job_id);
    assert_eq!(job.status, pb::JobStatus::Pending as i32);

    // 4) List
    let l = client
        .list_jobs(pb::ListJobsRequest {
            status_filter: None,
            limit: 0,
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(l.jobs.len(), 1);
    assert_eq!(l.jobs[0].job_id, job_id);

    // 5) Delete
    client
        .delete_job(pb::DeleteJobRequest {
            job_id: job_id.clone(),
        })
        .await
        .unwrap();

    // 6) Query 后应当看到 CANCELLED
    let q = client
        .query_job(pb::QueryJobRequest {
            job_id: job_id.clone(),
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(q.job.unwrap().status, pb::JobStatus::Cancelled as i32);

    handle.abort();
}

#[tokio::test]
async fn create_rejects_unknown_fs() {
    let (mut client, handle) = start_master().await;
    let mut spec = good_spec();
    spec.fs_name = "nope".into();
    let err = client
        .create_job(pb::CreateJobRequest { spec: Some(spec) })
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::FailedPrecondition);
    handle.abort();
}

#[tokio::test]
async fn worker_rpcs_return_unknown_job_for_now() {
    let (mut client, handle) = start_master().await;

    let fr = client
        .fetch_job(pb::FetchJobRequest {
            worker_id: "host-1-aaaa1111".into(),
        })
        .await
        .unwrap()
        .into_inner();
    assert!(fr.job_id.is_none(), "no job should be assigned in v0");
    assert!(fr.master_now_ms > 0);

    let rr = client
        .report_ready(pb::ReportReadyRequest {
            job_id: "ghost".into(),
            worker_id: "host-1-aaaa1111".into(),
            error: None,
        })
        .await
        .unwrap()
        .into_inner();
    assert!(rr.unknown_job);

    let fb = client
        .fetch_file_batch(pb::FetchFileBatchRequest {
            job_id: "ghost".into(),
            worker_id: "host-1-aaaa1111".into(),
            batch_size: 100,
        })
        .await
        .unwrap()
        .into_inner();
    assert!(fb.unknown_job);

    let pr = client
        .report_progress(pb::ReportProgressRequest {
            job_id: "ghost".into(),
            progress: Some(pb::WorkerProgress {
                worker_id: "host-1-aaaa1111".into(),
                elapsed_ms: 0,
                per_op: Default::default(),
                phase: pb::WorkerPhase::Preparing as i32,
            }),
        })
        .await
        .unwrap()
        .into_inner();
    assert!(pr.unknown_job);

    let rs = client
        .report_result(pb::ReportResultRequest {
            job_id: "ghost".into(),
            result: Some(pb::WorkerResult {
                worker_id: "host-1-aaaa1111".into(),
                ..pb::WorkerResult::default()
            }),
        })
        .await
        .unwrap()
        .into_inner();
    assert!(rs.unknown_job);

    handle.abort();
}

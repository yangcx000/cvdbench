//! 后台 watcher 端到端：staleness 把卡住的 PREPARING/RUNNING job 推 FAILED；
//! 终态 GC 删掉过期 job。
//!
//! 为了让测试在数百毫秒内完成，使用很短的 staleness/retention 配置，并且
//! 直接调用 `tick_once` 触发巡检，不依赖默认 5s/60s 间隔。

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use cvd_master::config::{MasterConfig, SchedulerConfig};
use cvd_master::service::MasterServiceImpl;
use cvd_master::state::MasterState;
use cvd_master::{gc, scheduler::staleness};
use cvd_proto::cvdbench as pb;
use cvd_proto::cvdbench::master_service_client::MasterServiceClient;
use cvd_proto::cvdbench::master_service_server::MasterServiceServer;
use tokio::net::TcpListener;
use tonic::transport::Server;

async fn start(
    scheduler_cfg: SchedulerConfig,
) -> (Arc<MasterState>, String, tokio::task::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let local = listener.local_addr().unwrap();
    let stream = tokio_stream::wrappers::TcpListenerStream::new(listener);

    let mut filesystems = HashMap::new();
    filesystems.insert("examplefs".into(), PathBuf::from("/mnt/examplefs"));
    let cfg = MasterConfig {
        listen: local,
        metrics_listen: None,
        scheduler: scheduler_cfg,
        filesystems,
    };
    let state = Arc::new(MasterState::new(cfg));
    let service = MasterServiceImpl::new(state.clone());
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
            return (state, endpoint, handle);
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

fn long_running_spec(target_workers: i32) -> pb::BenchSpec {
    pb::BenchSpec {
        fs_name: "examplefs".into(),
        io_mode: "seq".into(),
        io_aligned: true,
        direct_io: false,
        block_size: "1Mi".into(),
        // 长 duration，防止测试期间 job 自然完成
        duration: "1h".into(),
        warmup: String::new(),
        target_workers,
        // write spec：不需要真实 manifest 文件
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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn staleness_watcher_fails_unattended_running_job() {
    let cfg = SchedulerConfig {
        // 让 staleness 立即触发；FetchJob touch 后只要等 ≥150ms 即可
        worker_staleness: Duration::from_millis(100),
        prepare_timeout: Duration::from_secs(60),
        start_delay: Duration::from_millis(10),
        ..SchedulerConfig::default()
    };
    let (state, endpoint, handle) = start(cfg).await;

    // 创建 job target_workers=2
    let job_id = connect(&endpoint)
        .await
        .create_job(pb::CreateJobRequest {
            spec: Some(long_running_spec(2)),
        })
        .await
        .unwrap()
        .into_inner()
        .job_id;

    // 占满 slot 进 PREPARING；workers 之后再不发 RPC
    for w in ["host1-1-aaaaaaaa", "host2-1-bbbbbbbb"] {
        connect(&endpoint)
            .await
            .fetch_job(pb::FetchJobRequest {
                worker_id: w.into(),
            })
            .await
            .unwrap();
    }

    // 等过 staleness 阈值
    tokio::time::sleep(Duration::from_millis(200)).await;
    staleness::tick_once(&state);

    let q = connect(&endpoint)
        .await
        .query_job(pb::QueryJobRequest {
            job_id: job_id.clone(),
        })
        .await
        .unwrap()
        .into_inner();
    let job = q.job.unwrap();
    assert_eq!(job.status, pb::JobStatus::Failed as i32);

    handle.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn prepare_timeout_fails_job_when_some_worker_never_reports_ready() {
    let cfg = SchedulerConfig {
        worker_staleness: Duration::from_secs(60), // 单独验 prepare_timeout，不被 staleness 触发
        prepare_timeout: Duration::from_millis(100),
        start_delay: Duration::from_millis(10),
        ..SchedulerConfig::default()
    };
    let (state, endpoint, handle) = start(cfg).await;

    let job_id = connect(&endpoint)
        .await
        .create_job(pb::CreateJobRequest {
            spec: Some(long_running_spec(2)),
        })
        .await
        .unwrap()
        .into_inner()
        .job_id;

    // 占满 slot 进 PREPARING
    let workers = ["host1-1-aaaaaaaa", "host2-1-bbbbbbbb"];
    for w in &workers {
        connect(&endpoint)
            .await
            .fetch_job(pb::FetchJobRequest {
                worker_id: (*w).into(),
            })
            .await
            .unwrap();
    }
    // 一个 worker ReportReady（不算 stale）；另一个就是不 ready
    connect(&endpoint)
        .await
        .report_ready(pb::ReportReadyRequest {
            job_id: job_id.clone(),
            worker_id: workers[0].into(),
            error: None,
        })
        .await
        .unwrap();

    tokio::time::sleep(Duration::from_millis(200)).await;
    staleness::tick_once(&state);

    let q = connect(&endpoint)
        .await
        .query_job(pb::QueryJobRequest {
            job_id: job_id.clone(),
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(q.job.unwrap().status, pb::JobStatus::Failed as i32);
    let err = state
        .jobs
        .lock()
        .unwrap()
        .get(&job_id)
        .unwrap()
        .error
        .clone();
    let err = err.unwrap_or_default();
    assert!(
        err.contains("prepare_timeout") || err.contains("PREPARING"),
        "expected prepare_timeout reason, got {err:?}"
    );

    handle.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn gc_removes_terminal_jobs_past_retention() {
    let cfg = SchedulerConfig {
        // retention 设极短：100ms
        job_retention: Duration::from_millis(100),
        ..SchedulerConfig::default()
    };
    let (state, endpoint, handle) = start(cfg).await;

    // 创建一个 job + 立即取消 → 进 CANCELLED 终态
    let job_id = connect(&endpoint)
        .await
        .create_job(pb::CreateJobRequest {
            spec: Some(long_running_spec(1)),
        })
        .await
        .unwrap()
        .into_inner()
        .job_id;

    connect(&endpoint)
        .await
        .delete_job(pb::DeleteJobRequest {
            job_id: job_id.clone(),
        })
        .await
        .unwrap();

    // 终态 + 等过 retention 后 tick GC
    tokio::time::sleep(Duration::from_millis(150)).await;
    gc::tick_once(&state);

    // QueryJob 应当返回 NotFound
    let err = connect(&endpoint)
        .await
        .query_job(pb::QueryJobRequest {
            job_id: job_id.clone(),
        })
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::NotFound);

    handle.abort();
}

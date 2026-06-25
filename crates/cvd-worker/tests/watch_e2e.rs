//! WatchJob 端到端：CLI 订阅看到 PENDING → PREPARING → RUNNING → PROGRESS → COMPLETED。

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
use tokio_stream::StreamExt;
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

fn spec_with_manifest(target: i32, duration: &str, manifest_path: &str) -> pb::BenchSpec {
    pb::BenchSpec {
        fs_name: "examplefs".into(),
        io_mode: "seq".into(),
        io_aligned: true,
        direct_io: false,
        block_size: "1Mi".into(),
        duration: duration.into(),
        warmup: String::new(),
        target_workers: target,
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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn watch_observes_full_state_machine() {
    let (endpoint, handle) = start_master().await;

    // 创建 + 立即订阅，保证后续状态变化能被订阅者全部看到。
    let mut admin = connect(&endpoint).await;
    let mf = empty_manifest();
    let mut spec = spec_with_manifest(2, "1s", &mf.path().to_string_lossy());
    if let Some(read) = spec.read.as_mut() {
        // 让 worker 一直 poll FetchFileBatch，duration 内有时间产生 progress 事件
        read.loop_files = true;
    }
    let job_id = admin
        .create_job(pb::CreateJobRequest { spec: Some(spec) })
        .await
        .unwrap()
        .into_inner()
        .job_id;

    let mut watcher = connect(&endpoint).await;
    let mut events = watcher
        .watch_job(pb::WatchJobRequest {
            job_id: job_id.clone(),
        })
        .await
        .unwrap()
        .into_inner();

    // 跑 2 个 worker 完成 job
    let mut worker_tasks = Vec::new();
    for i in 0..2 {
        let endpoint = endpoint.clone();
        let job_id = job_id.clone();
        worker_tasks.push(tokio::spawn(async move {
            let mut c = connect(&endpoint).await;
            let worker_id = format!("hostw-{}-aaaaaaaa", 4000 + i);
            let fetch = c
                .fetch_job(pb::FetchJobRequest {
                    worker_id: worker_id.clone(),
                })
                .await
                .unwrap()
                .into_inner();
            assert_eq!(fetch.job_id.as_deref(), Some(job_id.as_str()));
            let cancelled = Arc::new(AtomicBool::new(false));
            lifecycle::run_assigned_job(&mut c, &worker_id, fetch, &cancelled).await;
        }));
    }

    // 收集所有事件直到流关闭（终态时 master 关流）
    let mut collected: Vec<pb::JobEvent> = Vec::new();
    while let Some(evt) = events.next().await {
        let evt = evt.unwrap();
        collected.push(evt);
    }

    // worker 任务也应已结束
    for t in worker_tasks {
        t.await.unwrap();
    }

    // ─── 断言 ────────────────────────────────────────────────────────────────
    assert!(!collected.is_empty(), "no events received");

    // seq 单调递增
    let mut seqs: Vec<i64> = collected.iter().map(|e| e.seq).collect();
    let mut sorted = seqs.clone();
    sorted.sort_unstable();
    sorted.dedup();
    assert_eq!(seqs, sorted, "seq not monotonic+unique: {seqs:?}");

    // 状态序列里必须出现 PENDING/PREPARING/RUNNING/COMPLETED
    let statuses: Vec<i32> = collected.iter().map(|e| e.status).collect();
    let has = |s: pb::JobStatus| statuses.iter().any(|x| *x == i32::from(s));
    assert!(has(pb::JobStatus::Pending), "no PENDING in {statuses:?}");
    assert!(
        has(pb::JobStatus::Preparing),
        "no PREPARING in {statuses:?}"
    );
    assert!(has(pb::JobStatus::Running), "no RUNNING in {statuses:?}");
    assert!(
        has(pb::JobStatus::Completed),
        "no COMPLETED in {statuses:?}"
    );

    // 至少出现一次 Progress 事件（duration=1s, interval=500ms 应该 1~2 次）
    let progress_count = collected
        .iter()
        .filter(|e| e.kind == i32::from(pb::EventKind::Progress))
        .count();
    assert!(
        progress_count >= 1,
        "expected progress events, got {progress_count}; events={collected:#?}"
    );

    // 最后一条事件状态必须是 COMPLETED
    seqs.sort_unstable();
    let last = collected.iter().max_by_key(|e| e.seq).expect("non-empty");
    assert_eq!(last.status, pb::JobStatus::Completed as i32);

    handle.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn watch_on_already_terminal_emits_one_event_then_closes() {
    let (endpoint, handle) = start_master().await;

    let mf = empty_manifest();
    let job_id = connect(&endpoint)
        .await
        .create_job(pb::CreateJobRequest {
            spec: Some(spec_with_manifest(1, "200ms", &mf.path().to_string_lossy())),
        })
        .await
        .unwrap()
        .into_inner()
        .job_id;

    // 立即取消让 job 进 CANCELLED 终态
    connect(&endpoint)
        .await
        .delete_job(pb::DeleteJobRequest {
            job_id: job_id.clone(),
        })
        .await
        .unwrap();

    // watch 应当只收到一条 CANCELLED 然后流结束
    let mut events = connect(&endpoint)
        .await
        .watch_job(pb::WatchJobRequest { job_id })
        .await
        .unwrap()
        .into_inner();
    let first = events.next().await.unwrap().unwrap();
    assert_eq!(first.status, pb::JobStatus::Cancelled as i32);
    assert!(
        events.next().await.is_none(),
        "stream should close after one event"
    );

    handle.abort();
}

//! 完整生命周期端到端：3 个 worker 走 PENDING→PREPARING→RUNNING→COMPLETED。

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

async fn start_master_with_short_delay() -> (String, tokio::task::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let local = listener.local_addr().unwrap();
    let stream = tokio_stream::wrappers::TcpListenerStream::new(listener);

    let mut filesystems = HashMap::new();
    filesystems.insert("examplefs".into(), std::path::PathBuf::from("/mnt/examplefs"));
    let cfg = MasterConfig {
        listen: local,
        metrics_listen: None,
        scheduler: SchedulerConfig {
            // 50ms 起跑延迟，避免测试等 5s
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
    // 等 server ready
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

/// 创建一个空的 manifest 文件并返回 (handle, path)。
/// 调用方必须保留 NamedTempFile 让文件在测试结束前一直存在。
fn empty_manifest() -> tempfile::NamedTempFile {
    tempfile::NamedTempFile::new().unwrap()
}

fn short_spec_with_manifest(target_workers: i32, manifest_path: &str) -> pb::BenchSpec {
    pb::BenchSpec {
        fs_name: "examplefs".into(),
        io_mode: "seq".into(),
        io_aligned: true,
        direct_io: false,
        block_size: "1Mi".into(),
        // 200ms 整体压测时间，配合 50ms start_delay，整测试 < 1s
        duration: "200ms".into(),
        warmup: String::new(),
        target_workers,
        // read spec + 真实但空的 manifest：master 会 spawn 真 reader 读完即 done=true，
        // worker 端 read runner（v1 sleep stub）耗满 duration → ReportResult success。
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
async fn three_workers_complete_a_job() {
    let (endpoint, handle) = start_master_with_short_delay().await;
    let mf = empty_manifest();

    // 创建 job
    let job_id = connect(&endpoint)
        .await
        .create_job(pb::CreateJobRequest {
            spec: Some(short_spec_with_manifest(3, &mf.path().to_string_lossy())),
        })
        .await
        .unwrap()
        .into_inner()
        .job_id;

    // 启动 3 个 worker：每个独立 client，串完一次 fetch + run_assigned_job
    let mut tasks = Vec::new();
    for i in 0..3 {
        let endpoint = endpoint.clone();
        let job_id = job_id.clone();
        tasks.push(tokio::spawn(async move {
            let mut c = connect(&endpoint).await;
            let worker_id = format!("hostx-{}-aaaaaaaa", 1000 + i);

            // 拿到 job
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

    for t in tasks {
        t.await.unwrap();
    }

    // 验证 COMPLETED
    let mut admin = connect(&endpoint).await;
    let q = admin
        .query_job(pb::QueryJobRequest { job_id })
        .await
        .unwrap()
        .into_inner();
    let job = q.job.unwrap();
    assert_eq!(
        job.status,
        pb::JobStatus::Completed as i32,
        "expected COMPLETED, got {}",
        job.status
    );

    handle.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn worker_local_validate_fail_makes_job_fail() {
    let (endpoint, handle) = start_master_with_short_delay().await;

    // CreateJob 用合法 spec（master 校验过），随后 worker 收到的 redacted spec
    // 仍合法。要触发 worker 端 ReportReady(error=...) 路径，构造一个**非 worker 自身错**的
    // 失败：让 worker 收到 cancelled 之外的 ready error 触发 master FAILED。
    //
    // 这里的做法：让 worker 在拿到 job 后立刻发送 ReportReady(error=...) 模拟本地准备失败，
    // 然后断言 job 转 FAILED。其它 worker 后续 ready/result 应被 master 视为 unknown_job。

    let mf = empty_manifest();
    let job_id = connect(&endpoint)
        .await
        .create_job(pb::CreateJobRequest {
            spec: Some(short_spec_with_manifest(2, &mf.path().to_string_lossy())),
        })
        .await
        .unwrap()
        .into_inner()
        .job_id;

    // 第一个 worker：fetch + 直接 ReportReady(error)
    let mut c1 = connect(&endpoint).await;
    let w1 = "hostx-2001-aaaaaaaa".to_owned();
    c1.fetch_job(pb::FetchJobRequest {
        worker_id: w1.clone(),
    })
    .await
    .unwrap();
    // 第二个 worker：占第二个 slot 触发 PREPARING
    let mut c2 = connect(&endpoint).await;
    let w2 = "hostx-2002-bbbbbbbb".to_owned();
    c2.fetch_job(pb::FetchJobRequest {
        worker_id: w2.clone(),
    })
    .await
    .unwrap();

    let resp = c1
        .report_ready(pb::ReportReadyRequest {
            job_id: job_id.clone(),
            worker_id: w1.clone(),
            error: Some("simulated local failure".into()),
        })
        .await
        .unwrap()
        .into_inner();
    assert!(!resp.cancelled);
    assert!(!resp.unknown_job);
    assert_eq!(resp.start_at_ms, 0);

    // 第二个 worker 再 ReportReady：应当看到 unknown_job
    let r2 = c2
        .report_ready(pb::ReportReadyRequest {
            job_id: job_id.clone(),
            worker_id: w2.clone(),
            error: None,
        })
        .await
        .unwrap()
        .into_inner();
    assert!(
        r2.unknown_job,
        "after FAILED, late ready should see unknown_job"
    );

    // 验证 master 进入 FAILED
    let q = connect(&endpoint)
        .await
        .query_job(pb::QueryJobRequest { job_id })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(q.job.unwrap().status, pb::JobStatus::Failed as i32);

    handle.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn delete_during_running_propagates_cancel() {
    let (endpoint, handle) = start_master_with_short_delay().await;
    let mf = empty_manifest();
    let mut spec = short_spec_with_manifest(1, &mf.path().to_string_lossy());
    spec.duration = "5s".into(); // 给 cancel 充分时间窗口
    if let Some(read) = spec.read.as_mut() {
        // loop_files=true 让 master reader 一直循环空 manifest，FetchFileBatch
        // 永远返回 has_more=true，worker 不会自然完成 → cancel 才有窗口生效
        read.loop_files = true;
    }
    let job_id = connect(&endpoint)
        .await
        .create_job(pb::CreateJobRequest { spec: Some(spec) })
        .await
        .unwrap()
        .into_inner()
        .job_id;

    // 启动一个 worker
    let endpoint_clone = endpoint.clone();
    let job_id_clone = job_id.clone();
    let worker_task = tokio::spawn(async move {
        let mut c = connect(&endpoint_clone).await;
        let worker_id = "hostx-3001-aaaaaaaa".to_owned();
        let fetch = c
            .fetch_job(pb::FetchJobRequest {
                worker_id: worker_id.clone(),
            })
            .await
            .unwrap()
            .into_inner();
        assert_eq!(fetch.job_id.as_deref(), Some(job_id_clone.as_str()));

        let cancelled = Arc::new(AtomicBool::new(false));
        lifecycle::run_assigned_job(&mut c, &worker_id, fetch, &cancelled).await;
    });

    // 等 worker 进 RUNNING
    tokio::time::sleep(Duration::from_millis(200)).await;

    // CLI cancel
    connect(&endpoint)
        .await
        .delete_job(pb::DeleteJobRequest {
            job_id: job_id.clone(),
        })
        .await
        .unwrap();

    // worker 应在 spec.duration 内感知 cancel 并 ReportResult；最多等 6s
    tokio::time::timeout(Duration::from_secs(6), worker_task)
        .await
        .expect("worker did not finish after cancel")
        .unwrap();

    let q = connect(&endpoint)
        .await
        .query_job(pb::QueryJobRequest { job_id })
        .await
        .unwrap()
        .into_inner();
    // CANCELLED 不被 worker 后续 success=true 改写
    assert_eq!(q.job.unwrap().status, pb::JobStatus::Cancelled as i32);

    handle.abort();
}

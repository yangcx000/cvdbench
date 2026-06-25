//! 一致性测试 read runner 端到端：master + 1 worker + 本地 mock S3。
//!
//! 验证两点：
//! 1. 当 mock S3 返回与 FS 文件内容一致 → COMPLETED；
//! 2. 当 mock S3 返回内容不一致 → WorkerResult.success=false，
//!    `consistency_errors` 含一条 `CET_HASH_MISMATCH`。

use std::collections::HashMap;
use std::io::Write as _;
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
use tempfile::tempdir;
use tokio::net::TcpListener;
use tonic::transport::Server;
use wiremock::matchers::{method, path_regex};
use wiremock::{Mock, MockServer, ResponseTemplate};

const BUCKET: &str = "bench";

async fn start_master_with_mount(
    mount_point: std::path::PathBuf,
) -> (String, tokio::task::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let local = listener.local_addr().unwrap();
    let stream = tokio_stream::wrappers::TcpListenerStream::new(listener);

    let mut filesystems = HashMap::new();
    filesystems.insert("tmpfs".into(), mount_point);
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

fn build_spec(
    target_workers: i32,
    duration: &str,
    manifest_path: &str,
    s3_endpoint: &str,
) -> pb::BenchSpec {
    pb::BenchSpec {
        fs_name: "tmpfs".into(),
        io_mode: "seq".into(),
        io_aligned: true,
        direct_io: false,
        block_size: "1Ki".into(),
        duration: duration.into(),
        warmup: String::new(),
        target_workers,
        read: Some(pb::ReadConfig {
            concurrency: 1,
            file_manifest: manifest_path.to_owned(),
            dir_manifest: String::new(),
            think_time: String::new(),
            rate_limit: String::new(),
            s3_consistency_check: Some(pb::ConsistencyConfig {
                bucket_name: BUCKET.into(),
                bucket_url: s3_endpoint.to_owned(),
                access_key: "EXAMPLE_ACCESS_KEY".into(),
                secret_key: "secret".into(),
                region: "us-east-1".into(),
                prefix: String::new(),
                session_token: String::new(),
            }),
            loop_files: false,
        }),
        write: None,
        metadata: None,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn consistency_pass_when_fs_matches_s3() {
    let tmp = tempdir().unwrap();
    let mount = tmp.path().to_path_buf();

    // 写入一个 FS 文件
    let payload = vec![0xAB_u8; 512];
    std::fs::create_dir(mount.join("data")).unwrap();
    std::fs::write(mount.join("data/a.dat"), &payload).unwrap();

    // manifest 指向该文件，s3_key 指向 mock S3 的同名 key
    let manifest_path = tmp.path().join("read.csv");
    {
        let mut f = std::fs::File::create(&manifest_path).unwrap();
        writeln!(f, "data/a.dat,data/a.dat").unwrap();
        f.flush().unwrap();
    }

    // mock S3：GET /bench/data/a.dat 返回相同内容
    let mock_s3 = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path_regex(format!(r"^/{BUCKET}/data/a\.dat$")))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(payload.clone()))
        .mount(&mock_s3)
        .await;

    // 起 master + worker
    let (endpoint, mhandle) = start_master_with_mount(mount.clone()).await;
    let job_id = connect(&endpoint)
        .await
        .create_job(pb::CreateJobRequest {
            spec: Some(build_spec(
                1,
                "200ms",
                &manifest_path.to_string_lossy(),
                &mock_s3.uri(),
            )),
        })
        .await
        .unwrap()
        .into_inner()
        .job_id;

    // 单 worker 跑完
    let endpoint_clone = endpoint.clone();
    let job_id_clone = job_id.clone();
    let worker_id = "hosts-1-aaaaaaaa".to_owned();
    let wid = worker_id.clone();
    let task = tokio::spawn(async move {
        let mut c = connect(&endpoint_clone).await;
        let fetch = c
            .fetch_job(pb::FetchJobRequest {
                worker_id: wid.clone(),
            })
            .await
            .unwrap()
            .into_inner();
        assert_eq!(fetch.job_id.as_deref(), Some(job_id_clone.as_str()));
        let cancelled = Arc::new(AtomicBool::new(false));
        lifecycle::run_assigned_job(&mut c, &wid, fetch, &cancelled).await;
    });
    task.await.unwrap();

    let q = connect(&endpoint)
        .await
        .query_job(pb::QueryJobRequest {
            job_id: job_id.clone(),
        })
        .await
        .unwrap()
        .into_inner();
    let job = q.job.unwrap();
    assert_eq!(
        job.status,
        pb::JobStatus::Completed as i32,
        "expected COMPLETED"
    );
    let agg = q.aggregated.unwrap();
    // 应当有 read + consistency 两个 op
    assert!(agg.total_per_op.contains_key("read"));
    assert!(agg.total_per_op.contains_key("consistency"));
    let consistency = &agg.total_per_op["consistency"];
    assert!(
        consistency.total_ops >= 1,
        "expected ≥1 consistency op, got {}",
        consistency.total_ops
    );
    // worker 没有 ConsistencyError
    for w in &agg.per_worker {
        assert!(
            w.consistency_errors.is_empty(),
            "consistency_errors should be empty: {:?}",
            w.consistency_errors
        );
    }

    mhandle.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn consistency_fails_on_hash_mismatch() {
    let tmp = tempdir().unwrap();
    let mount = tmp.path().to_path_buf();

    // FS 文件内容 vs S3 内容故意不同
    std::fs::create_dir(mount.join("data")).unwrap();
    std::fs::write(mount.join("data/a.dat"), vec![0xAA_u8; 256]).unwrap();
    let manifest_path = tmp.path().join("read.csv");
    {
        let mut f = std::fs::File::create(&manifest_path).unwrap();
        writeln!(f, "data/a.dat,data/a.dat").unwrap();
        f.flush().unwrap();
    }
    let mock_s3 = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path_regex(format!(r"^/{BUCKET}/data/a\.dat$")))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(vec![0xBB_u8; 256]))
        .mount(&mock_s3)
        .await;

    let (endpoint, mhandle) = start_master_with_mount(mount.clone()).await;
    let job_id = connect(&endpoint)
        .await
        .create_job(pb::CreateJobRequest {
            spec: Some(build_spec(
                1,
                "200ms",
                &manifest_path.to_string_lossy(),
                &mock_s3.uri(),
            )),
        })
        .await
        .unwrap()
        .into_inner()
        .job_id;

    let endpoint_clone = endpoint.clone();
    let job_id_clone = job_id.clone();
    let worker_id = "hosts-2-bbbbbbbb".to_owned();
    let wid = worker_id.clone();
    let task = tokio::spawn(async move {
        let mut c = connect(&endpoint_clone).await;
        let fetch = c
            .fetch_job(pb::FetchJobRequest {
                worker_id: wid.clone(),
            })
            .await
            .unwrap()
            .into_inner();
        assert_eq!(fetch.job_id.as_deref(), Some(job_id_clone.as_str()));
        let cancelled = Arc::new(AtomicBool::new(false));
        lifecycle::run_assigned_job(&mut c, &wid, fetch, &cancelled).await;
    });
    task.await.unwrap();

    let q = connect(&endpoint)
        .await
        .query_job(pb::QueryJobRequest {
            job_id: job_id.clone(),
        })
        .await
        .unwrap()
        .into_inner();
    let job = q.job.unwrap();
    assert_eq!(
        job.status,
        pb::JobStatus::Failed as i32,
        "expected FAILED, got {}",
        job.status
    );

    let agg = q.aggregated.unwrap();
    assert_eq!(agg.per_worker.len(), 1);
    let w = &agg.per_worker[0];
    assert!(!w.success);
    assert!(
        !w.consistency_errors.is_empty(),
        "expected consistency_errors, got none"
    );
    let ce = &w.consistency_errors[0];
    assert_eq!(
        ce.r#type,
        pb::ConsistencyErrorType::CetHashMismatch as i32,
        "expected CET_HASH_MISMATCH, got {} ({})",
        ce.r#type,
        ce.message
    );

    mhandle.abort();
}

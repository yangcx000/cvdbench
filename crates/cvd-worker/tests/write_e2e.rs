//! 写场景端到端：用 tempdir 作 mount_point，2 个 worker 实际写文件，
//! 校验 master 终态聚合的 metrics 非零，以及工作集 cleanup 行为正确。

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
use tempfile::tempdir;
use tokio::net::TcpListener;
use tonic::transport::Server;

async fn start_master_with_mount(
    fs_name: &str,
    mount_point: std::path::PathBuf,
) -> (String, tokio::task::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let local = listener.local_addr().unwrap();
    let stream = tokio_stream::wrappers::TcpListenerStream::new(listener);

    let mut filesystems = HashMap::new();
    filesystems.insert(fs_name.to_owned(), mount_point);
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

fn write_spec(target: i32, duration: &str, cleanup: bool) -> pb::BenchSpec {
    pb::BenchSpec {
        fs_name: "tmpfs".into(),
        io_mode: "seq".into(),
        io_aligned: true,
        direct_io: false,
        block_size: "4Ki".into(),
        duration: duration.into(),
        warmup: String::new(),
        target_workers: target,
        read: None,
        write: Some(pb::WriteConfig {
            concurrency: 2,
            dir: "bench/write".into(),
            file_size: "4Ki".into(),
            file_size_range: None,
            fsync: false,
            cleanup,
            think_time: String::new(),
            rate_limit: String::new(),
            verify_after_write: false,
        }),
        metadata: None,
    }
}

async fn drive_worker(endpoint: String, expected_job_id: String, worker_id: String) {
    let mut c = connect(&endpoint).await;
    let fetch = c
        .fetch_job(pb::FetchJobRequest {
            worker_id: worker_id.clone(),
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(fetch.job_id.as_deref(), Some(expected_job_id.as_str()));
    let cancelled = Arc::new(AtomicBool::new(false));
    lifecycle::run_assigned_job(&mut c, &worker_id, fetch, &cancelled).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn write_runner_two_workers_completes_with_metrics() {
    let tmp = tempdir().unwrap();
    let mount = tmp.path().to_path_buf();
    let (endpoint, handle) = start_master_with_mount("tmpfs", mount.clone()).await;

    let job_id = connect(&endpoint)
        .await
        .create_job(pb::CreateJobRequest {
            spec: Some(write_spec(2, "300ms", false)),
        })
        .await
        .unwrap()
        .into_inner()
        .job_id;

    let mut tasks = Vec::new();
    for i in 0..2 {
        let endpoint = endpoint.clone();
        let job_id = job_id.clone();
        let worker_id = format!("hostw-{}-aaaaaaaa", 5000 + i);
        tasks.push(tokio::spawn(drive_worker(endpoint, job_id, worker_id)));
    }
    for t in tasks {
        t.await.unwrap();
    }

    // 终态查询
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

    let agg = q
        .aggregated
        .expect("aggregated must be present in terminal");
    let total = agg.total.expect("total");
    assert!(
        total.total_ops > 0,
        "expected at least one write op, got {}",
        total.total_ops
    );
    assert!(total.total_bytes > 0);
    assert!(total.throughput_mbps > 0.0);
    assert!(agg.total_per_op.contains_key("write"));
    assert_eq!(agg.per_worker.len(), 2);
    for w in &agg.per_worker {
        assert!(w.success);
    }

    // 工作集应当还在（cleanup=false）
    let work_root = mount.join("bench/write");
    assert!(work_root.is_dir());

    handle.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn write_runner_with_cleanup_removes_work_root() {
    let tmp = tempdir().unwrap();
    let mount = tmp.path().to_path_buf();
    let (endpoint, handle) = start_master_with_mount("tmpfs", mount.clone()).await;

    let job_id = connect(&endpoint)
        .await
        .create_job(pb::CreateJobRequest {
            spec: Some(write_spec(1, "200ms", true)),
        })
        .await
        .unwrap()
        .into_inner()
        .job_id;

    let worker_id = "hostw-9001-aaaaaaaa".to_owned();
    drive_worker(endpoint.clone(), job_id.clone(), worker_id.clone()).await;

    let q = connect(&endpoint)
        .await
        .query_job(pb::QueryJobRequest { job_id })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(q.job.unwrap().status, pb::JobStatus::Completed as i32);

    // cleanup=true → 该 worker 自己的 job 子目录应被删
    let job_root = mount.join("bench/write").join(&worker_id);
    assert!(
        !job_root.exists() || job_root.read_dir().map_or(true, |d| d.count() == 0),
        "cleanup should remove worker's job subtree"
    );

    handle.abort();
}

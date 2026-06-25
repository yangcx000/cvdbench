//! 元数据场景端到端：用 tempdir 作 mount_point，2 个 worker 真跑 metadata 5 op，
//! 校验 master 终态聚合的 per_op 都非零，且 layout 真实落盘。

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

fn metadata_spec(target: i32, duration: &str) -> pb::BenchSpec {
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
        write: None,
        metadata: Some(pb::MetadataConfig {
            concurrency: 2,
            dir: "bench/meta".into(),
            ops: vec![
                "stat".into(),
                "open".into(),
                "readdir".into(),
                "create".into(),
                "mkdir".into(),
            ],
            depth: 2,
            width: 2,
            files_per_dir: 2,
            think_time: String::new(),
            rate_limit: String::new(),
            layout_concurrency: 4,
            read_only: false,
            read_only_scan_limit: 0,
            dir_manifest: String::new(),
        }),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn metadata_runner_two_workers_complete() {
    let tmp = tempdir().unwrap();
    let mount = tmp.path().to_path_buf();
    let (endpoint, handle) = start_master_with_mount(mount.clone()).await;

    let job_id = connect(&endpoint)
        .await
        .create_job(pb::CreateJobRequest {
            spec: Some(metadata_spec(2, "300ms")),
        })
        .await
        .unwrap()
        .into_inner()
        .job_id;

    let mut tasks = Vec::new();
    for i in 0..2 {
        let endpoint = endpoint.clone();
        let job_id = job_id.clone();
        let worker_id = format!("hostm-{}-aaaaaaaa", 6000 + i);
        tasks.push(tokio::spawn(async move {
            let mut c = connect(&endpoint).await;
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

    let agg = q.aggregated.expect("aggregated");
    assert_eq!(agg.per_worker.len(), 2);
    for w in &agg.per_worker {
        assert!(w.success);
    }
    // 5 个 op 都应有非零样本
    for op in [
        "metadata.stat",
        "metadata.open",
        "metadata.readdir",
        "metadata.create",
        "metadata.mkdir",
    ] {
        let m = agg
            .total_per_op
            .get(op)
            .unwrap_or_else(|| panic!("missing per_op key {op}"));
        assert!(m.total_ops > 0, "{op} ops should be > 0");
    }

    // 工作集落盘验证（depth=2, width=2 → 6 dirs，files_per_dir=2 → 12 files per worker）
    let worker_root = mount.join("bench/meta");
    assert!(worker_root.is_dir());
    let mut count = 0;
    let mut iter = tokio::fs::read_dir(&worker_root).await.unwrap();
    while iter.next_entry().await.unwrap().is_some() {
        count += 1;
    }
    assert_eq!(count, 2, "should have 2 worker_id subdirs");

    handle.abort();
}

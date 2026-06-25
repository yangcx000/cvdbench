//! 读场景端到端：tempdir 作 mount_point，造若干文件 + 一个 manifest，
//! 2 个 worker 通过 FetchFileBatch + 真读跑完，校验 master 终态聚合的 per_op="read"。

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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn read_runner_two_workers_consume_manifest() {
    // 1) 准备 mount + 数据集
    let tmp = tempdir().unwrap();
    let mount = tmp.path().to_path_buf();
    let data_dir = mount.join("dataset");
    std::fs::create_dir(&data_dir).unwrap();
    let mut file_paths_relative = Vec::new();
    for i in 0..32 {
        let name = format!("dataset/f_{i:04}.dat");
        let path = mount.join(&name);
        let mut f = std::fs::File::create(&path).unwrap();
        // 写入 4KiB 内容
        let buf = vec![b'x'; 4096];
        f.write_all(&buf).unwrap();
        file_paths_relative.push(name);
    }

    // 2) 写 manifest 文件（在 tempdir 外即可；这里放 mount 旁边）
    let manifest_path = tmp.path().join("read.csv");
    {
        let mut f = std::fs::File::create(&manifest_path).unwrap();
        for rel in &file_paths_relative {
            writeln!(f, "{rel}").unwrap();
        }
        f.flush().unwrap();
    }

    // 3) 起 master
    let (endpoint, handle) = start_master_with_mount(mount.clone()).await;

    // 4) CreateJob
    let spec = pb::BenchSpec {
        fs_name: "tmpfs".into(),
        io_mode: "seq".into(),
        io_aligned: true,
        direct_io: false,
        block_size: "4Ki".into(),
        duration: "300ms".into(),
        warmup: String::new(),
        target_workers: 2,
        read: Some(pb::ReadConfig {
            concurrency: 2,
            file_manifest: manifest_path.to_string_lossy().into_owned(),
            dir_manifest: String::new(),
            think_time: String::new(),
            rate_limit: String::new(),
            s3_consistency_check: None,
            loop_files: true, // 让 manifest 不耗尽，duration 主导停止
        }),
        write: None,
        metadata: None,
    };
    let job_id = connect(&endpoint)
        .await
        .create_job(pb::CreateJobRequest { spec: Some(spec) })
        .await
        .unwrap()
        .into_inner()
        .job_id;

    // 5) 启动 2 个 worker 跑完整 lifecycle
    let mut tasks = Vec::new();
    for i in 0..2 {
        let endpoint = endpoint.clone();
        let job_id = job_id.clone();
        let worker_id = format!("hostr-{}-aaaaaaaa", 7000 + i);
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

    // 6) 验证终态
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
        "expected COMPLETED, got {}",
        job.status
    );

    let agg = q.aggregated.expect("aggregated must be present");
    let read_metric = agg
        .total_per_op
        .get("read")
        .expect("per_op[read] must exist");
    let open_metric = agg
        .total_per_op
        .get("read.open")
        .expect("per_op[read.open] must exist");
    let close_metric = agg
        .total_per_op
        .get("read.close")
        .expect("per_op[read.close] must exist");
    assert!(
        read_metric.total_ops > 0,
        "expected > 0 read ops, got {}",
        read_metric.total_ops
    );
    assert!(
        read_metric.total_bytes >= 4096,
        "expected at least one block worth of bytes, got {}",
        read_metric.total_bytes
    );
    assert!(open_metric.total_ops > 0);
    assert_eq!(open_metric.total_bytes, 0);
    assert!(close_metric.total_ops > 0);
    assert_eq!(close_metric.total_bytes, 0);

    handle.abort();
}

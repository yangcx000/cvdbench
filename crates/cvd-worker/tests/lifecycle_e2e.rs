//! 端到端：启动 in-process master，让 worker lifecycle 跑一会儿再 shutdown，
//! 同时验证 `try_fetch_once` 在没有 job 时返回 [`FetchOutcome::NoJob`]。

use std::sync::Arc;
use std::time::Duration;

use cvd_master::config::{MasterConfig, SchedulerConfig};
use cvd_master::service::MasterServiceImpl;
use cvd_master::state::MasterState;
use cvd_proto::cvdbench::master_service_client::MasterServiceClient;
use cvd_proto::cvdbench::master_service_server::MasterServiceServer;
use cvd_worker::lifecycle::{run_loop, try_fetch_once, FetchOutcome};
use tokio::net::TcpListener;
use tonic::transport::Server;

async fn start_master() -> (
    MasterServiceClient<tonic::transport::Channel>,
    tokio::task::JoinHandle<()>,
) {
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

    let endpoint = format!("http://{local}");
    for _ in 0..40 {
        if let Ok(c) = MasterServiceClient::connect(endpoint.clone()).await {
            return (c, handle);
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("connect master failed");
}

#[tokio::test]
async fn try_fetch_once_returns_no_job_when_master_idle() {
    let (mut client, handle) = start_master().await;
    let outcome = try_fetch_once(&mut client, "host-1-aaaa1111")
        .await
        .unwrap();
    assert!(matches!(outcome, FetchOutcome::NoJob));
    handle.abort();
}

#[tokio::test]
async fn run_loop_exits_on_shutdown() {
    let (client, handle) = start_master().await;
    let (tx, rx) = tokio::sync::oneshot::channel::<()>();

    // 200ms 后请求关停；同时 lifecycle 会在 idle 期间自然检查 shutdown。
    let shutdown = async move {
        let _ = rx.await;
    };
    let loop_handle = tokio::spawn(run_loop(client, "host-1-aaaa1111".into(), shutdown));

    tokio::time::sleep(Duration::from_millis(200)).await;
    let _ = tx.send(());

    // 给一个充足的退出窗口（idle sleep = 3s，但 select 会立即取消 sleep）。
    let res = tokio::time::timeout(Duration::from_secs(2), loop_handle)
        .await
        .expect("run_loop did not exit within 2s")
        .expect("run_loop task panicked");
    res.unwrap();

    handle.abort();
}

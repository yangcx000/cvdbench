//! `cvd-master` 二进制入口。

use std::path::PathBuf;
use std::sync::Arc;

use clap::Parser;
use cvd_master::scheduler::staleness;
use cvd_master::service::MasterServiceImpl;
use cvd_master::state::MasterState;
use cvd_master::{config, gc, metrics};
use cvd_proto::cvdbench::master_service_server::MasterServiceServer;
use tonic::transport::Server;

#[derive(Debug, Parser)]
#[command(name = "cvd-master", about = "cvdbench master daemon")]
struct Cli {
    /// 配置文件路径
    #[arg(long, default_value = "cvd-master.toml")]
    config: PathBuf,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let cli = Cli::parse();
    let cfg = config::load(&cli.config)?;
    let listen = cfg.listen;
    tracing::info!(%listen, filesystems = cfg.filesystems.len(), "cvd-master starting");

    let state = Arc::new(MasterState::new(cfg));

    // 后台巡检任务：staleness + 终态 GC（spec §5.5 / §2）
    let _staleness_handle = staleness::spawn_watcher(state.clone());
    let _gc_handle = gc::spawn_watcher(state.clone());
    let _metrics_handle = state
        .config
        .metrics_listen
        .map(|listen| metrics::spawn_endpoint(state.clone(), listen));

    let service = MasterServiceImpl::new(state);
    Server::builder()
        .add_service(MasterServiceServer::new(service))
        .serve(listen)
        .await?;
    Ok(())
}

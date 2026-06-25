//! `cvd-worker` 二进制入口。
//!
//! 启动顺序：
//! 1. 解析 `--master <ip:port>`；
//! 2. 生成 worker_id（spec §6.1）；
//! 3. 连接 master（指数退避至成功）；
//! 4. 进入 [`cvd_worker::lifecycle::run_loop`]，监听 SIGINT / SIGTERM 退出。

use clap::Parser;

#[derive(Debug, Parser)]
#[command(
    name = "cvd-worker",
    about = "cvdbench worker daemon (pure gRPC client)"
)]
struct Cli {
    /// master 地址 `<ip>:<port>`
    #[arg(long)]
    master: String,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let cli = Cli::parse();
    let worker_id = cvd_worker::id::generate();
    tracing::info!(%worker_id, master = %cli.master, "cvd-worker starting");

    let endpoint = format!("http://{}", cli.master);
    let client = cvd_worker::client::connect_with_backoff(endpoint).await;
    tracing::info!(%worker_id, "connected to master");

    cvd_worker::lifecycle::run_loop(client, worker_id, cvd_worker::lifecycle::shutdown_signal())
        .await
}

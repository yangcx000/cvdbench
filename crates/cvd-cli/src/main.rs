//! `cvd-cli` 二进制入口。

use std::path::PathBuf;

use clap::{Parser, Subcommand};

use cvd_cli::cmd;

#[derive(Debug, Parser)]
#[command(name = "cvd-cli", about = "cvdbench command-line client")]
struct Cli {
    /// master 地址 `<ip>:<port>`
    #[arg(long, global = true, default_value = "127.0.0.1:9090")]
    master: String,

    #[command(subcommand)]
    command: Cmd,
}

#[derive(Debug, Subcommand)]
enum Cmd {
    /// 创建 job 并默认 watch 进度直到终态
    Create {
        /// job 配置 JSON 路径
        #[arg(long)]
        config: PathBuf,
        /// 结果输出路径（默认 ./{job_id}.json）
        #[arg(long)]
        output: Option<PathBuf>,
        /// 显示 workload 和 per-op 等详细信息
        #[arg(long)]
        verbose: bool,
    },
    /// 观察 job 实时事件流
    Watch {
        job_id: String,
        /// 终态汇总显示 workload 和 per-op 等详细信息
        #[arg(long)]
        verbose: bool,
    },
    /// 一次性查询 job
    Query {
        job_id: String,
        /// 把响应额外写到该 JSON 文件
        #[arg(long)]
        output: Option<PathBuf>,
        /// 显示 workload 和 per-op 等详细信息
        #[arg(long)]
        verbose: bool,
    },
    /// 删除 / 取消 job
    Delete { job_id: String },
    /// 列举 jobs
    List {
        #[arg(long)]
        status: Option<String>,
        #[arg(long, default_value_t = 0)]
        limit: i32,
    },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let cli = Cli::parse();
    match cli.command {
        Cmd::Create {
            config,
            output,
            verbose,
        } => cmd::create::run(&cli.master, &config, output, verbose).await,
        Cmd::Watch { job_id, verbose } => cmd::watch::run(&cli.master, &job_id, verbose).await,
        Cmd::Query {
            job_id,
            output,
            verbose,
        } => cmd::query::run(&cli.master, &job_id, output, verbose).await,
        Cmd::Delete { job_id } => cmd::delete::run(&cli.master, &job_id).await,
        Cmd::List { status, limit } => cmd::list::run(&cli.master, status.as_deref(), limit).await,
    }
}

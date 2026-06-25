//! Ctrl+C 处理：spec §7 要求 CLI 第一次 Ctrl+C 时询问「detach（继续后台运行）/ cancel job」。
//!
//! 实现策略：
//! - 第一次按下 Ctrl+C：在终端 prompt 用户选择；选 `cancel` → 调 DeleteJob 并继续 watch；
//!   选 `detach` → 直接退出 CLI，job 仍在 master 上跑。
//! - 第二次（prompt 期间再按 Ctrl+C）：进程立即退出，让用户能跳过 prompt。

use std::io::{self, IsTerminal, Write};

use cvd_proto::cvdbench as pb;
use cvd_proto::cvdbench::master_service_client::MasterServiceClient;
use tonic::transport::Channel;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CtrlCAction {
    /// 用户选择继续后台运行：CLI 退出但 master 上 job 不取消。
    Detach,
    /// 用户选择取消 job：CLI 调 DeleteJob 后等待终态。
    Cancel,
    /// 终端非 tty 或 stdin 不可用，按 Cancel 处理（保守路径，符合 spec §7
    /// "Ctrl+C 默认询问继续后台运行 / 取消 job" 的语义中的"取消"侧）。
    DefaultCancel,
}

/// 在终端 prompt 用户选择 `detach` 或 `cancel`；非 tty 时直接 `DefaultCancel`。
pub fn prompt_action() -> CtrlCAction {
    if !io::stdin().is_terminal() {
        eprintln!("\n^C received; non-tty stdin → defaulting to cancel");
        return CtrlCAction::DefaultCancel;
    }
    eprintln!();
    eprintln!("^C received. Choose action:");
    eprintln!("  [d] detach (continue job in background)");
    eprintln!("  [c] cancel job (default)");
    eprint!("> ");
    let _ = io::stderr().flush();
    let mut buf = String::new();
    if io::stdin().read_line(&mut buf).is_err() {
        return CtrlCAction::DefaultCancel;
    }
    let trimmed = buf.trim().to_ascii_lowercase();
    match trimmed.as_str() {
        "d" | "detach" => CtrlCAction::Detach,
        // 默认 cancel；空回车也按 cancel 处理（避免错按 Enter 把 job 留在后台）
        _ => CtrlCAction::Cancel,
    }
}

/// 异步 prompt：把阻塞 stdin 读取放到 blocking 线程，并在 prompt 期间监听第二次
/// Ctrl+C。这样主 async runtime 仍可推进其它任务，也符合本模块文档中的二次
/// Ctrl+C 立即退出语义。
pub async fn prompt_action_async() -> CtrlCAction {
    let prompt = tokio::task::spawn_blocking(prompt_action);
    tokio::select! {
        action = prompt => action.unwrap_or(CtrlCAction::DefaultCancel),
        _ = tokio::signal::ctrl_c() => {
            eprintln!("\nsecond Ctrl+C received; exiting immediately");
            std::process::exit(130);
        }
    }
}

/// 调用 DeleteJob 取消 job。失败仅 warn，让 watch 继续等终态（master 可能已经
/// 处理过；幂等）。
pub async fn cancel_job(
    client: &mut MasterServiceClient<Channel>,
    job_id: &str,
) -> anyhow::Result<()> {
    match client
        .delete_job(pb::DeleteJobRequest {
            job_id: job_id.to_owned(),
        })
        .await
    {
        Ok(_) => {
            eprintln!("DeleteJob requested for {job_id}");
            Ok(())
        }
        Err(e) => {
            eprintln!("warning: DeleteJob failed: {e}");
            Ok(()) // 仍继续 watch
        }
    }
}

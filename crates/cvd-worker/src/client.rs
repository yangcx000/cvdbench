//! tonic Channel 重连封装。
//!
//! tonic 自身的 [`tonic::transport::Channel`] 已经带连接复用 + 单调重连（只要保留
//! Channel 实例，下一次 RPC 会触发底层 hyper 客户端重新建连）。本模块只负责
//! **首次** 连接的指数退避（spec §6.4：起 200ms，倍增到 30s 上限），随后续 RPC
//! 失败由 lifecycle 层用更短的退避自行处理。

use std::time::Duration;

use cvd_proto::cvdbench::master_service_client::MasterServiceClient;
use tonic::transport::Channel;

const INITIAL_BACKOFF: Duration = Duration::from_millis(200);
const MAX_BACKOFF: Duration = Duration::from_secs(30);

/// 阻塞地连接 master，直到成功为止。
///
/// 仅在 worker 启动早期使用一次；运行期失联交给 lifecycle 处理。
pub async fn connect_with_backoff(endpoint: String) -> MasterServiceClient<Channel> {
    let mut delay = INITIAL_BACKOFF;
    loop {
        match MasterServiceClient::connect(endpoint.clone()).await {
            Ok(client) => return client,
            Err(err) => {
                tracing::warn!(%endpoint, %err, ?delay, "connect master failed, retrying");
                tokio::time::sleep(delay).await;
                delay = (delay * 2).min(MAX_BACKOFF);
            }
        }
    }
}

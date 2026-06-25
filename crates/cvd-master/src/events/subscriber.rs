//! 单条 WatchJob 流的订阅者：mpsc 通道 + 断连 evict。
//!
//! 设计点：
//! - 每个 `WatchJob` RPC 注册一个独立 `mpsc::Sender`；接收端送给 tonic 的
//!   `ReceiverStream`。
//! - 广播时遍历该 job 下所有 sender，`try_send` 非阻塞；返回
//!   `Closed` 的 sender（CLI 端断连）从列表里 evict。
//! - 容量 64：单 watcher 的事件突发缓冲。`Full` 时认定 client 端拖慢，evict
//!   该订阅者并向 stream 推一个 `ResourceExhausted` 状态作为最后一条消息——
//!   这样 master 不会被慢 client 持续占住缓冲（spec §6.7 fail-fast 风格）。
//! - 终态时调用 [`SubscriberRegistry::terminate`] 一次性 drop 所有 sender，
//!   client 看到 stream 结束。

use std::collections::HashMap;
use std::sync::Mutex;

use cvd_proto::cvdbench as pb;
use tokio::sync::mpsc;

const SUBSCRIBER_CAPACITY: usize = 64;

/// 单个 watcher 的事件 channel sender。
pub type EventSender = mpsc::Sender<Result<pb::JobEvent, tonic::Status>>;

/// 全局订阅注册表。挂在 `MasterState` 上。
pub struct SubscriberRegistry {
    inner: Mutex<HashMap<String, Vec<EventSender>>>,
}

impl SubscriberRegistry {
    pub fn new() -> Self {
        Self {
            inner: Mutex::new(HashMap::new()),
        }
    }

    /// 注册一个新 watcher，返回事件接收端供 tonic stream 使用。
    pub fn subscribe(&self, job_id: &str) -> mpsc::Receiver<Result<pb::JobEvent, tonic::Status>> {
        let (tx, rx) = mpsc::channel(SUBSCRIBER_CAPACITY);
        let mut map = self.inner.lock().expect("subscribers mutex");
        map.entry(job_id.to_owned()).or_default().push(tx);
        rx
    }

    /// 注册 watcher 并把一条快照事件作为流上的首条消息排队。
    ///
    /// 调用方应在持有 `state.jobs` 锁期间使用本方法，保证快照与后续广播按
    /// `seq` 单调，避免快照插入时被并发 emit 抢跑。
    pub fn subscribe_with_snapshot(
        &self,
        job_id: &str,
        snapshot: pb::JobEvent,
    ) -> mpsc::Receiver<Result<pb::JobEvent, tonic::Status>> {
        let (tx, rx) = mpsc::channel(SUBSCRIBER_CAPACITY);
        // 容量 64，刚 new 出来一定有空位
        let _ = tx.try_send(Ok(snapshot));
        let mut map = self.inner.lock().expect("subscribers mutex");
        map.entry(job_id.to_owned()).or_default().push(tx);
        rx
    }

    /// 把事件广播给该 `job_id` 下所有 watcher。
    pub fn broadcast(&self, job_id: &str, event: &pb::JobEvent) {
        let mut map = self.inner.lock().expect("subscribers mutex");
        let Some(list) = map.get_mut(job_id) else {
            return;
        };
        list.retain(|tx| match tx.try_send(Ok(event.clone())) {
            Ok(()) => true,
            Err(mpsc::error::TrySendError::Closed(_)) => false,
            Err(mpsc::error::TrySendError::Full(_)) => {
                tracing::warn!(%job_id, "subscriber buffer full, evicting slow watcher");
                // 尝试塞一条 ResourceExhausted 作为 last word；失败也无所谓，
                // client 会以 Stream 提前结束的方式得到提示。
                let _ = tx.try_send(Err(tonic::Status::resource_exhausted(
                    "watcher buffer overflowed; reconnect to resume",
                )));
                false
            }
        });
        if list.is_empty() {
            map.remove(job_id);
        }
    }

    /// 终态时调用：drop 全部 sender，让 watcher stream 结束。
    pub fn terminate(&self, job_id: &str) {
        let mut map = self.inner.lock().expect("subscribers mutex");
        map.remove(job_id);
    }

    #[cfg(test)]
    pub fn subscriber_count(&self, job_id: &str) -> usize {
        self.inner
            .lock()
            .unwrap()
            .get(job_id)
            .map_or(0, std::vec::Vec::len)
    }
}

impl Default for SubscriberRegistry {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use cvd_proto::cvdbench as pb;

    use super::*;

    fn dummy_event(seq: i64) -> pb::JobEvent {
        pb::JobEvent {
            job_id: "j1".into(),
            status: pb::JobStatus::Pending.into(),
            worker_progress: vec![],
            aggregated: None,
            error: None,
            timestamp: 0,
            seq,
            kind: pb::EventKind::StatusChange.into(),
            dirs_scanned: 0,
            files_scanned: 0,
            scan_duration_ms: 0,
        }
    }

    #[tokio::test]
    async fn broadcast_reaches_all_subscribers() {
        let reg = SubscriberRegistry::new();
        let mut a = reg.subscribe("j1");
        let mut b = reg.subscribe("j1");
        reg.broadcast("j1", &dummy_event(1));
        let ea = a.recv().await.unwrap().unwrap();
        let eb = b.recv().await.unwrap().unwrap();
        assert_eq!(ea.seq, 1);
        assert_eq!(eb.seq, 1);
    }

    #[tokio::test]
    async fn dropped_receiver_is_evicted() {
        let reg = SubscriberRegistry::new();
        let a = reg.subscribe("j1");
        assert_eq!(reg.subscriber_count("j1"), 1);
        drop(a);
        // 第一次广播会触发 Closed 检测，evict
        reg.broadcast("j1", &dummy_event(1));
        assert_eq!(reg.subscriber_count("j1"), 0);
    }

    #[tokio::test]
    async fn terminate_closes_stream() {
        let reg = SubscriberRegistry::new();
        let mut a = reg.subscribe("j1");
        reg.terminate("j1");
        // sender 被 drop，receiver 看到 None
        assert!(a.recv().await.is_none());
    }

    #[tokio::test]
    async fn full_buffer_evicts_slow_watcher() {
        let reg = SubscriberRegistry::new();
        let mut a = reg.subscribe("j1");
        // 灌满 64 个事件；watcher 不读 → buffer 持续 Full
        for seq in 0..(SUBSCRIBER_CAPACITY as i64) {
            reg.broadcast("j1", &dummy_event(seq));
        }
        assert_eq!(reg.subscriber_count("j1"), 1);
        // 第 65 条触发 Full → evict
        reg.broadcast("j1", &dummy_event(SUBSCRIBER_CAPACITY as i64));
        assert_eq!(reg.subscriber_count("j1"), 0);
        // sender 被 drop 后，watcher 把已缓冲的 64 条收完，再读到 None。
        // ResourceExhausted 只是 best-effort（buffer 已满时塞不进去），不强测。
        for _ in 0..SUBSCRIBER_CAPACITY {
            let _ = a.recv().await.unwrap().unwrap();
        }
        // 之后任何下一条要么是 Status::ResourceExhausted，要么是 None（流结束）
        match a.recv().await {
            None => {}
            Some(Err(s)) => assert_eq!(s.code(), tonic::Code::ResourceExhausted),
            Some(Ok(e)) => panic!(
                "did not expect another Ok event after eviction: seq={}",
                e.seq
            ),
        }
    }
}

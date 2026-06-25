//! 有界队列：`BoundedQueue<FileEntry>` / `BoundedQueue<PathBuf>`。
//!
//! 写入阻塞作为 manifest 生产侧背压（spec §5.7：「队列满则阻塞」）；
//! 消费侧批量 drain 与 FetchFileBatch 的 batch_size 配合。
//!
//! 实现细节：
//! - `Semaphore` 维护「空槽位」许可数，`acquire` 用作生产端反压；
//! - 队列内容用 `Mutex<VecDeque<T>>` 保持 FIFO；
//! - `close()` 关闭 semaphore，使 in-flight 的 `push().await` 立即返回 `Err`；
//!   已 buffer 的元素仍可被 `drain_up_to` 拉走。

use std::collections::VecDeque;
use std::sync::Mutex;

use thiserror::Error;
use tokio::sync::Semaphore;

#[derive(Debug, Error)]
#[error("queue closed")]
pub struct QueueClosed;

pub struct BoundedQueue<T> {
    inner: Mutex<VecDeque<T>>,
    free_slots: Semaphore,
    capacity: usize,
}

impl<T> BoundedQueue<T> {
    /// 容量 0 视为 1，避免 push 永久阻塞。
    #[must_use]
    pub fn new(capacity: usize) -> Self {
        let cap = capacity.max(1);
        Self {
            inner: Mutex::new(VecDeque::with_capacity(cap.min(1024))),
            free_slots: Semaphore::new(cap),
            capacity: cap,
        }
    }

    #[must_use]
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// 异步 push。容量满则阻塞，直到 [`drain_up_to`] 释放槽位或 [`close`] 被调用。
    pub async fn push(&self, item: T) -> Result<(), QueueClosed> {
        let permit = self.free_slots.acquire().await.map_err(|_| QueueClosed)?;
        permit.forget();
        self.inner
            .lock()
            .expect("queue inner mutex")
            .push_back(item);
        Ok(())
    }

    /// 同步 drain：取出至多 `n` 条数据。
    ///
    /// 释放被 push 占住的 semaphore 许可，让生产方能继续推数据。
    pub fn drain_up_to(&self, n: usize) -> Vec<T> {
        let mut out = Vec::new();
        let take = {
            let mut q = self.inner.lock().expect("queue inner mutex");
            let take = n.min(q.len());
            for _ in 0..take {
                if let Some(item) = q.pop_front() {
                    out.push(item);
                }
            }
            take
        };
        if take > 0 {
            self.free_slots.add_permits(take);
        }
        out
    }

    /// 当前已 buffer 的元素数。
    #[must_use]
    pub fn len(&self) -> usize {
        self.inner.lock().expect("queue inner mutex").len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// 关闭：立即唤醒所有 await 中的 push 让它们返回 `Err(QueueClosed)`；
    /// drain 仍可拉走 buffer 中的剩余元素。
    pub fn close(&self) {
        self.free_slots.close();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::time::Duration;

    #[tokio::test]
    async fn push_within_capacity() {
        let q = BoundedQueue::<u32>::new(3);
        q.push(1).await.unwrap();
        q.push(2).await.unwrap();
        q.push(3).await.unwrap();
        assert_eq!(q.len(), 3);
    }

    #[tokio::test]
    async fn drain_up_to_returns_subset_in_order() {
        let q = BoundedQueue::<u32>::new(8);
        for i in 0..5u32 {
            q.push(i).await.unwrap();
        }
        let batch = q.drain_up_to(3);
        assert_eq!(batch, vec![0, 1, 2]);
        let batch = q.drain_up_to(10);
        assert_eq!(batch, vec![3, 4]);
        assert!(q.is_empty());
    }

    #[tokio::test]
    async fn drain_releases_capacity_for_push() {
        let q = Arc::new(BoundedQueue::<u32>::new(1));
        q.push(0).await.unwrap();
        // 第二次 push 应当被卡住直到我们 drain
        let q2 = q.clone();
        let waiter = tokio::spawn(async move { q2.push(1).await });
        // 让 waiter 先进入 await
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert_eq!(q.drain_up_to(1), vec![0]);
        // 现在 waiter 应当能完成
        tokio::time::timeout(Duration::from_millis(200), waiter)
            .await
            .expect("waiter should finish quickly")
            .unwrap()
            .unwrap();
    }

    #[tokio::test]
    async fn close_unblocks_pending_push_with_err() {
        let q = Arc::new(BoundedQueue::<u32>::new(1));
        q.push(0).await.unwrap();
        let q2 = q.clone();
        let waiter = tokio::spawn(async move { q2.push(1).await });
        tokio::time::sleep(Duration::from_millis(20)).await;
        q.close();
        let res = tokio::time::timeout(Duration::from_millis(200), waiter)
            .await
            .expect("waiter should resolve")
            .unwrap();
        assert!(res.is_err());
        // close 后 buffer 还可以 drain
        assert_eq!(q.drain_up_to(2), vec![0]);
    }
}

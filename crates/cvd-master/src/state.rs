//! Master 内存状态（spec §5.2）。
//!
//! 当前阶段只承载 CLI 端能闭环所需的最小集合：
//! - [`MasterState`] 含 jobs map / pending FIFO / event seq；
//! - [`JobRecord`] 是 spec.md §5.2 中 `JobState` 的精简前身，仅保留 v0 必需字段。

use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::SystemTime;

use crate::config::MasterConfig;
use crate::events::SubscriberRegistry;

pub mod job;
pub mod pending;

pub use job::{JobRecord, WorkerAssignment};

pub struct MasterState {
    pub config: Arc<MasterConfig>,
    pub jobs: Mutex<HashMap<String, JobRecord>>,
    pub pending_queue: Mutex<VecDeque<String>>,
    /// `worker_id → job_id`：保证一个 worker 同一时刻最多绑定一个非终态 job，
    /// 同时支撑 FetchJob 幂等重放（spec §5.4 / §5.5）。
    pub worker_active_jobs: Mutex<HashMap<String, String>>,
    /// WatchJob 订阅者注册表（spec §5.6）。
    pub subscribers: SubscriberRegistry,
    event_seq: AtomicI64,
}

impl MasterState {
    pub fn new(config: MasterConfig) -> Self {
        Self {
            config: Arc::new(config),
            jobs: Mutex::new(HashMap::new()),
            pending_queue: Mutex::new(VecDeque::new()),
            worker_active_jobs: Mutex::new(HashMap::new()),
            subscribers: SubscriberRegistry::new(),
            event_seq: AtomicI64::new(1),
        }
    }

    pub fn next_event_seq(&self) -> i64 {
        self.event_seq.fetch_add(1, Ordering::Relaxed)
    }
}

/// 当前 unix 毫秒时间戳；`SystemTime` 异常时返回 `0`。
#[must_use]
pub fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map(|d| i64::try_from(d.as_millis()).unwrap_or(i64::MAX))
        .unwrap_or(0)
}

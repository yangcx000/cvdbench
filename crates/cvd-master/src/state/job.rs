//! 单 job 状态记录与 proto 互转（spec §5.2 / §5.3）。

use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::sync::Arc;
use std::time::Instant;

use cvd_proto::cvdbench as pb;
use tokio::task::JoinHandle;

use crate::manifest::BoundedQueue;

/// 同一个 job 上一个 worker 占走的 slot 元信息。
#[derive(Debug, Clone)]
pub struct WorkerAssignment {
    /// `0..target_workers - 1`，用于 worker 内部 shard / seed 隔离。
    pub worker_index: u32,
}

/// Master 侧 manifest 扫描统计。
#[derive(Debug, Default)]
pub struct ManifestScanStats {
    pub dirs_scanned: AtomicI64,
    pub files_scanned: AtomicI64,
    pub scan_duration_ms: AtomicI64,
}

impl ManifestScanStats {
    pub fn snapshot(&self) -> (i64, i64, i64) {
        (
            self.dirs_scanned.load(Ordering::SeqCst),
            self.files_scanned.load(Ordering::SeqCst),
            self.scan_duration_ms.load(Ordering::SeqCst),
        )
    }
}

/// 一个 job 的内存状态。
///
/// 字段分组：
/// - 「基本」：`job_id` / `spec_redacted` / `credentials` / 时间戳 / 错误；
/// - 「调度快照」：CreateJob 时确定的 `mount_point` / `target_workers`；
/// - 「PREPARING/RUNNING」：worker 分配、ready 屏障、起跑时间、结果汇总；
/// - 「活性」：`worker_last_seen` 由所有 worker RPC 刷新（spec §5.5）。
pub struct JobRecord {
    pub job_id: String,
    pub spec_redacted: pb::BenchSpec,
    pub credentials: Option<pb::S3CredentialMaterial>,
    pub status: pb::JobStatus,
    pub created_at_ms: i64,
    pub error: Option<String>,

    pub mount_point: PathBuf,
    pub target_workers: u32,

    pub worker_assignments: HashMap<String, WorkerAssignment>,
    pub run_workers: HashSet<String>,
    pub ready_workers: HashSet<String>,
    pub start_at_ms: i64,
    pub worker_results: HashMap<String, pb::WorkerResult>,
    pub worker_last_seen: HashMap<String, Instant>,
    pub preparing_since: Option<Instant>,

    /// 每个 worker 的最近一次 ReportProgress；用于构造 JobEvent 的 `worker_progress`。
    pub latest_progress: HashMap<String, pb::WorkerProgress>,

    /// 终态聚合结果（spec §4.2）：进入 COMPLETED / FAILED / CANCELLED 时一次性计算并固化。
    pub aggregated: Option<pb::AggregatedMetrics>,

    /// 聚合时各 worker measure 窗口交集为空（spec §4.2 window_misaligned）。
    /// 仅作为诊断信息透出；不影响状态机。
    pub window_misaligned: bool,
    /// 缺失 histogram 的 worker 数量；CLI 据此标注 latency 不完整（spec §4.2）。
    pub missing_histogram_count: usize,
    /// 参与 histogram 合并的成功 worker 数（用于审计 / CLI 标注）。
    pub success_worker_count: usize,

    /// 进入终态时刻的 unix ms；GC 据此判断 retention 是否到期（spec §2 / §5.1）。
    pub terminal_at_ms: Option<i64>,

    // ── 读 job 流水线（spec §5.7） ──────────────────────────────────────────
    /// 读 job 的有界文件队列；manifest reader 写入，FetchFileBatch 消费。
    pub file_queue: Option<Arc<BoundedQueue<pb::FileEntry>>>,
    /// 进入终态时翻 true，让 manifest reader / scanner 立即退出。
    pub cancel_flag: Arc<AtomicBool>,
    /// manifest 全部读完时翻 true；FetchFileBatch 据此决定 has_more。
    pub manifest_done: Arc<AtomicBool>,
    /// manifest reader / scanner spawn 出来的任务句柄；终态时 abort。
    pub manifest_handle: Option<JoinHandle<()>>,
    /// dir_manifest 扫描统计；file_manifest 模式保持 0。
    pub manifest_scan_stats: Arc<ManifestScanStats>,
}

impl JobRecord {
    pub fn new(
        job_id: String,
        spec_redacted: pb::BenchSpec,
        credentials: Option<pb::S3CredentialMaterial>,
        mount_point: PathBuf,
        target_workers: u32,
        created_at_ms: i64,
    ) -> Self {
        Self {
            job_id,
            spec_redacted,
            credentials,
            status: pb::JobStatus::Pending,
            created_at_ms,
            error: None,
            mount_point,
            target_workers,
            worker_assignments: HashMap::new(),
            run_workers: HashSet::new(),
            ready_workers: HashSet::new(),
            start_at_ms: 0,
            worker_results: HashMap::new(),
            worker_last_seen: HashMap::new(),
            preparing_since: None,
            latest_progress: HashMap::new(),
            aggregated: None,
            window_misaligned: false,
            missing_histogram_count: 0,
            success_worker_count: 0,
            terminal_at_ms: None,
            file_queue: None,
            cancel_flag: Arc::new(AtomicBool::new(false)),
            manifest_done: Arc::new(AtomicBool::new(false)),
            manifest_handle: None,
            manifest_scan_stats: Arc::new(ManifestScanStats::default()),
        }
    }

    /// 序列化成协议 `Job` 消息。
    pub fn to_pb_job(&self) -> pb::Job {
        pb::Job {
            job_id: self.job_id.clone(),
            spec: Some(self.spec_redacted.clone()),
            status: self.status.into(),
            created_at: self.created_at_ms,
        }
    }

    /// 是否处于终态（不再接受调度推进）。
    pub fn is_terminal(&self) -> bool {
        matches!(
            self.status,
            pb::JobStatus::Completed | pb::JobStatus::Failed | pb::JobStatus::Cancelled
        )
    }

    /// 已占的 slot 数。
    pub fn slots_filled(&self) -> u32 {
        // worker_assignments 数量受 target_workers 控制，hash 大小一定能塞进 u32。
        u32::try_from(self.worker_assignments.len()).unwrap_or(u32::MAX)
    }

    /// 剩余可占 slot 数。
    pub fn slots_remaining(&self) -> u32 {
        self.target_workers.saturating_sub(self.slots_filled())
    }

    /// Master 视角的参与 worker 数：PREPARING/RUNNING/终态使用固化后的
    /// `run_workers`，PENDING 阶段使用已占 slot 数。
    pub fn run_worker_count(&self) -> usize {
        if self.run_workers.is_empty() {
            self.worker_assignments.len()
        } else {
            self.run_workers.len()
        }
    }

    /// 把任意 worker RPC 当作活性 ping，刷新 last_seen。
    pub fn touch(&mut self, worker_id: &str) {
        self.worker_last_seen
            .insert(worker_id.to_owned(), Instant::now());
    }

    /// 在 PENDING 阶段为 worker 分配 slot。
    ///
    /// 如果 slot 已占完返回 `None`；调用者根据 `slots_remaining() == 0`
    /// 自行触发 PENDING → PREPARING 转换。
    pub fn try_assign(&mut self, worker_id: &str) -> Option<WorkerAssignment> {
        if self.slots_remaining() == 0 {
            return None;
        }
        let assignment = WorkerAssignment {
            worker_index: self.slots_filled(),
        };
        self.worker_assignments
            .insert(worker_id.to_owned(), assignment.clone());
        self.touch(worker_id);
        Some(assignment)
    }

    /// 占满 slot 后调用：固化 run_workers 并切到 PREPARING。
    ///
    /// 幂等：仅在 status==PENDING 时翻状态，避免 fetch_job 防御分支重复调用时
    /// 重置 `preparing_since` 让 prepare_timeout 被无限续命（spec §5.3 / §5.5）。
    pub fn enter_preparing(&mut self) {
        if self.status != pb::JobStatus::Pending {
            return;
        }
        debug_assert_eq!(self.slots_remaining(), 0);
        self.run_workers = self.worker_assignments.keys().cloned().collect();
        self.preparing_since = Some(Instant::now());
        self.status = pb::JobStatus::Preparing;
    }

    /// 进入终态时调用：通知 manifest reader 退出 + close 队列 + abort 后台 task。
    /// 幂等；多次调用没有副作用。
    pub fn cleanup_on_terminal(&mut self) {
        self.cancel_flag.store(true, Ordering::SeqCst);
        if let Some(q) = &self.file_queue {
            q.close();
        }
        if let Some(h) = self.manifest_handle.take() {
            h.abort();
        }
    }
}

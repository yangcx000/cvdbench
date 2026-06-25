//! Worker 主循环骨架（spec §6.4）。
//!
//! 范围：
//! - 启动连接 master，FetchJob 轮询；
//! - 收到 job 后跑完 happy path：local validate → preflight (mkdir/layout/
//!   consistency client) → ReportReady 屏障 → 等 `start_at_ms` →
//!   runner hot-loop → ReportResult；
//! - cancelled / unknown_job：清理后回到 FetchJob 轮询；
//! - SIGINT / SIGTERM：daemon 级 shutdown，bridge 任务把信号镜像成 per-job
//!   cancelled，hot-loop 干净退出后 daemon 才离开主循环。

use std::collections::HashMap;
use std::future::Future;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};
use std::sync::{Arc, Mutex as StdMutex};
use std::time::{Duration, Instant};

use cvd_common::metrics::MetricsRegistry;
use cvd_common::spec::{
    validate::{validate, ValidationContext},
    BenchSpec,
};
use cvd_proto::cvdbench as pb;
use cvd_proto::cvdbench::master_service_client::MasterServiceClient;
use tokio::fs as tokio_fs;
use tonic::transport::Channel;

use crate::backoff::ExponentialBackoff;
use crate::clock::MasterClockOffset;
use crate::fs_io::IoProfile;
use crate::prebuild::metadata::{self as metadata_prebuild, Layout as MetadataLayout};
use crate::runner::consistency::ConsistencyClient;
use crate::runner::metadata::{self as metadata_runner, MetadataContext};
use crate::runner::read::{self as read_runner, ReadContext};
use crate::runner::write::{self as write_runner, WriteContext};

const IDLE_SLEEP: Duration = Duration::from_secs(3);
/// `ReportReady` 屏障未开时的轮询周期；spec §6.4 要求 worker 在 start_at_ms=0
/// 时短暂 sleep 后再问。
const READY_POLL_INTERVAL: Duration = Duration::from_millis(500);
/// 中断检测的颗粒度：每 100ms 检查一次 shutdown flag。
const SHUTDOWN_POLL_GRANULARITY: Duration = Duration::from_millis(100);
/// hot loop / preflight 阶段 ReportProgress 周期（与 spec §5.5 worker_staleness=60s 默认对齐）。
const PROGRESS_INTERVAL: Duration = Duration::from_millis(500);
/// `send_ready_error` 最多重试次数，配合 backoff；防止退避无限运行影响 daemon 退出。
const READY_ERROR_MAX_RETRIES: u32 = 5;
/// `ReportResult` 最多重试次数。结果上报会释放 master 侧 active 占位，不能因
/// 一次瞬时 RPC 失败就丢弃。
const REPORT_RESULT_MAX_RETRIES: u32 = 8;

/// `FetchJob` 的返回简写。
#[derive(Debug, Clone)]
pub enum FetchOutcome {
    NoJob,
    JobAssigned(Box<pb::FetchJobResponse>),
}

/// 单次 FetchJob，把响应归类。
pub async fn try_fetch_once(
    client: &mut MasterServiceClient<Channel>,
    worker_id: &str,
) -> Result<FetchOutcome, tonic::Status> {
    let resp = client
        .fetch_job(pb::FetchJobRequest {
            worker_id: worker_id.to_owned(),
        })
        .await?
        .into_inner();
    if resp.job_id.is_some() {
        Ok(FetchOutcome::JobAssigned(Box::new(resp)))
    } else {
        Ok(FetchOutcome::NoJob)
    }
}

/// 主循环：持续 FetchJob，直到 `shutdown` 触发。
pub async fn run_loop<S>(
    mut client: MasterServiceClient<Channel>,
    worker_id: String,
    shutdown: S,
) -> anyhow::Result<()>
where
    S: Future<Output = ()> + Send + 'static,
{
    tracing::info!(%worker_id, "worker lifecycle loop started");

    // daemon 级 shutdown：fed by SIGINT/SIGTERM。job 内部用独立的 per-job
    // cancelled，由 bridge 任务把 shutdown 镜像过去；spec §6.4 区分两者，
    // 让正在跑的 job 能 graceful 收尾后再退出 daemon。
    let shutdown_flag = Arc::new(AtomicBool::new(false));
    let _watcher = {
        let flag = shutdown_flag.clone();
        tokio::spawn(async move {
            shutdown.await;
            flag.store(true, Ordering::SeqCst);
        })
    };

    let mut backoff = ExponentialBackoff::default();
    loop {
        if shutdown_flag.load(Ordering::SeqCst) {
            break;
        }

        match try_fetch_once(&mut client, &worker_id).await {
            Ok(FetchOutcome::NoJob) => {
                tracing::trace!(%worker_id, "no job assigned, idling");
                backoff.reset();
                interruptible_sleep(IDLE_SLEEP, &shutdown_flag).await;
            }
            Ok(FetchOutcome::JobAssigned(resp)) => {
                backoff.reset();
                run_assigned_job(&mut client, &worker_id, *resp, &shutdown_flag).await;
                // job 处理完毕（无论成败 / cancel / unknown），立即回到轮询。
            }
            Err(status) => {
                let delay = backoff.next_delay();
                tracing::warn!(%worker_id, code = ?status.code(), msg = %status.message(), ?delay,
                    "FetchJob RPC failed, retry after backoff");
                interruptible_sleep(delay, &shutdown_flag).await;
            }
        }
    }

    tracing::info!(%worker_id, "shutdown signalled, leaving lifecycle loop");
    Ok(())
}

/// 把 daemon 级 `shutdown_flag` 桥接到 per-job `job_cancelled`。返回
/// `JoinHandle`，job 终结后由调用方 abort 以释放该任务。
fn spawn_shutdown_bridge(
    shutdown_flag: Arc<AtomicBool>,
    job_cancelled: Arc<AtomicBool>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            if shutdown_flag.load(Ordering::SeqCst) {
                job_cancelled.store(true, Ordering::SeqCst);
                return;
            }
            if job_cancelled.load(Ordering::SeqCst) {
                return;
            }
            tokio::time::sleep(SHUTDOWN_POLL_GRANULARITY).await;
        }
    })
}

/// 尽力清理 root 目录（不跟随 symlink），失败仅 warn 不阻断流程（spec §6.6）。
async fn best_effort_cleanup(mount: &Path, root: &Path) -> std::io::Result<()> {
    if !root.starts_with(mount) {
        return Err(std::io::Error::new(
            std::io::ErrorKind::PermissionDenied,
            format!("cleanup target {root:?} escapes mount {mount:?}"),
        ));
    }
    let meta = match tokio_fs::symlink_metadata(root).await {
        Ok(m) => m,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(e) => return Err(e),
    };
    if meta.file_type().is_symlink() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::PermissionDenied,
            "cleanup root is a symlink",
        ));
    }
    if !meta.is_dir() {
        return Ok(());
    }
    // 不跟随 symlink 的递归删除：栈式遍历。
    let mut stack: Vec<(PathBuf, bool)> = vec![(root.to_path_buf(), false)];
    while let Some((dir, visited)) = stack.pop() {
        if visited {
            if let Err(e) = tokio_fs::remove_dir(&dir).await {
                if e.kind() != std::io::ErrorKind::NotFound {
                    return Err(e);
                }
            }
            continue;
        }
        stack.push((dir.clone(), true));
        let mut entries = match tokio_fs::read_dir(&dir).await {
            Ok(e) => e,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
            Err(e) => return Err(e),
        };
        while let Some(entry) = entries.next_entry().await? {
            let p = entry.path();
            let m = match tokio_fs::symlink_metadata(&p).await {
                Ok(m) => m,
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
                Err(e) => return Err(e),
            };
            if m.file_type().is_dir() {
                stack.push((p, false));
            } else if let Err(e) = tokio_fs::remove_file(&p).await {
                if e.kind() != std::io::ErrorKind::NotFound {
                    return Err(e);
                }
            }
        }
    }
    Ok(())
}

/// 跟踪 worker 当前所处阶段；与 master clock 上的关键时间点比较得出。
///
/// 编码到 `AtomicU8` 里，跨 task 廉价共享。`Cancelled` / `UnknownJob` 不在
/// 状态机中，由独立的 `AtomicBool` 处理。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
enum LocalPhase {
    Preparing = 0,
    WaitingStart = 1,
    Warmup = 2,
    Measuring = 3,
    Cleanup = 4,
    Finished = 5,
}

impl LocalPhase {
    fn to_pb(self) -> pb::WorkerPhase {
        match self {
            LocalPhase::Preparing => pb::WorkerPhase::Preparing,
            LocalPhase::WaitingStart => pb::WorkerPhase::WaitingStart,
            LocalPhase::Warmup => pb::WorkerPhase::Warmup,
            LocalPhase::Measuring => pb::WorkerPhase::Measuring,
            LocalPhase::Cleanup => pb::WorkerPhase::Cleanup,
            LocalPhase::Finished => pb::WorkerPhase::Finished,
        }
    }

    fn from_u8(v: u8) -> Self {
        match v {
            0 => LocalPhase::Preparing,
            1 => LocalPhase::WaitingStart,
            2 => LocalPhase::Warmup,
            3 => LocalPhase::Measuring,
            4 => LocalPhase::Cleanup,
            _ => LocalPhase::Finished,
        }
    }
}

#[derive(Debug)]
struct PhaseTracker(AtomicU8);

impl PhaseTracker {
    fn new(initial: LocalPhase) -> Self {
        Self(AtomicU8::new(initial as u8))
    }
    fn set(&self, p: LocalPhase) {
        self.0.store(p as u8, Ordering::SeqCst);
    }
    fn get(&self) -> LocalPhase {
        LocalPhase::from_u8(self.0.load(Ordering::SeqCst))
    }
}

/// 走完一个被分配的 job 的 happy / cancel 路径。
///
/// `shutdown_flag` 是 daemon 级别（SIGINT/SIGTERM 喂食）的；本函数为该 job
/// 创建独立的 `job_cancelled`，并通过 bridge 任务把 shutdown 镜像进去。
pub async fn run_assigned_job(
    client: &mut MasterServiceClient<Channel>,
    worker_id: &str,
    fetch_resp: pb::FetchJobResponse,
    shutdown_flag: &Arc<AtomicBool>,
) {
    let Some(job_id) = fetch_resp.job_id.clone() else {
        tracing::error!("FetchJobResponse missing job_id; ignoring");
        return;
    };
    let mount_point = match fetch_resp.mount_point.clone() {
        Some(s) if !s.is_empty() => PathBuf::from(s),
        _ => {
            tracing::error!(%worker_id, %job_id, "FetchJobResponse missing mount_point");
            send_ready_error(client, &job_id, worker_id, "missing mount_point").await;
            return;
        }
    };
    let worker_index = fetch_resp.worker_index.unwrap_or(0).max(0) as u32;
    let local_recv = Instant::now();
    let mut clock = MasterClockOffset::from_response(fetch_resp.master_now_ms, local_recv);

    // per-job cancel：master 通知 cancelled 或 daemon 收到 shutdown 时翻转。
    // bridge 任务把 daemon shutdown 镜像过来，job 收尾后 abort。
    let job_cancelled = Arc::new(AtomicBool::new(false));
    // unknown_job 与 cancelled 分开：unknown 表示 master 不再认识本 job，
    // worker 应清理后跳过 ReportResult；cancelled 是正常退出，仍需上报 success=true。
    let unknown_job = Arc::new(AtomicBool::new(false));
    let bridge = spawn_shutdown_bridge(shutdown_flag.clone(), job_cancelled.clone());

    // phase tracker：preflight 阶段的 progress task 用这个判断当前 phase。
    let phase = Arc::new(PhaseTracker::new(LocalPhase::Preparing));

    // 1) 解析 + 本地校验
    let business_spec = match parse_and_validate(fetch_resp.spec.clone()) {
        Ok(s) => s,
        Err(reason) => {
            tracing::warn!(%worker_id, %job_id, %reason, "local validate failed");
            send_ready_error(client, &job_id, worker_id, &reason).await;
            bridge.abort();
            return;
        }
    };

    // 2) Preflight 期间持续上报 ReportProgress(phase=PREPARING, per_op=空)
    //    避免长 metadata layout 期间被 master staleness 误判（spec §5.5 / §6.6）。
    let metrics_for_preflight = Arc::new(MetricsRegistry::new());
    let preflight_progress = spawn_progress_task(
        client.clone(),
        job_id.clone(),
        worker_id.to_owned(),
        unknown_job.clone(),
        job_cancelled.clone(),
        Arc::new(MetricsRegistry::new()), // preflight 期 per_op 必须空
        0,
        clock.local_anchor,
        clock.master_anchor_ms,
        phase.clone(),
        /* report_metrics = */ false,
    );

    // 3) Preflight：在 ReportReady 之前完成 mkdir / metadata layout / 一致性
    //    client 构造，任一失败立即 ReportReady(error) 让 master 转 FAILED。
    //    避免起跑后才暴露这些慢且容易失败的初始化（spec §6.4）。
    let preflight = match run_preflight(
        &fetch_resp,
        &business_spec,
        &mount_point,
        worker_id,
        &job_id,
        worker_index,
    )
    .await
    {
        Ok(p) => p,
        Err(reason) => {
            preflight_progress.abort();
            tracing::warn!(%worker_id, %job_id, %reason, "preflight failed");
            send_ready_error(client, &job_id, worker_id, &reason).await;
            bridge.abort();
            return;
        }
    };
    let _ = metrics_for_preflight;
    preflight_progress.abort();

    // 保留 metadata layout root 路径用于结束时 best-effort cleanup（spec §6.6
    // 写明根目录是 `{dir}/{worker_id}/{job_id}/`，long-running daemon 不及时
    // 清理会撑爆 mount）。
    let metadata_root_for_cleanup = if business_spec
        .metadata
        .as_ref()
        .is_some_and(|meta| !meta.read_only && meta.dir_manifest.is_none())
    {
        preflight.metadata_layout.as_ref().map(|l| l.root.clone())
    } else {
        None
    };

    // 4) ReportReady 屏障：进入 WAITING_START 阶段，刷 phase。
    phase.set(LocalPhase::WaitingStart);
    let start_at_ms = match wait_for_start_barrier(
        client,
        &job_id,
        worker_id,
        &job_cancelled,
        &mut clock,
    )
    .await
    {
        BarrierOutcome::Start(t) => t,
        BarrierOutcome::Cancelled => {
            tracing::info!(%worker_id, %job_id, "job cancelled at barrier, reporting clean stop");
            phase.set(LocalPhase::Finished);
            let now_master = clock_now_master(&clock, Instant::now());
            let result = build_result_no_metrics(
                worker_id,
                true,
                Some("cancelled"),
                0,
                now_master,
                now_master,
            );
            let _ = client
                .report_result(pb::ReportResultRequest {
                    job_id: job_id.clone(),
                    result: Some(result),
                })
                .await;
            bridge.abort();
            return;
        }
        BarrierOutcome::UnknownJob => {
            tracing::info!(%worker_id, %job_id, "job unknown at barrier, abandoning");
            unknown_job.store(true, Ordering::SeqCst);
            phase.set(LocalPhase::Finished);
            bridge.abort();
            return;
        }
    };

    // 5) 时间轴换算（master clock）
    let warmup_ms = i64::try_from(business_spec.warmup.as_millis()).unwrap_or(0);
    let duration_ms = i64::try_from(business_spec.duration.as_millis()).unwrap_or(0);
    let measure_start_ms = start_at_ms.saturating_add(warmup_ms);
    let planned_end_ms = measure_start_ms.saturating_add(duration_ms);

    let start_local = clock.master_to_local(start_at_ms);
    let measure_start_local = clock.master_to_local(measure_start_ms);
    let deadline_local = clock.master_to_local(planned_end_ms);

    // 等到 start_at_ms（warmup 起点）—— 仍在 WAITING_START 阶段。
    let now_local = Instant::now();
    if start_local > now_local {
        interruptible_sleep(start_local - now_local, &job_cancelled).await;
    }

    // 6) 进入 hot loop：phase 切换由独立 task 在 measure_start_local / deadline_local
    //    时间点驱动。
    phase.set(if business_spec.warmup.is_zero() {
        LocalPhase::Measuring
    } else {
        LocalPhase::Warmup
    });
    let phase_advancer = spawn_phase_advancer(
        phase.clone(),
        measure_start_local,
        deadline_local,
        job_cancelled.clone(),
    );

    let metrics = Arc::new(MetricsRegistry::new());
    let abort = Arc::new(AtomicBool::new(false));
    let consistency_errors = Arc::new(StdMutex::new(Vec::<pb::ConsistencyError>::new()));

    let progress_task = spawn_progress_task(
        client.clone(),
        job_id.clone(),
        worker_id.to_owned(),
        unknown_job.clone(),
        job_cancelled.clone(),
        metrics.clone(),
        measure_start_ms,
        clock.local_anchor,
        clock.master_anchor_ms,
        phase.clone(),
        /* report_metrics = */ true,
    );

    // 7) 选择并跑 runner，使用 preflight 的产物
    let runner_outcome = run_workload(
        client.clone(),
        &business_spec,
        &mount_point,
        worker_id,
        &job_id,
        worker_index,
        metrics.clone(),
        abort,
        job_cancelled.clone(),
        unknown_job.clone(),
        measure_start_local,
        deadline_local,
        preflight,
        consistency_errors.clone(),
    )
    .await;

    progress_task.abort();
    phase_advancer.abort();
    phase.set(LocalPhase::Cleanup);

    // metadata best-effort cleanup（spec §6.6 layout per-job 隔离）
    if let Some(root) = &metadata_root_for_cleanup {
        if let Err(e) = best_effort_cleanup(&mount_point, root).await {
            tracing::warn!(%worker_id, %job_id, ?root, ?e,
                "metadata layout cleanup failed; leaving in place");
        }
    }

    // 8) unknown_job：master 已不认识本 job，跳过 ReportResult，立即收尾（spec §6.4 / §6.9）。
    if unknown_job.load(Ordering::SeqCst) {
        tracing::info!(%worker_id, %job_id, "unknown_job mid-run, skipping ReportResult");
        phase.set(LocalPhase::Finished);
        bridge.abort();
        return;
    }

    // 9) 计算实际测量窗口（worker 视角）。effective_secs 不再 `.max(1)` 假装
    //    至少 1ms：如果实际窗口为 0（job 在测量前就 cancel/失败），iops/throughput
    //    应当老老实实写 0，不要把分母虚构为 1ms 让 IOPS 看起来异常高。
    let measure_end_ms = clock_now_master(&clock, Instant::now()).min(planned_end_ms);
    let effective_duration_ms = (measure_end_ms - measure_start_ms).max(0);
    #[allow(clippy::cast_precision_loss)]
    let effective_secs = (effective_duration_ms as f64) / 1000.0;

    let was_cancelled = job_cancelled.load(Ordering::SeqCst);
    let (success, error) = match runner_outcome {
        RunnerOutcome::Ok => (true, None),
        RunnerOutcome::Failed { error } => (false, Some(error)),
        RunnerOutcome::Cancelled => (true, Some("cancelled".to_owned())),
        RunnerOutcome::NotImplemented => (true, None),
    };
    let final_error = if was_cancelled && success && error.is_none() {
        Some("cancelled".to_owned())
    } else {
        error
    };

    let per_op = metrics.snapshot_pb(effective_secs);
    let drained_consistency_errors = consistency_errors
        .lock()
        .map(|mut v| std::mem::take(&mut *v))
        .unwrap_or_default();
    let result = pb::WorkerResult {
        worker_id: worker_id.to_owned(),
        per_op,
        consistency_errors: drained_consistency_errors,
        success,
        error: final_error,
        effective_duration_ms,
        measure_start_ms,
        measure_end_ms,
    };

    match report_result_with_retry(
        client,
        &job_id,
        result,
        worker_id,
        &job_cancelled,
        &unknown_job,
    )
    .await
    {
        Ok(ReportResultOutcome::Acked) => {
            tracing::info!(%worker_id, %job_id, was_cancelled, success, "job result reported");
        }
        Ok(ReportResultOutcome::UnknownJob) => {
            tracing::info!(%worker_id, %job_id, "ReportResult returned unknown_job");
        }
        Err(e) => {
            tracing::warn!(%worker_id, %job_id, error = %e, "ReportResult retries exhausted");
        }
    }

    phase.set(LocalPhase::Finished);
    bridge.abort();
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ReportResultOutcome {
    Acked,
    UnknownJob,
}

async fn report_result_with_retry(
    client: &mut MasterServiceClient<Channel>,
    job_id: &str,
    result: pb::WorkerResult,
    worker_id: &str,
    cancelled: &AtomicBool,
    unknown_job: &AtomicBool,
) -> Result<ReportResultOutcome, tonic::Status> {
    let mut backoff = ExponentialBackoff::default();
    let mut last_err: Option<tonic::Status> = None;
    for attempt in 0..REPORT_RESULT_MAX_RETRIES {
        if unknown_job.load(Ordering::SeqCst) {
            return Ok(ReportResultOutcome::UnknownJob);
        }
        match client
            .report_result(pb::ReportResultRequest {
                job_id: job_id.to_owned(),
                result: Some(result.clone()),
            })
            .await
        {
            Ok(resp) => {
                if resp.into_inner().unknown_job {
                    unknown_job.store(true, Ordering::SeqCst);
                    return Ok(ReportResultOutcome::UnknownJob);
                }
                return Ok(ReportResultOutcome::Acked);
            }
            Err(err) => {
                let delay = backoff.next_delay();
                tracing::warn!(%worker_id, %job_id, error = %err, ?delay, attempt,
                    "ReportResult RPC failed, retry");
                last_err = Some(err);
                if attempt + 1 >= REPORT_RESULT_MAX_RETRIES {
                    break;
                }
                interruptible_sleep(delay, cancelled).await;
            }
        }
    }

    Err(last_err.unwrap_or_else(|| tonic::Status::unknown("ReportResult not attempted")))
}

/// Preflight：barrier 之前要做完的所有可失败初始化。
///
/// - write 场景：mkdir 工作根目录；
/// - metadata 场景：build 整棵 layout（同步耗时大）；
/// - read.s3_consistency_check 启用时：构造 `ConsistencyClient`。
///
/// 任一失败 → 返回 Err，由调用方走 ReportReady(error)。
struct Preflight {
    write_root: Option<PathBuf>,
    metadata_layout: Option<Arc<MetadataLayout>>,
    consistency: Option<Arc<ConsistencyClient>>,
}

async fn run_preflight(
    fetch_resp: &pb::FetchJobResponse,
    spec: &BenchSpec,
    mount_point: &Path,
    worker_id: &str,
    job_id: &str,
    _worker_index: u32,
) -> Result<Preflight, String> {
    let mut out = Preflight {
        write_root: None,
        metadata_layout: None,
        consistency: None,
    };

    // write：先 mkdir 工作根
    if let Some(write_cfg) = &spec.write {
        let root = mount_point
            .join(&write_cfg.dir)
            .join(worker_id)
            .join(job_id);
        tokio_fs::create_dir_all(&root)
            .await
            .map_err(|e| format!("write preflight mkdir {root:?}: {e}"))?;
        out.write_root = Some(root);
    }

    // metadata：build layout（含创建目录、files、task_roots），或 read_only
    // 模式下扫描已有目录树。
    if let Some(meta_cfg) = &spec.metadata {
        if meta_cfg.dir_manifest.is_some() {
            return Ok(out);
        }
        let layout = if meta_cfg.read_only {
            metadata_prebuild::scan_existing(mount_point, meta_cfg)
                .await
                .map_err(|e| format!("metadata read_only scan: {e}"))?
        } else {
            metadata_prebuild::build(mount_point, meta_cfg, worker_id, job_id)
                .await
                .map_err(|e| format!("metadata preflight build: {e}"))?
        };
        out.metadata_layout = Some(Arc::new(layout));
    }

    // consistency client：spec 显式启用时必须构造成功，构造失败 = preflight 失败
    if let Some(read_cfg) = &spec.read {
        if read_cfg.s3_consistency_check.is_some() {
            // ConsistencyClient::build 需要 proto 类型；从 FetchJobResponse 里取，
            // 与 spec.read.s3_consistency_check 是同一份数据的两种视图。
            let pb_cfg = fetch_resp
                .spec
                .as_ref()
                .and_then(|s| s.read.as_ref())
                .and_then(|r| r.s3_consistency_check.as_ref())
                .ok_or_else(|| "consistency enabled but proto config missing".to_owned())?;
            let creds = fetch_resp
                .s3_credentials
                .as_ref()
                .ok_or_else(|| "consistency check enabled but s3_credentials missing".to_owned())?;
            let client = ConsistencyClient::build(pb_cfg, creds, worker_id)
                .map_err(|e| format!("consistency preflight: {e}"))?;
            out.consistency = Some(Arc::new(client));
        }
    }

    Ok(out)
}

#[allow(clippy::too_many_arguments)]
async fn run_workload(
    client: MasterServiceClient<Channel>,
    spec: &BenchSpec,
    mount_point: &std::path::Path,
    worker_id: &str,
    job_id: &str,
    worker_index: u32,
    metrics: Arc<MetricsRegistry>,
    abort: Arc<AtomicBool>,
    cancelled: Arc<AtomicBool>,
    unknown_job: Arc<AtomicBool>,
    measure_start: Instant,
    deadline: Instant,
    preflight: Preflight,
    consistency_errors: Arc<StdMutex<Vec<pb::ConsistencyError>>>,
) -> RunnerOutcome {
    let Preflight {
        write_root,
        metadata_layout,
        consistency,
    } = preflight;

    // Mixed runner（spec §6.5 MixedBenchRunner）：read / write / metadata 任意组合
    // 并行执行，共享同一组 metrics / abort / cancelled / measure_start / deadline。
    // per_op key 不冲突：read / write / write_verify / metadata.<op> / consistency。
    let mut handles = tokio::task::JoinSet::<Result<(), String>>::new();
    let any_runner = spec.read.is_some() || spec.write.is_some() || spec.metadata.is_some();

    if let Some(write_cfg) = &spec.write {
        let ctx = Arc::new(WriteContext {
            config: write_cfg.clone(),
            mount_point: mount_point.to_path_buf(),
            worker_id: worker_id.to_owned(),
            job_id: job_id.to_owned(),
            worker_index,
            io_profile: io_profile(spec),
            metrics: metrics.clone(),
            abort: abort.clone(),
            cancelled: cancelled.clone(),
            measure_start,
        });
        let root = write_root.expect("preflight built write root for write spec");
        handles.spawn(async move {
            write_runner::run_with_root(ctx, root, deadline)
                .await
                .map_err(|e| format!("write runner: {e}"))
        });
    }

    if let Some(meta_cfg) = &spec.metadata {
        let ctx = Arc::new(MetadataContext {
            config: meta_cfg.clone(),
            mount_point: mount_point.to_path_buf(),
            worker_id: worker_id.to_owned(),
            job_id: job_id.to_owned(),
            worker_index,
            metrics: metrics.clone(),
            abort: abort.clone(),
            cancelled: cancelled.clone(),
            unknown_job: unknown_job.clone(),
            measure_start,
        });
        if meta_cfg.dir_manifest.is_some() {
            let metadata_client = client.clone();
            handles.spawn(async move {
                metadata_runner::run_manifest_stat(metadata_client, ctx, deadline)
                    .await
                    .map_err(|e| format!("metadata manifest stat runner: {e}"))
            });
        } else {
            let layout =
                metadata_layout.expect("preflight built metadata layout for metadata spec");
            handles.spawn(async move {
                metadata_runner::run_with_layout(ctx, layout, deadline)
                    .await
                    .map_err(|e| format!("metadata runner: {e}"))
            });
        }
    }

    if let Some(read_cfg) = &spec.read {
        let ctx = Arc::new(ReadContext {
            config: read_cfg.clone(),
            mount_point: mount_point.to_path_buf(),
            worker_id: worker_id.to_owned(),
            job_id: job_id.to_owned(),
            worker_index,
            io_profile: io_profile(spec),
            metrics: metrics.clone(),
            abort: abort.clone(),
            cancelled: cancelled.clone(),
            unknown_job: unknown_job.clone(),
            measure_start,
            consistency,
            consistency_errors,
        });
        let read_client = client.clone();
        handles.spawn(async move {
            read_runner::run(read_client, ctx, deadline)
                .await
                .map_err(|e| format!("read runner: {e}"))
        });
    }

    if !any_runner {
        // 没有任何 workload：兜底 sleep 直到 deadline
        let now = Instant::now();
        if deadline > now {
            interruptible_sleep(deadline - now, &cancelled).await;
        }
        return if cancelled.load(Ordering::SeqCst) {
            RunnerOutcome::Cancelled
        } else {
            RunnerOutcome::NotImplemented
        };
    }

    // 等所有 sub-runner 退出；首条错误胜出，立即触发 abort 让其它 runner 收尾。
    let mut first_err: Option<String> = None;
    while let Some(joined) = handles.join_next().await {
        match joined {
            Ok(Ok(())) => {}
            Ok(Err(e)) => {
                abort.store(true, Ordering::SeqCst);
                if first_err.is_none() {
                    first_err = Some(e);
                }
            }
            Err(join_err) => {
                abort.store(true, Ordering::SeqCst);
                if first_err.is_none() {
                    first_err = Some(format!("runner join: {join_err}"));
                }
            }
        }
    }

    if let Some(error) = first_err {
        return RunnerOutcome::Failed { error };
    }
    RunnerOutcome::Ok
}

fn io_profile(spec: &BenchSpec) -> IoProfile {
    IoProfile {
        io_mode: spec.io_mode,
        io_aligned: spec.io_aligned,
        direct_io: spec.direct_io,
        block_size: spec.block_size,
    }
}

#[derive(Debug)]
enum RunnerOutcome {
    Ok,
    Failed {
        error: String,
    },
    Cancelled,
    /// 还没接 runner 的场景（read / metadata），暂以 success=true 返回，让状态机能闭环。
    NotImplemented,
}

/// 起一个独立 task 周期 ReportProgress；返回 JoinHandle 供主流程 abort。
///
/// `report_metrics=false` 时无论 phase 如何，per_op 都是空 map（spec §5.9 要求
/// 非 MEASURING 阶段的 per_op 为空，且 master 也会强制清空；preflight 期间显式
/// 传 false 是双保险）。phase 由 [`PhaseTracker`] 实时反馈。
#[allow(clippy::too_many_arguments)]
fn spawn_progress_task(
    mut client: MasterServiceClient<Channel>,
    job_id: String,
    worker_id: String,
    unknown_job: Arc<AtomicBool>,
    cancelled: Arc<AtomicBool>,
    metrics: Arc<MetricsRegistry>,
    measure_start_ms: i64,
    local_anchor: Instant,
    master_anchor_ms: i64,
    phase: Arc<PhaseTracker>,
    report_metrics: bool,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(PROGRESS_INTERVAL).await;
            if cancelled.load(Ordering::SeqCst) || unknown_job.load(Ordering::SeqCst) {
                break;
            }
            let cur_phase = phase.get();
            // master clock 上的当前时间
            let elapsed = Instant::now().saturating_duration_since(local_anchor);
            let elapsed_master_ms = master_anchor_ms
                .saturating_add(i64::try_from(elapsed.as_millis()).unwrap_or(i64::MAX));
            let elapsed_ms = (elapsed_master_ms - measure_start_ms).max(0);

            // per_op：仅在 MEASURING 阶段且明确允许时上报；warmup 也不进入聚合
            // （master 只对 MEASURING 计入终态）。这里也保持一致。
            let per_op = if report_metrics && cur_phase == LocalPhase::Measuring {
                #[allow(clippy::cast_precision_loss)]
                let elapsed_secs = (elapsed_ms.max(0) as f64) / 1000.0_f64;
                let secs_for_rate = if elapsed_secs > 0.0 {
                    elapsed_secs
                } else {
                    0.0
                };
                metrics.snapshot_pb(secs_for_rate)
            } else {
                std::collections::HashMap::new()
            };

            let req = pb::ReportProgressRequest {
                job_id: job_id.clone(),
                progress: Some(pb::WorkerProgress {
                    worker_id: worker_id.clone(),
                    elapsed_ms,
                    per_op,
                    phase: cur_phase.to_pb().into(),
                }),
            };
            match client.report_progress(req).await {
                Ok(r) => {
                    let r = r.into_inner();
                    if r.unknown_job {
                        // master 不再认识 → 让 hot loop 收尾，并明确标记 unknown，
                        // 主流程据此跳过 ReportResult（spec §6.4 / §6.9）。
                        unknown_job.store(true, Ordering::SeqCst);
                        cancelled.store(true, Ordering::SeqCst);
                        break;
                    }
                    if r.cancelled {
                        cancelled.store(true, Ordering::SeqCst);
                        break;
                    }
                }
                Err(e) => {
                    tracing::warn!(%worker_id, %job_id, error = %e,
                        "ReportProgress RPC failed");
                    // 单次失败不致命，继续下一周期；多次失败由 master staleness 兜底。
                }
            }
        }
    })
}

/// 在到达 measure_start 与 deadline 时把 phase 切换到 Measuring / Cleanup。
fn spawn_phase_advancer(
    phase: Arc<PhaseTracker>,
    measure_start_local: Instant,
    deadline_local: Instant,
    cancelled: Arc<AtomicBool>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        // wait for measure_start
        loop {
            if cancelled.load(Ordering::SeqCst) {
                return;
            }
            let now = Instant::now();
            if now >= measure_start_local {
                break;
            }
            let chunk = (measure_start_local - now).min(SHUTDOWN_POLL_GRANULARITY);
            tokio::time::sleep(chunk).await;
        }
        // 到达 measure_start，仅在仍是 Warmup 时翻转（保持已经被 cleanup/finished 替换的不动）
        if matches!(phase.get(), LocalPhase::Warmup) {
            phase.set(LocalPhase::Measuring);
        }
        // wait for deadline
        loop {
            if cancelled.load(Ordering::SeqCst) {
                return;
            }
            let now = Instant::now();
            if now >= deadline_local {
                break;
            }
            let chunk = (deadline_local - now).min(SHUTDOWN_POLL_GRANULARITY);
            tokio::time::sleep(chunk).await;
        }
        if matches!(phase.get(), LocalPhase::Measuring) {
            phase.set(LocalPhase::Cleanup);
        }
    })
}

/// 解析 + 本地校验 spec；任意失败返回字符串原因（直接上报给 master）。
fn parse_and_validate(proto_spec: Option<pb::BenchSpec>) -> Result<BenchSpec, String> {
    let proto_spec = proto_spec.ok_or_else(|| "FetchJobResponse missing spec".to_owned())?;
    let business_spec =
        BenchSpec::try_from_proto(proto_spec).map_err(|e| format!("spec parse: {e}"))?;
    validate(
        &business_spec,
        &ValidationContext {
            allow_redacted_consistency_credentials: true,
            ..ValidationContext::default()
        },
    )
    .map_err(|report| format!("spec validate: {report}"))?;
    Ok(business_spec)
}

#[derive(Debug)]
enum BarrierOutcome {
    Start(i64),
    Cancelled,
    UnknownJob,
}

async fn wait_for_start_barrier(
    client: &mut MasterServiceClient<Channel>,
    job_id: &str,
    worker_id: &str,
    cancelled: &AtomicBool,
    clock: &mut MasterClockOffset,
) -> BarrierOutcome {
    let mut backoff = ExponentialBackoff::default();
    loop {
        if cancelled.load(Ordering::SeqCst) {
            return BarrierOutcome::Cancelled;
        }
        let req = pb::ReportReadyRequest {
            job_id: job_id.to_owned(),
            worker_id: worker_id.to_owned(),
            error: None,
        };
        let send_at = Instant::now();
        match client.report_ready(req).await {
            Ok(r) => {
                let recv_at = Instant::now();
                let r = r.into_inner();
                // 用最新 master_now_ms 修正 clock offset（spec §6.4）
                clock.refine_with(r.master_now_ms, send_at, recv_at);
                backoff.reset();
                if r.cancelled {
                    return BarrierOutcome::Cancelled;
                }
                if r.unknown_job {
                    return BarrierOutcome::UnknownJob;
                }
                if r.start_at_ms > 0 {
                    return BarrierOutcome::Start(r.start_at_ms);
                }
                interruptible_sleep(READY_POLL_INTERVAL, cancelled).await;
            }
            Err(e) => {
                let delay = backoff.next_delay();
                tracing::warn!(%worker_id, %job_id, error = %e, ?delay,
                    "ReportReady RPC failed, retry after backoff");
                interruptible_sleep(delay, cancelled).await;
            }
        }
    }
}

async fn send_ready_error(
    client: &mut MasterServiceClient<Channel>,
    job_id: &str,
    worker_id: &str,
    reason: &str,
) {
    // 退避重试：确保失败原因送达 master 让 job 转 FAILED（spec §6.4）。
    let mut backoff = ExponentialBackoff::default();
    for attempt in 0..READY_ERROR_MAX_RETRIES {
        let req = pb::ReportReadyRequest {
            job_id: job_id.to_owned(),
            worker_id: worker_id.to_owned(),
            error: Some(reason.to_owned()),
        };
        match client.report_ready(req).await {
            Ok(_) => return,
            Err(e) if attempt + 1 < READY_ERROR_MAX_RETRIES => {
                let delay = backoff.next_delay();
                tracing::warn!(%worker_id, %job_id, error = %e, ?delay, attempt,
                    "ReportReady(error) failed, retry");
                tokio::time::sleep(delay).await;
            }
            Err(e) => {
                tracing::warn!(%worker_id, %job_id, error = %e, attempt,
                    "ReportReady(error) gave up");
                return;
            }
        }
    }
}

fn build_result_no_metrics(
    worker_id: &str,
    success: bool,
    error: Option<&str>,
    effective_duration_ms: i64,
    measure_start_ms: i64,
    measure_end_ms: i64,
) -> pb::WorkerResult {
    pb::WorkerResult {
        worker_id: worker_id.to_owned(),
        per_op: HashMap::new(),
        consistency_errors: vec![],
        success,
        error: error.map(str::to_owned),
        effective_duration_ms,
        measure_start_ms,
        measure_end_ms,
    }
}

/// 把本地 `Instant` 反推回 master clock 上的 unix ms（用于结果时间戳上报）。
fn clock_now_master(offset: &MasterClockOffset, now: Instant) -> i64 {
    let elapsed = now.saturating_duration_since(offset.local_anchor);
    let elapsed_ms = i64::try_from(elapsed.as_millis()).unwrap_or(i64::MAX);
    offset.master_anchor_ms.saturating_add(elapsed_ms)
}

/// 受 cancel flag 控制的 sleep：每 100ms 醒来检查一次，触发后立即返回。
async fn interruptible_sleep(dur: Duration, cancelled: &AtomicBool) {
    let deadline = Instant::now() + dur;
    while !cancelled.load(Ordering::SeqCst) {
        let now = Instant::now();
        if now >= deadline {
            break;
        }
        let chunk = (deadline - now).min(SHUTDOWN_POLL_GRANULARITY);
        tokio::time::sleep(chunk).await;
    }
}

/// 等到 SIGINT / SIGTERM 任一到达。
///
/// 仅在 Linux/类 Unix 上可用，与项目部署目标一致。
pub async fn shutdown_signal() {
    use tokio::signal::unix::{signal, SignalKind};
    let mut sigint = match signal(SignalKind::interrupt()) {
        Ok(s) => s,
        Err(e) => {
            tracing::error!(%e, "failed to register SIGINT handler");
            return;
        }
    };
    let mut sigterm = match signal(SignalKind::terminate()) {
        Ok(s) => s,
        Err(e) => {
            tracing::error!(%e, "failed to register SIGTERM handler");
            return;
        }
    };
    tokio::select! {
        _ = sigint.recv() => tracing::info!("received SIGINT"),
        _ = sigterm.recv() => tracing::info!("received SIGTERM"),
    }
}

//! 读压测 runner（spec §6.6 读压测）。
//!
//! 流水线模型：
//! - 1 个 Fetcher task：循环 `FetchFileBatch` → 把 `FileEntry` 推进 mpmc
//!   bounded channel（`async-channel`）。手动处理 `unknown_job` / `cancelled` /
//!   `has_more=false`。
//! - `read.concurrency` 个 IO task：从同一个 mpmc receiver 各自独立 `recv` 拉
//!   文件，按 `block_size` 流式分块读，记录每块 latency 与 bytes 到
//!   `per_op="read"`。多消费者无需共享锁，并发读不会因 mutex 串行化（spec §6.5）。
//! - fail-fast：任意 IO 错误（EIO / EACCES / 超时 / open 失败 / 路径越界）→
//!   abort，所有 task 退出，runner 返回 Err。
//! - warmup 不计：`Instant::now() >= measure_start` 才记录指标。
//! - rate_limit (throughput) + think_time 在每块读后 / 每文件后 sleep。

use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex as StdMutex};
use std::time::{Duration, Instant};

use cvd_common::metrics::{MetricsRegistry, PerOpMetrics};
use cvd_common::spec::ReadConfig;
use cvd_proto::cvdbench as pb;
use cvd_proto::cvdbench::master_service_client::MasterServiceClient;
use thiserror::Error;
use tonic::transport::Channel;

use crate::backoff::ExponentialBackoff;
use crate::fs_io::{self, IoProfile};
use crate::rate_limit::make_throughput_bucket;
use crate::runner::consistency::{ConsistencyClient, StreamingHash};

const FETCH_BATCH_SIZE: i32 = 1000;
const FETCHER_RETRY_SLEEP: Duration = Duration::from_millis(200);

#[derive(Debug, Error)]
pub enum ReadRunError {
    #[error("{op}: {path:?}: {source}")]
    Io {
        op: &'static str,
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("path {path:?} escapes mount_point {mount:?}")]
    PathEscape { mount: PathBuf, path: PathBuf },
    #[error("consistency check failed for {path:?}: {message}")]
    Consistency { path: PathBuf, message: String },
}

pub struct ReadContext {
    pub config: ReadConfig,
    pub mount_point: PathBuf,
    pub worker_id: String,
    pub job_id: String,
    pub worker_index: u32,
    pub io_profile: IoProfile,
    pub metrics: Arc<MetricsRegistry>,
    pub abort: Arc<AtomicBool>,
    pub cancelled: Arc<AtomicBool>,
    /// master 不再认识本 job 时翻 true；与 cancelled 区分以便主流程跳过 ReportResult。
    pub unknown_job: Arc<AtomicBool>,
    pub measure_start: Instant,
    /// 启用一致性测试时存在；为每个文件做 FS-vs-S3 比对（spec §6.10）。
    pub consistency: Option<Arc<ConsistencyClient>>,
    /// 共享给所有 IO task；首条 ConsistencyError 写入即终止 worker（fail-fast）。
    pub consistency_errors: Arc<StdMutex<Vec<pb::ConsistencyError>>>,
}

pub async fn run(
    client: MasterServiceClient<Channel>,
    ctx: Arc<ReadContext>,
    deadline: Instant,
) -> Result<(), ReadRunError> {
    // 本地缓冲：FETCH_BATCH_SIZE × 4 与 concurrency × 4 取较大值
    let buf_cap = ((FETCH_BATCH_SIZE as usize) * 4).max(ctx.config.concurrency as usize * 4);
    // mpmc：N 个 IO task 直接 recv，无需共享 Mutex（spec §6.5 fix）。
    let (tx, rx) = async_channel::bounded::<pb::FileEntry>(buf_cap);

    // Fetcher task
    let fetcher = {
        let ctx = ctx.clone();
        tokio::spawn(fetcher_task(client, ctx, tx, deadline))
    };

    // IO tasks
    let mut io_handles = Vec::with_capacity(ctx.config.concurrency as usize);
    for task_idx in 0..ctx.config.concurrency {
        let ctx = ctx.clone();
        let rx = rx.clone();
        io_handles.push(tokio::spawn(async move {
            io_task(ctx, task_idx, rx, deadline).await
        }));
    }
    // 关键：释放 run() 自己持有的 rx 副本，让最后一个 IO task 退出时
    // channel 接收端真正 drop，fetcher 的 send 才能 Err 解锁。
    drop(rx);

    // 等 IO tasks
    let mut first_err: Option<ReadRunError> = None;
    for h in io_handles {
        match h.await {
            Ok(Ok(())) => {}
            Ok(Err(err)) => {
                ctx.abort.store(true, Ordering::SeqCst);
                if first_err.is_none() {
                    first_err = Some(err);
                }
            }
            Err(join_err) => {
                ctx.abort.store(true, Ordering::SeqCst);
                tracing::warn!(error = %join_err, "read IO task join failed");
            }
        }
    }

    // Fetcher 也等下；channel 已 close（IO 全退）或 deadline 触发后会退出
    let _ = fetcher.await;

    if let Some(err) = first_err {
        return Err(err);
    }
    Ok(())
}

async fn fetcher_task(
    mut client: MasterServiceClient<Channel>,
    ctx: Arc<ReadContext>,
    tx: async_channel::Sender<pb::FileEntry>,
    deadline: Instant,
) {
    let mut backoff = ExponentialBackoff::default();
    loop {
        if Instant::now() >= deadline {
            break;
        }
        if ctx.abort.load(Ordering::SeqCst) || ctx.cancelled.load(Ordering::SeqCst) {
            break;
        }

        let resp = match client
            .fetch_file_batch(pb::FetchFileBatchRequest {
                job_id: ctx.job_id.clone(),
                worker_id: ctx.worker_id.clone(),
                batch_size: FETCH_BATCH_SIZE,
            })
            .await
        {
            Ok(r) => {
                backoff.reset();
                r.into_inner()
            }
            Err(status) => {
                let delay = backoff.next_delay();
                tracing::warn!(code = ?status.code(), msg = %status.message(), ?delay,
                    "FetchFileBatch RPC failed; retry after backoff");
                tokio::time::sleep(delay).await;
                continue;
            }
        };

        if resp.unknown_job {
            // master 不再认识 → 让 worker 放弃，明确标记 unknown_job 让主流程
            // 跳过 ReportResult（spec §6.4 / §6.9）。
            tracing::info!(worker_id = %ctx.worker_id, job_id = %ctx.job_id,
                "FetchFileBatch returned unknown_job; abandoning");
            ctx.unknown_job.store(true, Ordering::SeqCst);
            ctx.cancelled.store(true, Ordering::SeqCst);
            ctx.abort.store(true, Ordering::SeqCst);
            return;
        }
        if resp.cancelled {
            tracing::info!(worker_id = %ctx.worker_id, job_id = %ctx.job_id,
                "FetchFileBatch returned cancelled");
            ctx.cancelled.store(true, Ordering::SeqCst);
            return;
        }

        let any_files = !resp.files.is_empty();
        for entry in resp.files {
            if ctx.abort.load(Ordering::SeqCst) || ctx.cancelled.load(Ordering::SeqCst) {
                return;
            }
            if tx.send(entry).await.is_err() {
                // 所有消费者已退出
                return;
            }
        }

        if !resp.has_more {
            return; // 关闭 channel（drop tx），消费者收到 None 后退出
        }
        if !any_files {
            // master 队列暂空但 manifest 仍在生产 → 短暂 sleep 重试
            tokio::time::sleep(FETCHER_RETRY_SLEEP).await;
        }
    }
}

async fn io_task(
    ctx: Arc<ReadContext>,
    _task_idx: u32,
    rx: async_channel::Receiver<pb::FileEntry>,
    deadline: Instant,
) -> Result<(), ReadRunError> {
    let metrics = ctx.metrics.op("read");
    let metrics_open = ctx.metrics.op("read.open");
    let metrics_close = ctx.metrics.op("read.close");
    let metrics_consistency = ctx
        .consistency
        .as_ref()
        .map(|_| ctx.metrics.op("consistency"));
    let throughput_bucket = make_throughput_bucket(ctx.config.rate_limit);

    loop {
        if Instant::now() >= deadline {
            break;
        }
        if ctx.abort.load(Ordering::SeqCst) || ctx.cancelled.load(Ordering::SeqCst) {
            break;
        }

        let entry = match rx.recv().await {
            Ok(e) => e,
            Err(_) => break, // channel closed
        };

        let abs_path = ctx.mount_point.join(&entry.fs_path);
        // 防御性：拼接后路径必须仍在 mount_point 下（manifest reader 已校验过相对，
        // 这里再确认一次，避免 symlink/绝对路径漏网）
        if !abs_path.starts_with(&ctx.mount_point) {
            metrics.record_error();
            ctx.abort.store(true, Ordering::SeqCst);
            return Err(ReadRunError::PathEscape {
                mount: ctx.mount_point.clone(),
                path: abs_path,
            });
        }

        // 如果开了一致性测试，需要边读边累加 sha256；否则不计算
        let mut hasher = ctx.consistency.as_ref().map(|_| StreamingHash::new());

        if let Err(err) = read_file_recording_blocks(
            &abs_path,
            ctx.io_profile,
            &metrics,
            &metrics_open,
            &metrics_close,
            ctx.measure_start,
            throughput_bucket.as_ref(),
            &ctx.abort,
            &ctx.cancelled,
            hasher.as_mut(),
        )
        .await
        {
            ctx.abort.store(true, Ordering::SeqCst);
            let op = match err.phase {
                fs_io::ReadStreamPhase::Open => "read.open",
                fs_io::ReadStreamPhase::Read => "read",
                fs_io::ReadStreamPhase::Close => "read.close",
            };
            return Err(ReadRunError::Io {
                op,
                path: abs_path,
                source: err.source,
            });
        }

        // 关键：read_file_recording_blocks 在 abort/cancelled 翻转时会提前 break
        // 并返回 Ok(())，`hasher` 内只覆盖到部分字节。此时若继续做一致性比对，
        // 一定会 CET_SIZE_MISMATCH（fs_size < 实际文件大小），是个假阳性。spec
        // §6.10 不要求中断态做一致性核对，直接跳过。
        if ctx.abort.load(Ordering::SeqCst) || ctx.cancelled.load(Ordering::SeqCst) {
            break;
        }

        // 一致性比对（spec §6.10）
        if let (Some(client), Some(hasher), Some(metric_v)) = (
            ctx.consistency.as_ref(),
            hasher,
            metrics_consistency.as_ref(),
        ) {
            let (fs_hash, fs_size) = hasher.finalize();
            let in_measure = Instant::now() >= ctx.measure_start;
            let start = Instant::now();
            match client.check(&entry, fs_size, fs_hash).await {
                Ok(s3_size) => {
                    if in_measure {
                        metric_v.record_op(elapsed_us_saturating(start.elapsed()), s3_size);
                    }
                }
                Err(consistency_err) => {
                    metric_v.record_error();
                    // fail-fast：首条胜出，记录后立即翻 abort
                    {
                        let mut errs = ctx.consistency_errors.lock().expect("ce mutex");
                        if errs.is_empty() {
                            errs.push(consistency_err.clone());
                        }
                    }
                    ctx.abort.store(true, Ordering::SeqCst);
                    return Err(ReadRunError::Consistency {
                        path: abs_path,
                        message: consistency_err.message,
                    });
                }
            }
        }

        if let Some(t) = ctx.config.think_time {
            tokio::time::sleep(t).await;
        }
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn read_file_recording_blocks(
    path: &std::path::Path,
    io_profile: IoProfile,
    metrics: &PerOpMetrics,
    metrics_open: &PerOpMetrics,
    metrics_close: &PerOpMetrics,
    measure_start: Instant,
    throughput_bucket: Option<&crate::rate_limit::TokenBucket>,
    abort: &AtomicBool,
    cancelled: &AtomicBool,
    mut hash: Option<&mut StreamingHash>,
) -> Result<(), fs_io::ReadStreamError> {
    // 流式分块（spec §6.5）：用 channel 把每块结果送给消费循环，避免一次把整文件
    // 读进内存。direct_io 走 spawn_blocking + libc pread；其余走 tokio::fs。
    let (tx, mut rx) =
        tokio::sync::mpsc::channel::<Result<fs_io::ReadStreamEvent, fs_io::ReadStreamError>>(4);
    let path_buf = path.to_path_buf();
    let producer =
        tokio::spawn(async move { fs_io::stream_read_file(&path_buf, io_profile, tx).await });

    let mut io_err: Option<fs_io::ReadStreamError> = None;
    while let Some(event_result) = rx.recv().await {
        if abort.load(Ordering::SeqCst) || cancelled.load(Ordering::SeqCst) {
            break;
        }
        let event = match event_result {
            Ok(c) => c,
            Err(e) => {
                match e.phase {
                    fs_io::ReadStreamPhase::Open => metrics_open.record_error(),
                    fs_io::ReadStreamPhase::Read => metrics.record_error(),
                    fs_io::ReadStreamPhase::Close => metrics_close.record_error(),
                }
                io_err = Some(e);
                break;
            }
        };
        let in_measure = Instant::now() >= measure_start;
        match event {
            fs_io::ReadStreamEvent::Open { latency } => {
                if in_measure {
                    metrics_open.record_op(elapsed_us_saturating(latency), 0);
                }
            }
            fs_io::ReadStreamEvent::Chunk(chunk) => {
                if let Some(h) = hash.as_deref_mut() {
                    h.update(&chunk.bytes);
                }
                if in_measure {
                    metrics.record_op(
                        elapsed_us_saturating(chunk.latency),
                        chunk.bytes.len() as u64,
                    );
                }
                if let Some(rl) = throughput_bucket {
                    rl.acquire(chunk.bytes.len() as u64).await;
                }
            }
            fs_io::ReadStreamEvent::Close { latency } => {
                if in_measure {
                    metrics_close.record_op(elapsed_us_saturating(latency), 0);
                }
            }
        }
    }

    // 关闭消费端会让 producer 的 send 失败，自然退出。
    drop(rx);
    let _ = producer.await; // 不要让 producer 错误覆盖 io_err（producer 内部已经把错误发回）
    if let Some(e) = io_err {
        return Err(e);
    }
    Ok(())
}

fn elapsed_us_saturating(d: Duration) -> u64 {
    u64::try_from(d.as_micros()).unwrap_or(u64::MAX)
}

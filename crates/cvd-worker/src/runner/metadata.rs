//! 元数据压测 runner（spec §6.6 元数据）。
//!
//! 流程：
//! 1. 调用 [`crate::prebuild::metadata::build`] 预建 layout；
//! 2. 起 `metadata.concurrency` 个并发 task，循环到 deadline / cancel / abort；
//! 3. 每次循环从 `metadata.ops` 随机抽取一个 op：
//!    - `stat` / `open`：在预建 `files` 中随机选一个目标；
//!    - `readdir`：在预建 `directories` 中随机选一个目标；
//!    - `create` / `mkdir`：在该 task 的私有 `task_root` 下用单调计数生成新名，
//!      避免并发 race（spec §6.6 中 fail-fast 不允许 ENOENT/EEXIST）；
//! 4. 任意 op errno 非零 → 翻 abort，所有 task 退出，runner 返回 Err；
//! 5. 仅 `Instant::now() >= measure_start` 才记录 per_op；warmup 不计入。

use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use cvd_common::metrics::MetricsRegistry;
use cvd_common::spec::{MetadataConfig, MetadataOp};
use cvd_proto::cvdbench as pb;
use cvd_proto::cvdbench::master_service_client::MasterServiceClient;
use rand::rngs::StdRng;
use rand::seq::SliceRandom;
use rand::{Rng, SeedableRng};
use sha2::{Digest, Sha256};
use thiserror::Error;
use tokio::fs;
use tonic::transport::Channel;

use crate::backoff::ExponentialBackoff;
use crate::prebuild::metadata::{self as prebuild, BuildError, Layout};
use crate::rate_limit::make_iops_bucket;

const FETCH_BATCH_SIZE: i32 = 1000;
const FETCHER_RETRY_SLEEP: Duration = Duration::from_millis(200);

#[derive(Debug, Error)]
pub enum MetadataRunError {
    #[error("prebuild: {0}")]
    Prebuild(#[from] BuildError),
    #[error("op {op:?}: {path:?}: {source}")]
    Io {
        op: MetadataOp,
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("metadata layout has no {kind} (depth/width/files_per_dir produced empty set)")]
    EmptyLayout { kind: &'static str },
    #[error("path {path:?} escapes mount_point {mount:?}")]
    PathEscape { mount: PathBuf, path: PathBuf },
}

pub struct MetadataContext {
    pub config: MetadataConfig,
    pub mount_point: PathBuf,
    pub worker_id: String,
    pub job_id: String,
    pub worker_index: u32,
    pub metrics: Arc<MetricsRegistry>,
    pub abort: Arc<AtomicBool>,
    pub cancelled: Arc<AtomicBool>,
    pub unknown_job: Arc<AtomicBool>,
    pub measure_start: Instant,
}

pub async fn run(ctx: Arc<MetadataContext>, deadline: Instant) -> Result<(), MetadataRunError> {
    // 1) 预建 layout
    let layout =
        prebuild::build(&ctx.mount_point, &ctx.config, &ctx.worker_id, &ctx.job_id).await?;
    run_with_layout(ctx, Arc::new(layout), deadline).await
}

/// 已预建好 layout 的入口：仅跑 hot-loop。供 lifecycle 提前在 barrier 之前
/// build 出 layout，避免起跑后再做长耗时同步建树（spec §6.4 / §6.6 preflight）。
pub async fn run_with_layout(
    ctx: Arc<MetadataContext>,
    layout: Arc<Layout>,
    deadline: Instant,
) -> Result<(), MetadataRunError> {
    // 防御：concurrency 与 task_roots 必须一致
    debug_assert_eq!(layout.task_roots.len(), ctx.config.concurrency as usize);

    // 2) 启动 N 个 hot-loop task
    let mut handles = Vec::with_capacity(ctx.config.concurrency as usize);
    for task_idx in 0..ctx.config.concurrency {
        let ctx = ctx.clone();
        let layout = layout.clone();
        handles.push(tokio::spawn(async move {
            run_task(ctx, task_idx, layout, deadline).await
        }));
    }

    let mut first_err: Option<MetadataRunError> = None;
    for h in handles {
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
                tracing::warn!(error = %join_err, "metadata task join failed");
            }
        }
    }

    // 没有 cleanup 字段（spec 元数据 layout 跨 job 保留），直接返回
    if let Some(e) = first_err {
        return Err(e);
    }
    Ok(())
}

/// Master 扫描目录并通过 FetchFileBatch 分发文件；worker 仅对每个文件执行 stat。
pub async fn run_manifest_stat(
    client: MasterServiceClient<Channel>,
    ctx: Arc<MetadataContext>,
    deadline: Instant,
) -> Result<(), MetadataRunError> {
    let buf_cap = ((FETCH_BATCH_SIZE as usize) * 4).max(ctx.config.concurrency as usize * 4);
    let (tx, rx) = async_channel::bounded::<pb::FileEntry>(buf_cap);
    let fetcher = {
        let ctx = ctx.clone();
        tokio::spawn(fetcher_task(client, ctx, tx, deadline))
    };

    let mut handles = Vec::with_capacity(ctx.config.concurrency as usize);
    for _task_idx in 0..ctx.config.concurrency {
        let ctx = ctx.clone();
        let rx = rx.clone();
        handles.push(tokio::spawn(
            async move { stat_task(ctx, rx, deadline).await },
        ));
    }
    drop(rx);

    let mut first_err = None;
    for h in handles {
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
                tracing::warn!(error = %join_err, "metadata manifest stat task join failed");
            }
        }
    }
    let _ = fetcher.await;

    if let Some(err) = first_err {
        return Err(err);
    }
    Ok(())
}

async fn fetcher_task(
    mut client: MasterServiceClient<Channel>,
    ctx: Arc<MetadataContext>,
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
            ctx.unknown_job.store(true, Ordering::SeqCst);
            ctx.cancelled.store(true, Ordering::SeqCst);
            ctx.abort.store(true, Ordering::SeqCst);
            return;
        }
        if resp.cancelled {
            ctx.cancelled.store(true, Ordering::SeqCst);
            return;
        }

        let any_files = !resp.files.is_empty();
        for entry in resp.files {
            if ctx.abort.load(Ordering::SeqCst) || ctx.cancelled.load(Ordering::SeqCst) {
                return;
            }
            if tx.send(entry).await.is_err() {
                return;
            }
        }

        if !resp.has_more {
            return;
        }
        if !any_files {
            tokio::time::sleep(FETCHER_RETRY_SLEEP).await;
        }
    }
}

async fn stat_task(
    ctx: Arc<MetadataContext>,
    rx: async_channel::Receiver<pb::FileEntry>,
    deadline: Instant,
) -> Result<(), MetadataRunError> {
    let metrics = ctx.metrics.op("metadata.stat");
    let iops_bucket = make_iops_bucket(ctx.config.rate_limit);

    loop {
        if Instant::now() >= deadline {
            break;
        }
        if ctx.abort.load(Ordering::SeqCst) || ctx.cancelled.load(Ordering::SeqCst) {
            break;
        }

        let entry = match rx.recv().await {
            Ok(e) => e,
            Err(_) => break,
        };
        if let Some(rl) = &iops_bucket {
            rl.acquire(1).await;
        }

        let abs_path = ctx.mount_point.join(&entry.fs_path);
        if !abs_path.starts_with(&ctx.mount_point) {
            metrics.record_error();
            ctx.abort.store(true, Ordering::SeqCst);
            return Err(MetadataRunError::PathEscape {
                mount: ctx.mount_point.clone(),
                path: abs_path,
            });
        }

        let in_measure = Instant::now() >= ctx.measure_start;
        let start = Instant::now();
        match fs::metadata(&abs_path).await {
            Ok(_) => {
                if in_measure {
                    metrics.record_op(elapsed_us_saturating(start.elapsed()), 0);
                }
            }
            Err(source) => {
                metrics.record_error();
                ctx.abort.store(true, Ordering::SeqCst);
                return Err(MetadataRunError::Io {
                    op: MetadataOp::Stat,
                    path: abs_path,
                    source,
                });
            }
        }

        if let Some(t) = ctx.config.think_time {
            tokio::time::sleep(t).await;
        }
    }
    Ok(())
}

async fn run_task(
    ctx: Arc<MetadataContext>,
    task_idx: u32,
    layout: Arc<Layout>,
    deadline: Instant,
) -> Result<(), MetadataRunError> {
    let task_root = layout
        .task_roots
        .get(task_idx as usize)
        .cloned()
        .ok_or(MetadataRunError::EmptyLayout { kind: "task_roots" })?;

    let metrics_create = ctx.metrics.op("metadata.create");
    let metrics_mkdir = ctx.metrics.op("metadata.mkdir");
    let metrics_stat = ctx.metrics.op("metadata.stat");
    let metrics_open = ctx.metrics.op("metadata.open");
    let metrics_readdir = ctx.metrics.op("metadata.readdir");

    let iops_bucket = make_iops_bucket(ctx.config.rate_limit);

    let mut seed = [0u8; 32];
    {
        let mut hasher = Sha256::new();
        hasher.update(ctx.worker_id.as_bytes());
        hasher.update(ctx.job_id.as_bytes());
        hasher.update(b"metadata");
        hasher.update(task_idx.to_le_bytes());
        seed.copy_from_slice(&hasher.finalize());
    }
    let mut rng = StdRng::from_seed(seed);

    let mut create_seq: u64 = 0;
    let mut mkdir_seq: u64 = 0;

    loop {
        if Instant::now() >= deadline {
            break;
        }
        if ctx.abort.load(Ordering::SeqCst) || ctx.cancelled.load(Ordering::SeqCst) {
            break;
        }

        let op = match ctx.config.ops.choose(&mut rng) {
            Some(o) => *o,
            None => {
                return Err(MetadataRunError::EmptyLayout { kind: "ops" });
            }
        };

        if let Some(rl) = &iops_bucket {
            rl.acquire(1).await;
        }

        let in_measure = Instant::now() >= ctx.measure_start;
        let start = Instant::now();
        let outcome = match op {
            MetadataOp::Stat => {
                let target = pick_random(&layout.files, &mut rng)?;
                fs::metadata(&target)
                    .await
                    .map(|_| ())
                    .map_err(|source| MetadataRunError::Io {
                        op,
                        path: target,
                        source,
                    })
            }
            MetadataOp::Open => {
                let target = pick_random(&layout.files, &mut rng)?;
                match fs::File::open(&target).await {
                    Ok(_) => Ok(()),
                    Err(source) => Err(MetadataRunError::Io {
                        op,
                        path: target,
                        source,
                    }),
                }
            }
            MetadataOp::Readdir => {
                let target = pick_random(&layout.directories, &mut rng)?;
                drain_readdir(&target)
                    .await
                    .map_err(|source| MetadataRunError::Io {
                        op,
                        path: target,
                        source,
                    })
            }
            MetadataOp::Create => {
                create_seq += 1;
                let target = task_root.join(format!("c_{create_seq:08}.dat"));
                match fs::File::create(&target).await {
                    Ok(_) => Ok(()),
                    Err(source) => Err(MetadataRunError::Io {
                        op,
                        path: target,
                        source,
                    }),
                }
            }
            MetadataOp::Mkdir => {
                mkdir_seq += 1;
                let target = task_root.join(format!("m_{mkdir_seq:08}"));
                fs::create_dir(&target)
                    .await
                    .map_err(|source| MetadataRunError::Io {
                        op,
                        path: target,
                        source,
                    })
            }
        };
        let elapsed = start.elapsed();

        let metric = match op {
            MetadataOp::Stat => &metrics_stat,
            MetadataOp::Open => &metrics_open,
            MetadataOp::Readdir => &metrics_readdir,
            MetadataOp::Create => &metrics_create,
            MetadataOp::Mkdir => &metrics_mkdir,
        };
        match outcome {
            Ok(()) => {
                if in_measure {
                    metric.record_op(elapsed_us_saturating(elapsed), 0);
                }
            }
            Err(err) => {
                metric.record_error();
                // fail-fast：先翻 abort，再返回 Err；避免兄弟 task 在 join 之前
                // 还在跑、又踩到同一种错误（spec §6.7）。
                ctx.abort.store(true, Ordering::SeqCst);
                return Err(err);
            }
        }

        if let Some(t) = ctx.config.think_time {
            tokio::time::sleep(t).await;
        }
    }
    Ok(())
}

async fn drain_readdir(path: &std::path::Path) -> std::io::Result<()> {
    let mut iter = fs::read_dir(path).await?;
    while iter.next_entry().await?.is_some() {
        // drain，模拟用户态遍历完整目录
    }
    Ok(())
}

fn pick_random<R: Rng + ?Sized>(
    list: &[PathBuf],
    rng: &mut R,
) -> Result<PathBuf, MetadataRunError> {
    if list.is_empty() {
        return Err(MetadataRunError::EmptyLayout { kind: "paths" });
    }
    let idx = rng.gen_range(0..list.len());
    Ok(list[idx].clone())
}

fn elapsed_us_saturating(d: Duration) -> u64 {
    u64::try_from(d.as_micros()).unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;
    use cvd_common::spec::MetadataOp;
    use std::sync::atomic::AtomicBool;
    use tempfile::tempdir;

    fn ctx_for(
        tmp: &std::path::Path,
        ops: Vec<MetadataOp>,
        concurrency: u32,
        measure_start: Instant,
    ) -> Arc<MetadataContext> {
        Arc::new(MetadataContext {
            config: MetadataConfig {
                concurrency,
                dir: "bench/meta".into(),
                dir_manifest: None,
                ops,
                read_only: false,
                read_only_scan_limit: 0,
                depth: 2,
                width: 2,
                files_per_dir: 2,
                layout_concurrency: 4,
                think_time: None,
                rate_limit: None,
            },
            mount_point: tmp.to_path_buf(),
            worker_id: "host-1-aaaa1111".into(),
            job_id: "job-test".into(),
            worker_index: 0,
            metrics: Arc::new(MetricsRegistry::new()),
            abort: Arc::new(AtomicBool::new(false)),
            cancelled: Arc::new(AtomicBool::new(false)),
            unknown_job: Arc::new(AtomicBool::new(false)),
            measure_start,
        })
    }

    #[tokio::test]
    async fn runs_all_op_kinds_and_records_metrics() {
        let tmp = tempdir().unwrap();
        let ctx = ctx_for(
            tmp.path(),
            vec![
                MetadataOp::Stat,
                MetadataOp::Open,
                MetadataOp::Readdir,
                MetadataOp::Create,
                MetadataOp::Mkdir,
            ],
            2,
            Instant::now(),
        );
        let deadline = Instant::now() + Duration::from_millis(200);
        run(ctx.clone(), deadline).await.unwrap();

        // 5 个 op 的 metric 都应有 ops > 0（200ms 应当跑出每种至少 1 次）
        for op in [
            "metadata.stat",
            "metadata.open",
            "metadata.readdir",
            "metadata.create",
            "metadata.mkdir",
        ] {
            let (ops, _, errs) = ctx.metrics.op(op).totals();
            assert!(ops > 0, "expected at least one {op} op in 200ms, got {ops}");
            assert_eq!(errs, 0);
        }
    }

    #[tokio::test]
    async fn create_is_per_task_and_avoids_collisions() {
        let tmp = tempdir().unwrap();
        let ctx = ctx_for(tmp.path(), vec![MetadataOp::Create], 4, Instant::now());
        let deadline = Instant::now() + Duration::from_millis(150);
        run(ctx.clone(), deadline).await.unwrap();
        let (ops, _, errs) = ctx.metrics.op("metadata.create").totals();
        assert!(ops >= 4, "expected at least 4 create ops, got {ops}");
        assert_eq!(errs, 0);
        // 各 task 私有目录里都应有 create 出来的文件
        let tasks_root = tmp
            .path()
            .join("bench/meta/host-1-aaaa1111/job-test/_tasks");
        let mut found_files = 0;
        let mut iter = tokio::fs::read_dir(&tasks_root).await.unwrap();
        while let Some(entry) = iter.next_entry().await.unwrap() {
            let mut sub = tokio::fs::read_dir(entry.path()).await.unwrap();
            while sub.next_entry().await.unwrap().is_some() {
                found_files += 1;
            }
        }
        assert!(found_files >= 4);
    }

    #[tokio::test]
    async fn warmup_window_not_counted() {
        let tmp = tempdir().unwrap();
        // measure_start 远在未来 → 整个 run 都算 warmup，不记录 metric
        let ctx = ctx_for(
            tmp.path(),
            vec![MetadataOp::Stat],
            1,
            Instant::now() + Duration::from_secs(60),
        );
        let deadline = Instant::now() + Duration::from_millis(150);
        run(ctx.clone(), deadline).await.unwrap();
        let (ops, _, _) = ctx.metrics.op("metadata.stat").totals();
        assert_eq!(ops, 0, "warmup ops should not be counted");
    }

    #[tokio::test]
    async fn cancel_short_circuits_runner() {
        let tmp = tempdir().unwrap();
        let ctx = ctx_for(tmp.path(), vec![MetadataOp::Stat], 2, Instant::now());
        ctx.cancelled.store(true, Ordering::SeqCst);
        let deadline = Instant::now() + Duration::from_secs(5);
        let start = Instant::now();
        run(ctx.clone(), deadline).await.unwrap();
        assert!(start.elapsed() < Duration::from_millis(500));
    }
}

//! 写压测 runner（spec §6.6 写压测）。
//!
//! 范围：
//! - 在 `mount_point/{write.dir}/{worker_id}/{job_id}/` 下创建工作集；
//! - 启动 `concurrency` 个并发 task，每个独立 RNG，循环写入；
//! - 写完可选 `fsync` 和 `verify_after_write` (sha256 读回校验)；
//! - per_op 指标记录到 [`MetricsRegistry`]：`write` / `write_verify` 两个 key；
//! - rate_limit 走 token-bucket（throughput 模式）；think_time 用 `tokio::sleep`；
//! - fail-fast：任一 op 出错立刻翻 abort flag，全员退出，runner 返回 Err；
//! - cleanup=true 时递归删除工作集，并防御性校验路径仍位于 mount_point/dir 下。

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use cvd_common::metrics::MetricsRegistry;
use cvd_common::spec::{SizeDistribution, WriteConfig, WriteSize};
use rand::rngs::StdRng;
use rand::{Rng, RngCore, SeedableRng};
use sha2::{Digest, Sha256};
use thiserror::Error;
use tokio::fs;
use tokio::io::AsyncReadExt;

use crate::fs_io::{self, IoProfile};
use crate::rate_limit::make_throughput_bucket;

const SUBDIR_MAX_DEPTH: u32 = 5;
const SUBDIR_SEGMENT_LEN: usize = 3; // 16^3 = 4096，足够散列开

#[derive(Debug, Error)]
pub enum WriteRunError {
    #[error("{op}: {path:?}: {source}")]
    Io {
        op: &'static str,
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("verify_after_write hash mismatch for {path:?}")]
    VerifyMismatch { path: PathBuf },
    #[error("cleanup target {path:?} escapes mount_point {mount:?}")]
    CleanupEscape { mount: PathBuf, path: PathBuf },
}

/// 单次 runner 调用的全部上下文；用 Arc 共享给各并发 task。
pub struct WriteContext {
    pub config: WriteConfig,
    pub mount_point: PathBuf,
    pub worker_id: String,
    pub job_id: String,
    pub worker_index: u32,
    pub io_profile: IoProfile,
    pub metrics: Arc<MetricsRegistry>,
    /// fail-fast：任一 task 出错置 true，所有 task 在循环顶部检测后退出。
    pub abort: Arc<AtomicBool>,
    /// 全局 cancel：master 通知 cancelled 或 SIGTERM。
    pub cancelled: Arc<AtomicBool>,
    /// 测量窗口起点（本地 monotonic）；warmup 期间不记录 metrics。
    pub measure_start: Instant,
}

pub async fn run(ctx: Arc<WriteContext>, deadline: Instant) -> Result<(), WriteRunError> {
    // 1) 预建根目录 `<mount_point>/<write.dir>/<worker_id>/<job_id>/`
    let root = work_root(&ctx);
    fs::create_dir_all(&root)
        .await
        .map_err(|source| WriteRunError::Io {
            op: "mkdir_root",
            path: root.clone(),
            source,
        })?;
    run_with_root(ctx, root, deadline).await
}

/// 已预建好 root 的入口：仅跑 hot-loop + cleanup。供 lifecycle 提前在 barrier
/// 前做 mkdir_root，避免起跑后才暴露挂载/权限问题（spec §6.4 preflight）。
pub async fn run_with_root(
    ctx: Arc<WriteContext>,
    root: PathBuf,
    deadline: Instant,
) -> Result<(), WriteRunError> {
    // 2) 启动 concurrency 个并发 task
    let mut handles = Vec::with_capacity(ctx.config.concurrency as usize);
    for task_idx in 0..ctx.config.concurrency {
        let ctx = ctx.clone();
        let root = root.clone();
        handles.push(tokio::spawn(async move {
            run_task(ctx, task_idx, root, deadline).await
        }));
    }

    // 3) 等所有 task 退出；首条错误胜出
    let mut first_error: Option<WriteRunError> = None;
    for h in handles {
        match h.await {
            Ok(Ok(())) => {}
            Ok(Err(err)) => {
                ctx.abort.store(true, Ordering::SeqCst);
                if first_error.is_none() {
                    first_error = Some(err);
                }
            }
            Err(join_err) => {
                ctx.abort.store(true, Ordering::SeqCst);
                tracing::warn!(error = %join_err, "write task join failed");
            }
        }
    }

    // 4) cleanup（即使出错也尝试，避免遗留垃圾文件）。cleanup 失败不能把成功
    //    的跑动翻成 failure：metric 已经汇总，runner 业务结果是「成功」。仅在
    //    log 中以 warn 留痕，不写回 first_error。
    if ctx.config.cleanup {
        if let Err(err) = cleanup_root(&ctx.mount_point, &root).await {
            tracing::warn!(?err, "cleanup failed; leaving root in place");
        }
    }

    if let Some(err) = first_error {
        return Err(err);
    }
    Ok(())
}

pub fn work_root(ctx: &WriteContext) -> PathBuf {
    ctx.mount_point
        .join(&ctx.config.dir)
        .join(&ctx.worker_id)
        .join(&ctx.job_id)
}

async fn cleanup_root(mount: &Path, root: &Path) -> Result<(), WriteRunError> {
    if !root.starts_with(mount) {
        return Err(WriteRunError::CleanupEscape {
            mount: mount.to_path_buf(),
            path: root.to_path_buf(),
        });
    }
    // root 自身必须是真实目录，不能是 symlink（spec §6.6 不跟随 symlink）。
    let meta = match fs::symlink_metadata(root).await {
        Ok(m) => m,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(source) => {
            return Err(WriteRunError::Io {
                op: "cleanup_stat",
                path: root.to_path_buf(),
                source,
            });
        }
    };
    if meta.file_type().is_symlink() {
        return Err(WriteRunError::CleanupEscape {
            mount: mount.to_path_buf(),
            path: root.to_path_buf(),
        });
    }
    if !meta.is_dir() {
        return Err(WriteRunError::Io {
            op: "cleanup",
            path: root.to_path_buf(),
            source: std::io::Error::other("cleanup target is not a directory"),
        });
    }
    remove_dir_all_no_follow(root).await
}

/// 不跟随 symlink 的递归删除：遇到 symlink 用 `remove_file` 删链接本身，遇到目录
/// 递归。`tokio::fs::remove_dir_all` 跟随 symlink，会越权清理 mount 外的内容，
/// 因此手写一份 nofollow 实现（spec §6.6）。
async fn remove_dir_all_no_follow(root: &Path) -> Result<(), WriteRunError> {
    // 用栈避免递归 `Box::pin` 复杂度
    let mut stack = vec![(root.to_path_buf(), false)];
    while let Some((dir, visited)) = stack.pop() {
        if visited {
            // 子项删完，最后删本目录
            if let Err(source) = fs::remove_dir(&dir).await {
                if source.kind() != std::io::ErrorKind::NotFound {
                    return Err(WriteRunError::Io {
                        op: "rmdir",
                        path: dir,
                        source,
                    });
                }
            }
            continue;
        }
        // 把自己再压回去，等子项处理完再删
        stack.push((dir.clone(), true));
        let mut entries = match fs::read_dir(&dir).await {
            Ok(e) => e,
            Err(source) if source.kind() == std::io::ErrorKind::NotFound => continue,
            Err(source) => {
                return Err(WriteRunError::Io {
                    op: "read_dir",
                    path: dir,
                    source,
                });
            }
        };
        while let Some(entry) = entries
            .next_entry()
            .await
            .map_err(|source| WriteRunError::Io {
                op: "read_dir",
                path: dir.clone(),
                source,
            })?
        {
            let path = entry.path();
            // 用 symlink_metadata 不跟随
            let meta = match fs::symlink_metadata(&path).await {
                Ok(m) => m,
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
                Err(source) => {
                    return Err(WriteRunError::Io {
                        op: "stat",
                        path,
                        source,
                    });
                }
            };
            if meta.file_type().is_dir() {
                stack.push((path, false));
            } else {
                // file / symlink / fifo / socket：用 remove_file 删（对 symlink 仅删链接本身）
                if let Err(source) = fs::remove_file(&path).await {
                    if source.kind() != std::io::ErrorKind::NotFound {
                        return Err(WriteRunError::Io {
                            op: "remove",
                            path,
                            source,
                        });
                    }
                }
            }
        }
    }
    Ok(())
}

async fn run_task(
    ctx: Arc<WriteContext>,
    task_idx: u32,
    root: PathBuf,
    deadline: Instant,
) -> Result<(), WriteRunError> {
    let metrics_write = ctx.metrics.op("write");
    let metrics_verify = if ctx.config.verify_after_write {
        Some(ctx.metrics.op("write_verify"))
    } else {
        None
    };

    let throughput_bucket = make_throughput_bucket(ctx.config.rate_limit);

    // RNG seed = worker_id 字节 hash xor task_idx；不同 worker / 不同 task 互不重叠
    let mut seed_seed = [0u8; 32];
    {
        let mut hasher = Sha256::new();
        hasher.update(ctx.worker_id.as_bytes());
        hasher.update(ctx.job_id.as_bytes());
        hasher.update(task_idx.to_le_bytes());
        let digest = hasher.finalize();
        seed_seed.copy_from_slice(&digest);
    }
    let mut rng = StdRng::from_seed(seed_seed);

    loop {
        // —— 循环顶部检查所有退出条件 ——
        if Instant::now() >= deadline {
            break;
        }
        if ctx.abort.load(Ordering::SeqCst) || ctx.cancelled.load(Ordering::SeqCst) {
            break;
        }

        // 决定本次 file_size
        let size = match &ctx.config.size {
            WriteSize::Fixed { bytes } => *bytes,
            WriteSize::Range {
                min,
                max,
                distribution,
            } => sample_size(&mut rng, *min, *max, *distribution),
        };

        // throughput 限速：按 size 取 token
        if let Some(bucket) = &throughput_bucket {
            bucket.acquire(size).await;
        }

        // 随机子目录 + 文件名
        let subdir = random_subdir(&mut rng);
        let dir_path = root.join(&subdir);
        let file_path = dir_path.join(random_file_name(&mut rng));

        // mkdir -p
        if let Err(source) = fs::create_dir_all(&dir_path).await {
            metrics_write.record_error();
            ctx.abort.store(true, Ordering::SeqCst);
            return Err(WriteRunError::Io {
                op: "mkdir",
                path: dir_path,
                source,
            });
        }

        // 生成 payload
        let payload = generate_payload(&mut rng, size);
        let in_measure = Instant::now() >= ctx.measure_start;

        // 写入
        let elapsed =
            match fs_io::write_file(&file_path, &payload, ctx.io_profile, ctx.config.fsync).await {
                Ok(elapsed) => elapsed,
                Err(source) => {
                    metrics_write.record_error();
                    ctx.abort.store(true, Ordering::SeqCst);
                    return Err(WriteRunError::Io {
                        op: "write",
                        path: file_path,
                        source,
                    });
                }
            };
        if in_measure {
            metrics_write.record_op(elapsed_us_saturating(elapsed), size);
        }

        // verify_after_write
        if let Some(metrics_v) = &metrics_verify {
            let expected = sha256(&payload);
            let v_start = Instant::now();
            // 流式读回 + 增量 sha256，避免 GB 级文件 OOM（spec §6.6 verify_after_write）。
            let actual = match stream_sha256(&file_path).await {
                Ok(h) => h,
                Err(source) => {
                    metrics_v.record_error();
                    ctx.abort.store(true, Ordering::SeqCst);
                    return Err(WriteRunError::Io {
                        op: "verify_read",
                        path: file_path,
                        source,
                    });
                }
            };
            if actual != expected {
                metrics_v.record_error();
                ctx.abort.store(true, Ordering::SeqCst);
                return Err(WriteRunError::VerifyMismatch { path: file_path });
            }
            let v_elapsed = v_start.elapsed();
            if in_measure {
                metrics_v.record_op(elapsed_us_saturating(v_elapsed), size);
            }
        }

        // think_time
        if let Some(t) = ctx.config.think_time {
            // 也在 sleep 中检测 cancel 是更严格的，但 think_time 通常 ≤ 100ms，可接受。
            tokio::time::sleep(t).await;
        }
    }
    Ok(())
}

fn sha256(bytes: &[u8]) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    hasher.finalize().into()
}

/// 流式读取文件并计算 sha256：8 KiB buffer 循环 update，O(1) 内存（spec §6.6）。
async fn stream_sha256(path: &Path) -> std::io::Result<[u8; 32]> {
    let mut file = fs::File::open(path).await?;
    let mut hasher = Sha256::new();
    let mut buf = vec![0u8; 8 * 1024];
    loop {
        let n = file.read(&mut buf).await?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
    }
    Ok(hasher.finalize().into())
}

fn elapsed_us_saturating(d: Duration) -> u64 {
    u64::try_from(d.as_micros()).unwrap_or(u64::MAX)
}

fn random_subdir<R: Rng + ?Sized>(rng: &mut R) -> PathBuf {
    let depth = 1 + rng.gen_range(0..SUBDIR_MAX_DEPTH);
    let mut path = PathBuf::new();
    for _ in 0..depth {
        path.push(hex_segment(rng, SUBDIR_SEGMENT_LEN));
    }
    path
}

fn random_file_name<R: RngCore + ?Sized>(rng: &mut R) -> String {
    format!("file_{:08x}.dat", rng.next_u32())
}

fn hex_segment<R: RngCore + ?Sized>(rng: &mut R, len: usize) -> String {
    let mask = (1u64 << (len * 4)) - 1;
    let v = (rng.next_u64()) & mask;
    format!("{v:0width$x}", width = len)
}

fn generate_payload<R: RngCore + ?Sized>(rng: &mut R, size: u64) -> Vec<u8> {
    let len = usize::try_from(size).unwrap_or(usize::MAX);
    let mut buf = vec![0u8; len];
    rng.fill_bytes(&mut buf);
    buf
}

fn sample_size<R: Rng + ?Sized>(rng: &mut R, min: u64, max: u64, dist: SizeDistribution) -> u64 {
    if min >= max {
        return min;
    }
    match dist {
        SizeDistribution::Uniform => rng.gen_range(min..=max),
        SizeDistribution::LogUniform => {
            #[allow(clippy::cast_precision_loss)]
            let lo = (min as f64).max(1.0).ln();
            #[allow(clippy::cast_precision_loss)]
            let hi = (max as f64).max(1.0).ln();
            let r = rng.gen_range(lo..=hi);
            #[allow(
                clippy::cast_precision_loss,
                clippy::cast_possible_truncation,
                clippy::cast_sign_loss
            )]
            let v = r.exp() as u64;
            v.clamp(min, max)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicBool;
    use tempfile::tempdir;

    fn ctx_for(tmp: &Path, write_cfg: WriteConfig, measure_start: Instant) -> Arc<WriteContext> {
        Arc::new(WriteContext {
            config: write_cfg,
            mount_point: tmp.to_path_buf(),
            worker_id: "host-1-aaaa1111".into(),
            job_id: "job-test".into(),
            worker_index: 0,
            io_profile: IoProfile {
                io_mode: cvd_common::spec::IoMode::Seq,
                io_aligned: true,
                direct_io: false,
                block_size: 4096,
            },
            metrics: Arc::new(MetricsRegistry::new()),
            abort: Arc::new(AtomicBool::new(false)),
            cancelled: Arc::new(AtomicBool::new(false)),
            measure_start,
        })
    }

    fn fixed_cfg(concurrency: i32) -> WriteConfig {
        WriteConfig {
            concurrency: concurrency.try_into().unwrap(),
            dir: "bench/write".into(),
            size: WriteSize::Fixed { bytes: 4096 },
            fsync: false,
            cleanup: false,
            think_time: None,
            rate_limit: None,
            verify_after_write: false,
        }
    }

    #[tokio::test]
    async fn writes_files_into_work_root() {
        let tmp = tempdir().unwrap();
        let ctx = ctx_for(tmp.path(), fixed_cfg(2), Instant::now());
        // 200ms 短窗口
        let deadline = Instant::now() + Duration::from_millis(200);
        run(ctx.clone(), deadline).await.unwrap();
        let root = work_root(&ctx);
        // 工作集还在（cleanup=false）
        assert!(root.is_dir());
        // 写过至少 1 个 op
        let (ops, bytes, errs) = ctx.metrics.op("write").totals();
        assert!(ops >= 1, "expected at least one write, got {ops}");
        assert!(bytes >= 4096);
        assert_eq!(errs, 0);
    }

    #[tokio::test]
    async fn cleanup_removes_work_root() {
        let tmp = tempdir().unwrap();
        let mut cfg = fixed_cfg(1);
        cfg.cleanup = true;
        let ctx = ctx_for(tmp.path(), cfg, Instant::now());
        let deadline = Instant::now() + Duration::from_millis(150);
        run(ctx.clone(), deadline).await.unwrap();
        let root = work_root(&ctx);
        assert!(!root.exists(), "cleanup should remove root");
    }

    #[tokio::test]
    async fn verify_after_write_records_metric() {
        let tmp = tempdir().unwrap();
        let mut cfg = fixed_cfg(1);
        cfg.verify_after_write = true;
        let ctx = ctx_for(tmp.path(), cfg, Instant::now());
        let deadline = Instant::now() + Duration::from_millis(200);
        run(ctx.clone(), deadline).await.unwrap();
        let (write_ops, _, _) = ctx.metrics.op("write").totals();
        let (verify_ops, _, _) = ctx.metrics.op("write_verify").totals();
        assert!(write_ops >= 1);
        assert_eq!(verify_ops, write_ops, "verify count must match write count");
    }

    #[tokio::test]
    async fn warmup_window_is_not_recorded() {
        let tmp = tempdir().unwrap();
        let cfg = fixed_cfg(1);
        // measure_start 在 deadline 之后 → 整个跑动期都算 warmup，不记录
        let measure_start = Instant::now() + Duration::from_secs(60);
        let ctx = ctx_for(tmp.path(), cfg, measure_start);
        let deadline = Instant::now() + Duration::from_millis(150);
        run(ctx.clone(), deadline).await.unwrap();
        let (ops, bytes, _) = ctx.metrics.op("write").totals();
        assert_eq!(ops, 0, "warmup ops should not be recorded");
        assert_eq!(bytes, 0);
    }

    #[tokio::test]
    async fn cancel_flag_short_circuits_loop() {
        let tmp = tempdir().unwrap();
        let ctx = ctx_for(tmp.path(), fixed_cfg(2), Instant::now());
        // 立即翻 cancel；deadline 长一些以确保不是因为时间到才退出
        ctx.cancelled.store(true, Ordering::SeqCst);
        let deadline = Instant::now() + Duration::from_secs(5);
        let start = Instant::now();
        run(ctx.clone(), deadline).await.unwrap();
        assert!(start.elapsed() < Duration::from_millis(200));
    }
}

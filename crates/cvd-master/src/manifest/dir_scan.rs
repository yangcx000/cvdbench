//! dir_manifest 并发扫描（spec §5.7 dir_manifest 模式 / §9.1）。
//!
//! 流水线：
//! - 1 个 reader：从 dir_manifest 文件逐行读「相对 mount_point 的目录」，push
//!   到内部 `mpsc::channel<PathBuf>`（dir_queue）；
//! - N 个 scanner（`dir_scan_concurrency`）：从 dir_queue 取目录，对每个目录做
//!   递归扫描，普通文件 push 到 file_queue（s3_key 留空）；symlink 记 warning
//!   后跳过，避免越过 mount_point 或形成循环；
//! - reader 完成 + 所有 scanner 退出后，外层把 manifest_done 翻 true；
//!   `loop_files = true` 时清空 done 后再走一遍。
//!
//! 错误处理：reader / scanner 任一 fail-fast；外层在 [`crate::manifest`] 把
//! 错误转成 job FAILED。

use std::collections::VecDeque;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Instant;

use cvd_proto::cvdbench as pb;
use thiserror::Error;
use tokio::fs;
use tokio::io::AsyncReadExt;
use tokio::sync::{Mutex, Notify};

use super::queue::{BoundedQueue, QueueClosed};
use crate::state::job::ManifestScanStats;

/// 共享目录工作队列初始容量；子目录会重新入队，让 scanner 之间能够分摊
/// 单个大目录树。
const DIR_QUEUE_INITIAL_CAPACITY: usize = 5_000;

#[derive(Debug, Default)]
struct DirWorkQueueInner {
    dirs: VecDeque<PathBuf>,
    reader_done: bool,
    pending_dirs: usize,
    closed: bool,
}

#[derive(Debug)]
struct DirWorkQueue {
    inner: Mutex<DirWorkQueueInner>,
    notify: Notify,
}

impl DirWorkQueue {
    fn new() -> Self {
        Self {
            inner: Mutex::new(DirWorkQueueInner {
                dirs: VecDeque::with_capacity(DIR_QUEUE_INITIAL_CAPACITY),
                reader_done: false,
                pending_dirs: 0,
                closed: false,
            }),
            notify: Notify::new(),
        }
    }

    async fn push(&self, dir: PathBuf) -> Result<(), QueueClosed> {
        let mut inner = self.inner.lock().await;
        if inner.closed {
            return Err(QueueClosed);
        }
        inner.dirs.push_back(dir);
        inner.pending_dirs += 1;
        drop(inner);
        self.notify.notify_one();
        Ok(())
    }

    async fn pop(&self, cancel: &AtomicBool) -> Option<PathBuf> {
        loop {
            if cancel.load(Ordering::SeqCst) {
                return None;
            }
            let notified = {
                let mut inner = self.inner.lock().await;
                if inner.closed {
                    return None;
                }
                if let Some(dir) = inner.dirs.pop_front() {
                    return Some(dir);
                }
                if inner.reader_done && inner.pending_dirs == 0 {
                    return None;
                }
                self.notify.notified()
            };
            notified.await;
        }
    }

    async fn finish_dir(&self) {
        let mut inner = self.inner.lock().await;
        inner.pending_dirs = inner.pending_dirs.saturating_sub(1);
        let should_wake = inner.reader_done && inner.pending_dirs == 0;
        drop(inner);
        if should_wake {
            self.notify.notify_waiters();
        }
    }

    async fn mark_reader_done(&self) {
        let mut inner = self.inner.lock().await;
        inner.reader_done = true;
        let should_wake = inner.pending_dirs == 0 || inner.dirs.is_empty();
        drop(inner);
        if should_wake {
            self.notify.notify_waiters();
        }
    }

    async fn close(&self) {
        self.inner.lock().await.closed = true;
        self.notify.notify_waiters();
    }
}

#[derive(Debug, Error)]
pub enum DirScanError {
    #[error("read dir_manifest {path:?}: {source}")]
    Io {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("read_dir {path:?}: {source}")]
    ReadDir {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("metadata {path:?}: {source}")]
    Metadata {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("path_safe error at line {lineno} of {path:?}: {source}")]
    PathSafe {
        path: PathBuf,
        lineno: usize,
        #[source]
        source: cvd_common::path_safe::PathSafeError,
    },
    #[error("path {path:?} escapes mount_point {mount:?}")]
    PathEscape { mount: PathBuf, path: PathBuf },
    #[error("file_queue closed during push")]
    QueueClosed,
    #[error("scanner task join failed: {0}")]
    Join(String),
}

/// 顶层入口：把 dir_manifest 转换为 file_queue 中的 FileEntry。
///
/// 调用方应已在 `record.mount_point` 上做了 canonicalize，本函数只比较 prefix。
pub async fn run_pipeline(
    manifest_path: PathBuf,
    mount_point: PathBuf,
    file_queue: Arc<BoundedQueue<pb::FileEntry>>,
    scan_concurrency: usize,
    cancel: Arc<AtomicBool>,
    loop_files: bool,
    done: Arc<AtomicBool>,
    stats: Arc<ManifestScanStats>,
) -> Result<(), DirScanError> {
    loop {
        if cancel.load(Ordering::SeqCst) {
            return Ok(());
        }
        single_pass(
            manifest_path.clone(),
            mount_point.clone(),
            file_queue.clone(),
            scan_concurrency,
            cancel.clone(),
            stats.clone(),
        )
        .await?;
        if !loop_files {
            done.store(true, Ordering::SeqCst);
            return Ok(());
        }
        done.store(false, Ordering::SeqCst);
    }
}

async fn single_pass(
    manifest_path: PathBuf,
    mount_point: PathBuf,
    file_queue: Arc<BoundedQueue<pb::FileEntry>>,
    scan_concurrency: usize,
    cancel: Arc<AtomicBool>,
    stats: Arc<ManifestScanStats>,
) -> Result<(), DirScanError> {
    let started = Instant::now();
    let scan_concurrency = scan_concurrency.max(1);

    let dir_queue = Arc::new(DirWorkQueue::new());

    // Reader task：reader_done 后 dir_tx drop，channel 关闭，scanner drain 完即退
    let reader_handle = {
        let path = manifest_path.clone();
        let cancel = cancel.clone();
        let queue = dir_queue.clone();
        tokio::spawn(async move { run_reader(path, queue, cancel).await })
    };

    // Scanner tasks
    let mut scanner_handles = Vec::with_capacity(scan_concurrency);
    for _ in 0..scan_concurrency {
        let dir_queue = dir_queue.clone();
        let mp = mount_point.clone();
        let fq = file_queue.clone();
        let c = cancel.clone();
        let stats = stats.clone();
        scanner_handles.push(tokio::spawn(async move {
            run_scanner(dir_queue, mp, fq, c, stats).await
        }));
    }

    // 收集 reader 错误（reader 错就 fail-fast）
    let reader_result = reader_handle
        .await
        .map_err(|e| DirScanError::Join(e.to_string()))?;
    let mut first_err: Option<DirScanError> = reader_result.err();
    if first_err.is_some() {
        dir_queue.close().await;
    }

    // 等所有 scanner
    for h in scanner_handles {
        match h.await {
            Ok(Ok(())) => {}
            Ok(Err(e)) => {
                cancel.store(true, Ordering::SeqCst);
                dir_queue.close().await;
                if first_err.is_none() {
                    first_err = Some(e);
                }
            }
            Err(e) => {
                cancel.store(true, Ordering::SeqCst);
                dir_queue.close().await;
                if first_err.is_none() {
                    first_err = Some(DirScanError::Join(e.to_string()));
                }
            }
        }
    }

    if let Some(err) = first_err {
        add_elapsed_ms(&stats, started);
        return Err(err);
    }
    add_elapsed_ms(&stats, started);
    Ok(())
}

fn add_elapsed_ms(stats: &ManifestScanStats, started: Instant) {
    let elapsed = i64::try_from(started.elapsed().as_millis()).unwrap_or(i64::MAX);
    stats.scan_duration_ms.fetch_add(elapsed, Ordering::SeqCst);
}

async fn run_reader(
    path: PathBuf,
    queue: Arc<DirWorkQueue>,
    cancel: Arc<AtomicBool>,
) -> Result<(), DirScanError> {
    let mut data = String::new();
    let mut file = fs::File::open(&path)
        .await
        .map_err(|source| DirScanError::Io {
            path: path.clone(),
            source,
        })?;
    file.read_to_string(&mut data)
        .await
        .map_err(|source| DirScanError::Io {
            path: path.clone(),
            source,
        })?;

    for (idx, line) in data.lines().enumerate() {
        if cancel.load(Ordering::SeqCst) {
            queue.close().await;
            return Ok(());
        }
        let lineno = idx + 1;
        let trimmed = line.trim();
        if trimmed.is_empty() || trimmed.starts_with('#') {
            continue;
        }
        cvd_common::path_safe::validate_relative(trimmed).map_err(|source| {
            DirScanError::PathSafe {
                path: path.clone(),
                lineno,
                source,
            }
        })?;
        queue
            .push(PathBuf::from(trimmed))
            .await
            .map_err(|QueueClosed| DirScanError::QueueClosed)?;
    }
    queue.mark_reader_done().await;
    Ok(())
}

async fn run_scanner(
    dir_queue: Arc<DirWorkQueue>,
    mount_point: PathBuf,
    file_queue: Arc<BoundedQueue<pb::FileEntry>>,
    cancel: Arc<AtomicBool>,
    stats: Arc<ManifestScanStats>,
) -> Result<(), DirScanError> {
    loop {
        if cancel.load(Ordering::SeqCst) {
            return Ok(());
        }
        let dir = dir_queue.pop(&cancel).await;
        let Some(dir) = dir else {
            return Ok(());
        };
        let result =
            scan_one_dir(dir, &mount_point, &file_queue, &dir_queue, &cancel, &stats).await;
        dir_queue.finish_dir().await;
        if result.is_ok() {
            stats.dirs_scanned.fetch_add(1, Ordering::SeqCst);
        }
        result?;
    }
}

/// 扫描单个目录；发现的子目录重新加入共享队列，供任意 scanner 继续处理。
async fn scan_one_dir(
    rel: PathBuf,
    mount_point: &Path,
    file_queue: &Arc<BoundedQueue<pb::FileEntry>>,
    dir_queue: &Arc<DirWorkQueue>,
    cancel: &AtomicBool,
    stats: &ManifestScanStats,
) -> Result<(), DirScanError> {
    if cancel.load(Ordering::SeqCst) {
        return Ok(());
    }
    let abs = mount_point.join(&rel);
    let mut entries = fs::read_dir(&abs)
        .await
        .map_err(|source| DirScanError::ReadDir {
            path: abs.clone(),
            source,
        })?;
    loop {
        if cancel.load(Ordering::SeqCst) {
            return Ok(());
        }
        let entry = match entries.next_entry().await {
            Ok(Some(e)) => e,
            Ok(None) => break,
            Err(source) => {
                return Err(DirScanError::ReadDir {
                    path: abs.clone(),
                    source,
                });
            }
        };
        let entry_path = entry.path();
        let file_type = entry
            .file_type()
            .await
            .map_err(|source| DirScanError::Metadata {
                path: entry_path.clone(),
                source,
            })?;
        // 不跟随 symlink（spec §9.1）
        if file_type.is_symlink() {
            tracing::debug!(path = ?entry_path, "scanner: symlink skipped");
            continue;
        }
        // 文件路径必须 canonicalize 后仍位于 mount_point 下（spec §9.1）。
        // 借 spawn_blocking 跑同步 canonicalize 不阻塞 reactor。
        let entry_rel = if file_type.is_file() {
            let entry_clone = entry_path.clone();
            let mount_canon = mount_point.to_path_buf();
            let canon = tokio::task::spawn_blocking(move || std::fs::canonicalize(&entry_clone))
                .await
                .map_err(|e| DirScanError::Join(e.to_string()))?
                .map_err(|source| DirScanError::Metadata {
                    path: entry_path.clone(),
                    source,
                })?;
            if !canon.starts_with(&mount_canon) {
                return Err(DirScanError::PathEscape {
                    mount: mount_canon,
                    path: canon,
                });
            }
            match canon.strip_prefix(mount_point) {
                Ok(p) => p.to_path_buf(),
                Err(_) => {
                    return Err(DirScanError::PathEscape {
                        mount: mount_point.to_path_buf(),
                        path: entry_path,
                    });
                }
            }
        } else {
            // 目录：strip_prefix 即可（文件层会再校验）
            match entry_path.strip_prefix(mount_point) {
                Ok(p) => p.to_path_buf(),
                Err(_) => {
                    return Err(DirScanError::PathEscape {
                        mount: mount_point.to_path_buf(),
                        path: entry_path,
                    });
                }
            }
        };
        if file_type.is_file() {
            // normalize 为统一 `/` 分隔（spec §9.1）
            let fs_path = entry_rel.to_string_lossy().into_owned();
            let fs_path = match cvd_common::path_safe::normalize_relative(&fs_path) {
                Ok(p) => p,
                Err(_) => {
                    // 罕见：canonical 路径可能含 `..` 等，记录并 fail-fast。
                    return Err(DirScanError::PathEscape {
                        mount: mount_point.to_path_buf(),
                        path: entry_path,
                    });
                }
            };
            file_queue
                .push(pb::FileEntry {
                    fs_path,
                    s3_key: String::new(),
                })
                .await
                .map_err(|QueueClosed| DirScanError::QueueClosed)?;
            stats.files_scanned.fetch_add(1, Ordering::SeqCst);
        } else if file_type.is_dir() {
            dir_queue
                .push(entry_rel)
                .await
                .map_err(|QueueClosed| DirScanError::QueueClosed)?;
        }
        // 其它类型（fifo/socket/block/char）忽略
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write as _;
    use tempfile::tempdir;

    #[tokio::test]
    async fn scans_files_in_listed_dirs_recursively() {
        let mount = tempdir().unwrap();
        let root = mount.path().to_path_buf();
        // 造数据：dataset/2024/01.csv, dataset/2024/sub/02.csv, dataset/2025/03.csv
        std::fs::create_dir_all(root.join("dataset/2024/sub")).unwrap();
        std::fs::create_dir_all(root.join("dataset/2025")).unwrap();
        std::fs::write(root.join("dataset/2024/01.csv"), b"x").unwrap();
        std::fs::write(root.join("dataset/2024/sub/02.csv"), b"x").unwrap();
        std::fs::write(root.join("dataset/2025/03.csv"), b"x").unwrap();

        // dir_manifest 文件：列出两个 top-level 目录
        let manifest = mount.path().join("dirs.txt");
        let mut f = std::fs::File::create(&manifest).unwrap();
        writeln!(f, "dataset/2024").unwrap();
        writeln!(f, "dataset/2025").unwrap();
        f.flush().unwrap();

        let queue = Arc::new(BoundedQueue::<pb::FileEntry>::new(64));
        let cancel = Arc::new(AtomicBool::new(false));
        let done = Arc::new(AtomicBool::new(false));
        let stats = Arc::new(ManifestScanStats::default());

        run_pipeline(
            manifest,
            root.clone(),
            queue.clone(),
            2,
            cancel,
            false,
            done.clone(),
            stats.clone(),
        )
        .await
        .unwrap();
        assert!(done.load(Ordering::SeqCst));
        assert_eq!(stats.dirs_scanned.load(Ordering::SeqCst), 3);
        assert_eq!(stats.files_scanned.load(Ordering::SeqCst), 3);
        assert!(stats.scan_duration_ms.load(Ordering::SeqCst) >= 0);

        let mut entries: Vec<String> = queue
            .drain_up_to(64)
            .into_iter()
            .map(|e| e.fs_path)
            .collect();
        entries.sort();
        assert_eq!(
            entries,
            vec![
                "dataset/2024/01.csv".to_owned(),
                "dataset/2024/sub/02.csv".to_owned(),
                "dataset/2025/03.csv".to_owned(),
            ]
        );
    }

    #[tokio::test]
    async fn skips_symlinks() {
        let mount = tempdir().unwrap();
        let root = mount.path().to_path_buf();
        std::fs::create_dir(root.join("d")).unwrap();
        std::fs::write(root.join("d/real.dat"), b"x").unwrap();
        std::os::unix::fs::symlink(root.join("d/real.dat"), root.join("d/sym.dat")).unwrap();

        let manifest = mount.path().join("dirs.txt");
        let mut f = std::fs::File::create(&manifest).unwrap();
        writeln!(f, "d").unwrap();
        f.flush().unwrap();

        let queue = Arc::new(BoundedQueue::<pb::FileEntry>::new(8));
        let cancel = Arc::new(AtomicBool::new(false));
        let done = Arc::new(AtomicBool::new(false));
        let stats = Arc::new(ManifestScanStats::default());

        run_pipeline(manifest, root, queue.clone(), 1, cancel, false, done, stats)
            .await
            .unwrap();
        let entries: Vec<String> = queue
            .drain_up_to(8)
            .into_iter()
            .map(|e| e.fs_path)
            .collect();
        assert_eq!(entries, vec!["d/real.dat".to_owned()]);
    }

    #[tokio::test]
    async fn rejects_absolute_path_in_dir_manifest() {
        let mount = tempdir().unwrap();
        let manifest = mount.path().join("dirs.txt");
        let mut f = std::fs::File::create(&manifest).unwrap();
        writeln!(f, "/etc").unwrap();
        f.flush().unwrap();
        let queue = Arc::new(BoundedQueue::<pb::FileEntry>::new(1));
        let cancel = Arc::new(AtomicBool::new(false));
        let done = Arc::new(AtomicBool::new(false));
        let stats = Arc::new(ManifestScanStats::default());
        let err = run_pipeline(
            manifest,
            mount.path().into(),
            queue,
            1,
            cancel,
            false,
            done,
            stats,
        )
        .await
        .unwrap_err();
        assert!(matches!(err, DirScanError::PathSafe { .. }));
    }

    #[tokio::test]
    async fn discovered_subdirs_are_shared_between_scanners() {
        let queue = Arc::new(DirWorkQueue::new());
        queue.push(PathBuf::from("root")).await.unwrap();
        let first = queue.pop(&AtomicBool::new(false)).await.unwrap();
        assert_eq!(first, PathBuf::from("root"));

        queue.push(PathBuf::from("root/a")).await.unwrap();
        queue.push(PathBuf::from("root/b")).await.unwrap();

        let cancel = Arc::new(AtomicBool::new(false));
        let q1 = queue.clone();
        let c1 = cancel.clone();
        let h1 = tokio::spawn(async move { q1.pop(&c1).await });
        let q2 = queue.clone();
        let c2 = cancel.clone();
        let h2 = tokio::spawn(async move { q2.pop(&c2).await });

        let mut popped = vec![h1.await.unwrap().unwrap(), h2.await.unwrap().unwrap()];
        popped.sort();
        assert_eq!(
            popped,
            vec![PathBuf::from("root/a"), PathBuf::from("root/b")]
        );

        queue.finish_dir().await;
        queue.finish_dir().await;
        queue.finish_dir().await;
        queue.mark_reader_done().await;
        assert!(queue.pop(&cancel).await.is_none());
    }

    #[tokio::test]
    async fn cancel_short_circuits() {
        let mount = tempdir().unwrap();
        let root = mount.path().to_path_buf();
        // 造一堆文件
        std::fs::create_dir(root.join("d")).unwrap();
        for i in 0..200 {
            std::fs::write(root.join(format!("d/{i:04}.dat")), b"x").unwrap();
        }
        let manifest = mount.path().join("dirs.txt");
        std::fs::write(&manifest, "d\n").unwrap();
        let queue = Arc::new(BoundedQueue::<pb::FileEntry>::new(8));
        let cancel = Arc::new(AtomicBool::new(true));
        let done = Arc::new(AtomicBool::new(false));
        let stats = Arc::new(ManifestScanStats::default());
        // cancel 已经 true，pipeline 应当立即返回 Ok
        run_pipeline(
            manifest,
            root,
            queue.clone(),
            2,
            cancel,
            false,
            done.clone(),
            stats,
        )
        .await
        .unwrap();
        // done 不会被设置（cancel 退出路径）
        assert!(!done.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn loop_files_re_reads_until_cancel() {
        let mount = tempdir().unwrap();
        let root = mount.path().to_path_buf();
        std::fs::create_dir(root.join("d")).unwrap();
        std::fs::write(root.join("d/a.dat"), b"x").unwrap();
        let manifest = mount.path().join("dirs.txt");
        std::fs::write(&manifest, "d\n").unwrap();

        let queue = Arc::new(BoundedQueue::<pb::FileEntry>::new(64));
        let cancel = Arc::new(AtomicBool::new(false));
        let done = Arc::new(AtomicBool::new(false));
        let q2 = queue.clone();
        let c2 = cancel.clone();
        let d2 = done.clone();
        let stats = Arc::new(ManifestScanStats::default());
        let handle =
            tokio::spawn(
                async move { run_pipeline(manifest, root, q2, 1, c2, true, d2, stats).await },
            );
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        cancel.store(true, Ordering::SeqCst);
        queue.close();
        let res = handle.await.unwrap();
        // close 触发的 push 错误也算合法终止路径
        assert!(matches!(res, Ok(()) | Err(DirScanError::QueueClosed)));
        assert!(!done.load(Ordering::SeqCst));
        // 至少跑过一遍
        let count = queue.drain_up_to(64).len();
        assert!(count >= 1);
    }
}

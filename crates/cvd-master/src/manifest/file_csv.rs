//! file_manifest CSV 流式解析（spec §5.7 file_manifest 模式 / §9.1 manifest 文件格式）。
//!
//! - 支持 RFC 4180 quoting；空行 / `#` 注释行忽略；
//! - 每行第一列 `fs_path`（必需）+ 第二列 `s3_key`（可选）；
//! - 每条 `fs_path` 走 [`cvd_common::path_safe::validate_relative`] —— 拒绝绝对路径
//!   / `..` / NUL；
//! - 单次 push 进 [`BoundedQueue`]，写入阻塞作为生产侧反压；
//! - `loop_files = true` 时读完一遍后清空 done 标记并循环重读；
//! - 任意时刻 cancel flag 翻 true 即立即退出（不再 push 也不 set done）。

use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use cvd_proto::cvdbench as pb;
use thiserror::Error;
use tokio::fs;
use tokio::io::AsyncReadExt;

use super::queue::{BoundedQueue, QueueClosed};

#[derive(Debug, Error)]
pub enum ReaderError {
    #[error("read manifest {path:?}: {source}")]
    Io {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("CSV parse error at line {lineno} of {path:?}: {source}")]
    Csv {
        path: PathBuf,
        lineno: usize,
        #[source]
        source: csv::Error,
    },
    #[error("line {lineno} of {path:?}: {source}")]
    PathSafe {
        path: PathBuf,
        lineno: usize,
        #[source]
        source: cvd_common::path_safe::PathSafeError,
    },
    #[error("queue closed during push (line {lineno})")]
    QueueClosed { lineno: usize },
}

pub async fn run(
    path: PathBuf,
    queue: Arc<BoundedQueue<pb::FileEntry>>,
    cancel: Arc<AtomicBool>,
    loop_files: bool,
    done: Arc<AtomicBool>,
) -> Result<(), ReaderError> {
    loop {
        if cancel.load(Ordering::SeqCst) {
            return Ok(());
        }
        read_pass(&path, &queue, &cancel).await?;
        if !loop_files {
            done.store(true, Ordering::SeqCst);
            return Ok(());
        }
        // 进入下一轮重读；保持 done=false（应该本来就是）
        done.store(false, Ordering::SeqCst);
    }
}

async fn read_pass(
    path: &PathBuf,
    queue: &BoundedQueue<pb::FileEntry>,
    cancel: &AtomicBool,
) -> Result<(), ReaderError> {
    // v1：一次性读全文件到内存，再用 csv crate 同步解析。生产环境若 manifest
    // 极大可改为 spawn_blocking + Reader<File>，行为不变。
    let mut data = String::new();
    let mut file = fs::File::open(path)
        .await
        .map_err(|source| ReaderError::Io {
            path: path.clone(),
            source,
        })?;
    file.read_to_string(&mut data)
        .await
        .map_err(|source| ReaderError::Io {
            path: path.clone(),
            source,
        })?;

    // 自行预扫描：spec §9.1 注释判定是「首列以 `#` 开头」（即整行第一个非空白
    // 字符是 `#`），不是任意位置出现 `#`。csv crate 的 comment 选项语义不一致，
    // 同时双引号包裹的合法路径开头是 `#` 时也会被错误吞掉，故关掉它，自己处理。
    let mut filtered = String::with_capacity(data.len());
    for line in data.split_inclusive('\n') {
        let trimmed = line.trim_start();
        if trimmed.is_empty() || trimmed.starts_with('#') {
            // 保留换行（让 lineno 对齐用户原始视角）
            filtered.push('\n');
        } else {
            filtered.push_str(line);
        }
    }

    let mut rdr = csv::ReaderBuilder::new()
        .has_headers(false)
        .flexible(true)
        .from_reader(filtered.as_bytes());

    for (idx, record) in rdr.records().enumerate() {
        if cancel.load(Ordering::SeqCst) {
            return Ok(());
        }
        let lineno = idx + 1;
        let record = record.map_err(|source| ReaderError::Csv {
            path: path.clone(),
            lineno,
            source,
        })?;
        if record.is_empty() {
            continue;
        }
        let fs_path_raw = record.get(0).unwrap_or("").trim().to_owned();
        if fs_path_raw.is_empty() {
            continue;
        }
        let s3_key = record
            .get(1)
            .map(|s| s.trim().to_owned())
            .unwrap_or_default();
        // 校验 + 规范化（spec §9.1：fs_path 入队前统一为 `/` 分隔）
        let fs_path =
            cvd_common::path_safe::normalize_relative(&fs_path_raw).map_err(|source| {
                ReaderError::PathSafe {
                    path: path.clone(),
                    lineno,
                    source,
                }
            })?;
        queue
            .push(pb::FileEntry { fs_path, s3_key })
            .await
            .map_err(|QueueClosed| ReaderError::QueueClosed { lineno })?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write as _;
    use tempfile::NamedTempFile;

    fn write_manifest(content: &str) -> NamedTempFile {
        let mut f = NamedTempFile::new().unwrap();
        f.write_all(content.as_bytes()).unwrap();
        f.flush().unwrap();
        f
    }

    #[tokio::test]
    async fn reads_csv_with_two_columns_and_skips_comments_blank_lines() {
        let mf = write_manifest(
            "# comment\n\
             a/x.dat,a/x.dat\n\
             \n\
             b/y.dat\n\
             # trailing\n",
        );
        let q = Arc::new(BoundedQueue::<pb::FileEntry>::new(8));
        let cancel = Arc::new(AtomicBool::new(false));
        let done = Arc::new(AtomicBool::new(false));
        run(
            mf.path().to_path_buf(),
            q.clone(),
            cancel,
            false,
            done.clone(),
        )
        .await
        .unwrap();
        assert!(done.load(Ordering::SeqCst));
        let entries = q.drain_up_to(10);
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].fs_path, "a/x.dat");
        assert_eq!(entries[0].s3_key, "a/x.dat");
        assert_eq!(entries[1].fs_path, "b/y.dat");
        assert_eq!(entries[1].s3_key, "");
    }

    #[tokio::test]
    async fn rejects_absolute_or_parent_dir() {
        let mf = write_manifest("/etc/passwd,foo\n");
        let q = Arc::new(BoundedQueue::<pb::FileEntry>::new(8));
        let cancel = Arc::new(AtomicBool::new(false));
        let done = Arc::new(AtomicBool::new(false));
        let err = run(mf.path().to_path_buf(), q, cancel, false, done)
            .await
            .unwrap_err();
        assert!(matches!(err, ReaderError::PathSafe { .. }));
    }

    #[tokio::test]
    async fn loop_files_true_keeps_pushing_until_cancel() {
        let mf = write_manifest("a.dat\n");
        let q = Arc::new(BoundedQueue::<pb::FileEntry>::new(64));
        let cancel = Arc::new(AtomicBool::new(false));
        let done = Arc::new(AtomicBool::new(false));
        let q2 = q.clone();
        let cancel2 = cancel.clone();
        let done2 = done.clone();
        let path = mf.path().to_path_buf();
        let handle = tokio::spawn(async move { run(path, q2, cancel2, true, done2).await });
        // 让 reader 跑一会儿
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        cancel.store(true, Ordering::SeqCst);
        // close 让 push 立即返回（防止 reader 卡在满 queue 上）
        q.close();
        // 终止路径有两种：cancel 命中 (Ok) 或 push 命中 close (QueueClosed)；
        // 都算合法清理。
        let res = handle.await.unwrap();
        assert!(matches!(res, Ok(()) | Err(ReaderError::QueueClosed { .. })));
        // loop_files 模式不会写 done=true（仅 cancel 退出，无论哪条退出路径）
        assert!(!done.load(Ordering::SeqCst));
        // queue 至少被填了多次（容量 64）
        assert!(q.drain_up_to(64).len() > 1);
    }

    #[tokio::test]
    async fn missing_file_yields_io_error() {
        let q = Arc::new(BoundedQueue::<pb::FileEntry>::new(1));
        let cancel = Arc::new(AtomicBool::new(false));
        let done = Arc::new(AtomicBool::new(false));
        let err = run(
            PathBuf::from("/nonexistent/manifest.csv"),
            q,
            cancel,
            false,
            done,
        )
        .await
        .unwrap_err();
        assert!(matches!(err, ReaderError::Io { .. }));
    }
}

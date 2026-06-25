//! 元数据预建 / 只读扫描（spec §6.6）。
//!
//! 按 BFS 分层 mkdir：第 k 层完成后再开始第 k+1 层（保证父目录已存在）；
//! 同层内用 `layout_concurrency` 路并发。文件用 `tokio::fs::File::create` 触一下
//! 即关。空文件足够支持 stat/open/readdir 三个读 op。
//!
//! 输出 [`Layout`]：
//! - `root` —— `mount/{metadata.dir}/{worker_id}/{job_id}/`
//! - `directories` —— 第 1 ~ depth 层全部目录的扁平列表（不含 root）
//! - `files` —— `directories` 下每个目录里 `files_per_dir` 个文件的扁平列表
//! - `task_roots` —— `<root>/_tasks/t_xxxx/`，每个 hot-loop task 独占一个；
//!   create / mkdir 操作在各自的 task_root 下生成新名，避免并发 race。

use std::path::{Path, PathBuf};

use cvd_common::spec::MetadataConfig;
use futures::stream::{self, StreamExt, TryStreamExt};
use thiserror::Error;
use tokio::fs;

#[derive(Debug, Error)]
pub enum BuildError {
    #[error("{op}: {path:?}: {source}")]
    Io {
        op: &'static str,
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("read_only metadata found no {kind} under {root:?}")]
    EmptyReadOnlyLayout { kind: &'static str, root: PathBuf },
}

/// 预建产物，runner 会把它包成 `Arc<Layout>` 共享给所有 hot-loop task。
#[derive(Debug, Clone)]
pub struct Layout {
    pub root: PathBuf,
    pub directories: Vec<PathBuf>,
    pub files: Vec<PathBuf>,
    pub task_roots: Vec<PathBuf>,
}

/// 只读模式入口：扫描 `mount/{metadata.dir}` 下已有目录树，供 stat/open/readdir
/// hot-loop 使用；不创建任何目录或文件。
pub async fn scan_existing(
    mount_point: &Path,
    config: &MetadataConfig,
) -> Result<Layout, BuildError> {
    let root = mount_point.join(&config.dir);
    let mut directories = vec![root.clone()];
    let mut files = Vec::new();
    let mut stack = vec![root.clone()];
    let scan_limit = config.read_only_scan_limit as usize;
    let mut scanned_entries = 0usize;

    while let Some(dir) = stack.pop() {
        let mut entries = fs::read_dir(&dir).await.map_err(|source| BuildError::Io {
            op: "read_dir",
            path: dir.clone(),
            source,
        })?;
        while let Some(entry) = entries
            .next_entry()
            .await
            .map_err(|source| BuildError::Io {
                op: "read_dir_next",
                path: dir.clone(),
                source,
            })?
        {
            scanned_entries = scanned_entries.saturating_add(1);
            let path = entry.path();
            let kind = entry.file_type().await.map_err(|source| BuildError::Io {
                op: "file_type",
                path: path.clone(),
                source,
            })?;
            if kind.is_dir() {
                directories.push(path.clone());
                stack.push(path);
            } else if kind.is_file() {
                files.push(path);
            }
            if scan_limit > 0 && scanned_entries >= scan_limit {
                stack.clear();
                break;
            }
        }
    }

    if config
        .ops
        .iter()
        .any(|op| matches!(op, cvd_common::spec::MetadataOp::Readdir))
        && directories.is_empty()
    {
        return Err(BuildError::EmptyReadOnlyLayout {
            kind: "directories",
            root,
        });
    }
    if config.ops.iter().any(|op| {
        matches!(
            op,
            cvd_common::spec::MetadataOp::Stat | cvd_common::spec::MetadataOp::Open
        )
    }) && files.is_empty()
    {
        return Err(BuildError::EmptyReadOnlyLayout {
            kind: "files",
            root,
        });
    }

    let task_roots = vec![root.clone(); config.concurrency as usize];
    Ok(Layout {
        root,
        directories,
        files,
        task_roots,
    })
}

/// 主入口：在 `mount/{metadata.dir}/{worker_id}/{job_id}/` 下按 layout 构建工作集。
pub async fn build(
    mount_point: &Path,
    config: &MetadataConfig,
    worker_id: &str,
    job_id: &str,
) -> Result<Layout, BuildError> {
    let root = mount_point.join(&config.dir).join(worker_id).join(job_id);

    fs::create_dir_all(&root)
        .await
        .map_err(|source| BuildError::Io {
            op: "mkdir_root",
            path: root.clone(),
            source,
        })?;

    let layout_concurrency = config.layout_concurrency.max(1) as usize;

    // BFS 分层 mkdir
    let mut all_dirs: Vec<PathBuf> = Vec::new();
    let mut current_layer: Vec<PathBuf> = vec![root.clone()];
    for _level in 1..=config.depth {
        let mut next_layer: Vec<PathBuf> =
            Vec::with_capacity(current_layer.len().saturating_mul(config.width as usize));
        for parent in &current_layer {
            for i in 0..config.width {
                next_layer.push(parent.join(format!("d_{i:04}")));
            }
        }
        // mkdir 消耗一份；保留另一份给 all_dirs 与下一层基线
        mkdir_many(next_layer.clone(), layout_concurrency).await?;
        all_dirs.extend(next_layer.iter().cloned());
        current_layer = next_layer;
    }

    // 在每个非 root 目录里 touch `files_per_dir` 个空文件
    let mut file_paths: Vec<PathBuf> =
        Vec::with_capacity(all_dirs.len().saturating_mul(config.files_per_dir as usize));
    for dir in &all_dirs {
        for i in 0..config.files_per_dir {
            file_paths.push(dir.join(format!("f_{i:04}.dat")));
        }
    }
    create_many(file_paths.clone(), layout_concurrency).await?;

    // task 私有子目录
    let tasks_root = root.join("_tasks");
    fs::create_dir(&tasks_root)
        .await
        .map_err(|source| BuildError::Io {
            op: "mkdir_tasks_root",
            path: tasks_root.clone(),
            source,
        })?;
    let mut task_roots = Vec::with_capacity(config.concurrency as usize);
    for i in 0..config.concurrency {
        let p = tasks_root.join(format!("t_{i:04}"));
        fs::create_dir(&p).await.map_err(|source| BuildError::Io {
            op: "mkdir_task_root",
            path: p.clone(),
            source,
        })?;
        task_roots.push(p);
    }

    Ok(Layout {
        root,
        directories: all_dirs,
        files: file_paths,
        task_roots,
    })
}

async fn mkdir_many(paths: Vec<PathBuf>, concurrency: usize) -> Result<(), BuildError> {
    let concurrency = concurrency.max(1);
    stream::iter(paths.into_iter().map(|p| async move {
        fs::create_dir(&p).await.map_err(|source| BuildError::Io {
            op: "mkdir",
            path: p,
            source,
        })
    }))
    .buffer_unordered(concurrency)
    .try_collect::<Vec<()>>()
    .await?;
    Ok(())
}

async fn create_many(paths: Vec<PathBuf>, concurrency: usize) -> Result<(), BuildError> {
    let concurrency = concurrency.max(1);
    stream::iter(paths.into_iter().map(|p| async move {
        // create + drop 即可：空文件足够支持 stat/open/readdir
        fs::File::create(&p)
            .await
            .map_err(|source| BuildError::Io {
                op: "create_file",
                path: p,
                source,
            })?;
        Ok::<(), BuildError>(())
    }))
    .buffer_unordered(concurrency)
    .try_collect::<Vec<()>>()
    .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use cvd_common::spec::MetadataOp;
    use tempfile::tempdir;

    fn small_cfg() -> MetadataConfig {
        MetadataConfig {
            concurrency: 2,
            dir: "bench/meta".into(),
            dir_manifest: None,
            ops: vec![MetadataOp::Stat, MetadataOp::Mkdir],
            read_only: false,
            read_only_scan_limit: 0,
            depth: 2,
            width: 2,
            files_per_dir: 2,
            layout_concurrency: 4,
            think_time: None,
            rate_limit: None,
        }
    }

    #[tokio::test]
    async fn builds_layout_counts_match_spec() {
        let tmp = tempdir().unwrap();
        let cfg = small_cfg();
        let layout = build(tmp.path(), &cfg, "w-1", "j-1").await.unwrap();
        // 总目录 = 2^1 + 2^2 = 6
        assert_eq!(layout.directories.len(), 6);
        // 总文件 = 6 * 2 = 12
        assert_eq!(layout.files.len(), 12);
        // task_roots = concurrency = 2
        assert_eq!(layout.task_roots.len(), 2);
        // 每个 task_root 都是真目录
        for p in &layout.task_roots {
            assert!(p.is_dir());
        }
        // 文件路径都已 touch
        for p in &layout.files {
            assert!(p.is_file(), "file {p:?} should exist");
        }
    }

    #[tokio::test]
    async fn build_is_idempotent_for_root_only() {
        // 同一个 root 重复 build 会因为 layer mkdir 冲突报错（spec 不要求幂等）；
        // 但 mkdir_root 用 create_dir_all 不会报错。这条 test 验证至少 root 创建可重复。
        let tmp = tempdir().unwrap();
        let cfg = MetadataConfig {
            depth: 1,
            width: 1,
            files_per_dir: 1,
            concurrency: 1,
            layout_concurrency: 1,
            ..small_cfg()
        };
        let _ = build(tmp.path(), &cfg, "w", "j").await.unwrap();
        // 第二次 build 应当报错（layer-1 dir 已存在）—— spec 不允许复用同一 worker_id+job_id
        let err = build(tmp.path(), &cfg, "w", "j").await.unwrap_err();
        assert!(matches!(err, BuildError::Io { .. }));
    }

    #[tokio::test]
    async fn scan_existing_builds_read_only_layout_without_creating() {
        let tmp = tempdir().unwrap();
        let root = tmp.path().join("existing/meta");
        fs::create_dir_all(root.join("d1/sub")).await.unwrap();
        fs::write(root.join("d1/file_a"), b"").await.unwrap();
        fs::write(root.join("d1/sub/file_b"), b"").await.unwrap();
        let cfg = MetadataConfig {
            read_only: true,
            read_only_scan_limit: 0,
            dir: "existing/meta".into(),
            dir_manifest: None,
            ops: vec![MetadataOp::Stat, MetadataOp::Open, MetadataOp::Readdir],
            ..small_cfg()
        };

        let layout = scan_existing(tmp.path(), &cfg).await.unwrap();

        assert_eq!(layout.root, root);
        assert_eq!(layout.files.len(), 2);
        assert!(layout.directories.len() >= 3);
        assert_eq!(layout.task_roots.len(), cfg.concurrency as usize);
        assert!(layout.task_roots.iter().all(|p| p == &layout.root));
    }
}

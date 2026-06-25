//! `cvd-master.toml` 解析（spec §5.1）：`[server]` / `[metrics]` / `[scheduler]` /
//! `[[filesystems]]`，不含 TLS。

use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::time::Duration;

use serde::Deserialize;
use thiserror::Error;

/// 启动期解析出来的 master 配置。一旦加载即冻结，运行中不重读（spec §9.9）。
#[derive(Debug, Clone)]
pub struct MasterConfig {
    pub listen: SocketAddr,
    pub metrics_listen: Option<SocketAddr>,
    pub scheduler: SchedulerConfig,
    /// `fs_name` → `mount_point`（绝对路径）。
    pub filesystems: HashMap<String, PathBuf>,
}

#[derive(Debug, Clone, Copy)]
pub struct SchedulerConfig {
    pub worker_staleness: Duration,
    pub job_retention: Duration,
    pub prepare_timeout: Duration,
    pub start_delay: Duration,
    pub file_queue_capacity: usize,
    pub dir_queue_capacity: usize,
    pub dir_scan_concurrency: usize,
}

impl Default for SchedulerConfig {
    fn default() -> Self {
        Self {
            worker_staleness: Duration::from_secs(60),
            job_retention: Duration::from_secs(259_200),
            prepare_timeout: Duration::from_secs(600),
            start_delay: Duration::from_millis(5_000),
            file_queue_capacity: 100_000,
            dir_queue_capacity: 50_000,
            dir_scan_concurrency: 8,
        }
    }
}

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("read config {path:?}: {source}")]
    Io {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("parse TOML: {0}")]
    Toml(#[from] toml::de::Error),
    #[error("invalid listen address {0:?}: {1}")]
    Listen(String, std::net::AddrParseError),
    #[error("invalid metrics listen address {0:?}: {1}")]
    MetricsListen(String, std::net::AddrParseError),
    #[error("duplicate fs_name {0:?} in [[filesystems]]")]
    DuplicateFs(String),
    #[error("at least one [[filesystems]] entry is required")]
    NoFilesystems,
    #[error("filesystem {0:?} mount_point must be absolute")]
    NonAbsoluteMount(String),
    #[error("scheduler.{field} must be > 0, got {value}")]
    InvalidScheduler { field: &'static str, value: usize },
}

#[derive(Debug, Deserialize)]
struct RawConfig {
    server: RawServer,
    #[serde(default)]
    metrics: Option<RawMetrics>,
    #[serde(default)]
    scheduler: RawScheduler,
    #[serde(default)]
    filesystems: Vec<RawFilesystem>,
}

#[derive(Debug, Deserialize)]
struct RawServer {
    listen: String,
}

#[derive(Debug, Deserialize)]
struct RawMetrics {
    listen: String,
}

#[derive(Debug, Default, Deserialize)]
struct RawScheduler {
    worker_staleness_secs: Option<u64>,
    job_retention_secs: Option<u64>,
    prepare_timeout_secs: Option<u64>,
    start_delay_ms: Option<u64>,
    file_queue_capacity: Option<usize>,
    dir_queue_capacity: Option<usize>,
    dir_scan_concurrency: Option<usize>,
}

#[derive(Debug, Deserialize)]
struct RawFilesystem {
    name: String,
    mount_point: String,
}

/// 加载并解析 master 配置。
pub fn load(path: &Path) -> Result<MasterConfig, ConfigError> {
    let content = std::fs::read_to_string(path).map_err(|source| ConfigError::Io {
        path: path.to_path_buf(),
        source,
    })?;
    parse_str(&content)
}

/// 仅做字符串解析，便于单元测试。
pub fn parse_str(content: &str) -> Result<MasterConfig, ConfigError> {
    let raw: RawConfig = toml::from_str(content)?;
    let listen: SocketAddr = raw
        .server
        .listen
        .parse()
        .map_err(|e| ConfigError::Listen(raw.server.listen.clone(), e))?;
    let metrics_listen = raw
        .metrics
        .map(|m| {
            m.listen
                .parse()
                .map_err(|e| ConfigError::MetricsListen(m.listen, e))
        })
        .transpose()?;

    let mut scheduler = SchedulerConfig::default();
    apply_scheduler(&mut scheduler, &raw.scheduler)?;

    if raw.filesystems.is_empty() {
        return Err(ConfigError::NoFilesystems);
    }
    let mut filesystems = HashMap::new();
    for fs in raw.filesystems {
        let mount = PathBuf::from(&fs.mount_point);
        if !mount.is_absolute() {
            return Err(ConfigError::NonAbsoluteMount(fs.name));
        }
        // 启动期一次性 canonicalize（spec §9.1）：去掉 symlink、`.`、`..`，
        // 让 dir_scan 的 strip_prefix 比较语义可靠。
        // 路径不存在或不可访问时 fallback 到原值（不强制要求 mount_point 已存在，
        // 但若 dir_scan 时不存在则会自然报错）。
        let canonical = std::fs::canonicalize(&mount).unwrap_or_else(|_| mount.clone());
        if filesystems.insert(fs.name.clone(), canonical).is_some() {
            return Err(ConfigError::DuplicateFs(fs.name));
        }
    }

    Ok(MasterConfig {
        listen,
        metrics_listen,
        scheduler,
        filesystems,
    })
}

fn apply_scheduler(out: &mut SchedulerConfig, raw: &RawScheduler) -> Result<(), ConfigError> {
    if let Some(v) = raw.worker_staleness_secs {
        out.worker_staleness = Duration::from_secs(v);
    }
    if let Some(v) = raw.job_retention_secs {
        out.job_retention = Duration::from_secs(v);
    }
    if let Some(v) = raw.prepare_timeout_secs {
        out.prepare_timeout = Duration::from_secs(v);
    }
    if let Some(v) = raw.start_delay_ms {
        out.start_delay = Duration::from_millis(v);
    }
    if let Some(v) = raw.file_queue_capacity {
        if v == 0 {
            return Err(ConfigError::InvalidScheduler {
                field: "file_queue_capacity",
                value: v,
            });
        }
        out.file_queue_capacity = v;
    }
    if let Some(v) = raw.dir_queue_capacity {
        if v == 0 {
            return Err(ConfigError::InvalidScheduler {
                field: "dir_queue_capacity",
                value: v,
            });
        }
        out.dir_queue_capacity = v;
    }
    if let Some(v) = raw.dir_scan_concurrency {
        if v == 0 {
            return Err(ConfigError::InvalidScheduler {
                field: "dir_scan_concurrency",
                value: v,
            });
        }
        out.dir_scan_concurrency = v;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    const SAMPLE: &str = r#"
[server]
listen = "0.0.0.0:9090"

[metrics]
listen = "0.0.0.0:9100"

[scheduler]
worker_staleness_secs = 90
file_queue_capacity = 50000

[[filesystems]]
name = "examplefs"
mount_point = "/mnt/examplefs"

[[filesystems]]
name = "s3fs"
mount_point = "/mnt/s3fs"
"#;

    #[test]
    fn loads_full_config() {
        let cfg = parse_str(SAMPLE).unwrap();
        assert_eq!(cfg.listen.to_string(), "0.0.0.0:9090");
        assert_eq!(
            cfg.metrics_listen.map(|a| a.to_string()),
            Some("0.0.0.0:9100".to_owned())
        );
        assert_eq!(cfg.scheduler.worker_staleness, Duration::from_secs(90));
        assert_eq!(cfg.scheduler.file_queue_capacity, 50_000);
        // 未覆盖的字段保持默认
        assert_eq!(cfg.scheduler.dir_scan_concurrency, 8);
        assert_eq!(cfg.filesystems.len(), 2);
        assert_eq!(cfg.filesystems["examplefs"], PathBuf::from("/mnt/examplefs"),);
    }

    #[test]
    fn defaults_apply_when_scheduler_omitted() {
        let cfg = parse_str(
            r#"
[server]
listen = "127.0.0.1:0"

[[filesystems]]
name = "x"
mount_point = "/mnt/x"
"#,
        )
        .unwrap();
        assert!(cfg.metrics_listen.is_none());
        assert_eq!(cfg.scheduler, SchedulerConfig::default());
    }

    #[test]
    fn rejects_no_filesystems() {
        let err = parse_str(
            r#"
[server]
listen = "127.0.0.1:0"
"#,
        )
        .unwrap_err();
        assert!(matches!(err, ConfigError::NoFilesystems));
    }

    #[test]
    fn rejects_relative_mount_point() {
        let err = parse_str(
            r#"
[server]
listen = "127.0.0.1:0"

[[filesystems]]
name = "x"
mount_point = "relative/path"
"#,
        )
        .unwrap_err();
        assert!(matches!(err, ConfigError::NonAbsoluteMount(name) if name == "x"));
    }

    #[test]
    fn rejects_duplicate_fs() {
        let err = parse_str(
            r#"
[server]
listen = "127.0.0.1:0"

[[filesystems]]
name = "x"
mount_point = "/mnt/x"

[[filesystems]]
name = "x"
mount_point = "/mnt/y"
"#,
        )
        .unwrap_err();
        assert!(matches!(err, ConfigError::DuplicateFs(name) if name == "x"));
    }

    #[test]
    fn rejects_zero_scheduler_capacities() {
        let err = parse_str(
            r#"
[server]
listen = "127.0.0.1:0"

[scheduler]
file_queue_capacity = 0

[[filesystems]]
name = "x"
mount_point = "/mnt/x"
"#,
        )
        .unwrap_err();
        assert!(matches!(
            err,
            ConfigError::InvalidScheduler {
                field: "file_queue_capacity",
                value: 0
            }
        ));
    }

    impl PartialEq for SchedulerConfig {
        fn eq(&self, o: &Self) -> bool {
            self.worker_staleness == o.worker_staleness
                && self.job_retention == o.job_retention
                && self.prepare_timeout == o.prepare_timeout
                && self.start_delay == o.start_delay
                && self.file_queue_capacity == o.file_queue_capacity
                && self.dir_queue_capacity == o.dir_queue_capacity
                && self.dir_scan_concurrency == o.dir_scan_concurrency
        }
    }
    impl Eq for SchedulerConfig {}
}

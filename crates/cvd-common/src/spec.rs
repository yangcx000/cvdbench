//! 业务侧 BenchSpec：proto 转 in/out，校验，凭据脱敏。
//!
//! 与 `cvd_proto::cvdbench::BenchSpec` 的关系：
//! - **proto** 结构里所有字段都是 `string` / `i32` / `bool`，是 wire 形态；
//! - **本模块** 提供强类型版本（[`BenchSpec`] 等），把 `string` 字段解析成
//!   [`Duration`] / `u64` / [`crate::parse::RateLimit`] / 枚举，并把 `optional`
//!   语义直接落到 `Option<T>`。
//!
//! 流程：
//! 1. master 收到 CLI [`cvd_proto::cvdbench::CreateJobRequest`]；
//! 2. 调用 [`extract_credentials`] 把明文凭据抽出，单独保存；
//! 3. 调用 [`redact_in_place`] 让存储态 spec 仅含 `"***"`；
//! 4. 调用 [`BenchSpec::try_from_proto`] 得到强类型版本（解析失败 → 422）；
//! 5. 调用 [`validate::validate`] 跑 §9.4 中无需 master 配置的校验项；
//! 6. master 自己再补 `fs_name` → mount_point、manifest 文件可读、
//!    layout 软上限的覆盖（如有 toml 覆盖）。

use std::time::Duration;

use cvd_proto::cvdbench as pb;
use thiserror::Error;

use crate::parse::{parse_duration, parse_rate_limit, parse_size, ParseError, RateLimit};

pub mod redact;
pub mod validate;

pub use redact::{extract_credentials, redact_in_place, redacted, REDACTED};

/// 字符串字段解析失败的统一错误。
#[derive(Debug, Error)]
pub enum SpecConvertError {
    #[error("{field}: {source}")]
    Parse {
        field: &'static str,
        #[source]
        source: ParseError,
    },
    #[error("{field}: {message}")]
    Bad {
        field: &'static str,
        message: String,
    },
}

impl SpecConvertError {
    fn bad(field: &'static str, message: impl Into<String>) -> Self {
        Self::Bad {
            field,
            message: message.into(),
        }
    }
}

/// I/O 访问模式。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IoMode {
    Seq,
    Rand,
}

impl IoMode {
    fn from_proto(s: &str) -> Result<Self, SpecConvertError> {
        match s {
            "seq" => Ok(Self::Seq),
            "rand" => Ok(Self::Rand),
            other => Err(SpecConvertError::bad(
                "io_mode",
                format!("expected `seq` or `rand`, got {other:?}"),
            )),
        }
    }
}

/// 元数据操作类型。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MetadataOp {
    Create,
    Mkdir,
    Stat,
    Open,
    Readdir,
}

impl MetadataOp {
    fn from_proto(s: &str) -> Result<Self, SpecConvertError> {
        match s {
            "create" => Ok(Self::Create),
            "mkdir" => Ok(Self::Mkdir),
            "stat" => Ok(Self::Stat),
            "open" => Ok(Self::Open),
            "readdir" => Ok(Self::Readdir),
            other => Err(SpecConvertError::bad(
                "metadata.ops",
                format!("unsupported op {other:?}"),
            )),
        }
    }
}

/// 写场景文件大小分布。Spec §9.7 默认 `log_uniform`。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SizeDistribution {
    Uniform,
    LogUniform,
}

impl SizeDistribution {
    fn from_proto(s: &str) -> Result<Self, SpecConvertError> {
        match s {
            "" | "log_uniform" => Ok(Self::LogUniform),
            "uniform" => Ok(Self::Uniform),
            other => Err(SpecConvertError::bad(
                "write.file_size_range.distribution",
                format!("expected `uniform` / `log_uniform` / empty, got {other:?}"),
            )),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WriteSize {
    /// 固定大小（spec.write.file_size）。
    Fixed { bytes: u64 },
    /// 随机大小（spec.write.file_size_range）。
    Range {
        min: u64,
        max: u64,
        distribution: SizeDistribution,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReadSource {
    /// `read.file_manifest` —— 文件列表 CSV 相对/绝对路径。
    FileManifest { path: String },
    /// `read.dir_manifest` —— 目录列表文件路径。
    DirManifest { path: String },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConsistencyConfig {
    pub bucket_name: String,
    pub bucket_url: String,
    pub access_key: String,
    pub secret_key: String,
    pub region: String,
    pub prefix: String,
    pub session_token: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReadConfig {
    pub concurrency: u32,
    pub source: ReadSource,
    pub think_time: Option<Duration>,
    pub rate_limit: Option<RateLimit>,
    pub s3_consistency_check: Option<ConsistencyConfig>,
    pub loop_files: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WriteConfig {
    pub concurrency: u32,
    pub dir: String,
    pub size: WriteSize,
    pub fsync: bool,
    pub cleanup: bool,
    pub think_time: Option<Duration>,
    pub rate_limit: Option<RateLimit>,
    pub verify_after_write: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetadataConfig {
    pub concurrency: u32,
    pub dir: String,
    pub dir_manifest: Option<String>,
    pub ops: Vec<MetadataOp>,
    pub read_only: bool,
    pub read_only_scan_limit: u32,
    pub depth: u32,
    pub width: u32,
    pub files_per_dir: u32,
    /// `0`/省略 → 复用 `concurrency`，本结构体存归一化后的值。
    pub layout_concurrency: u32,
    pub think_time: Option<Duration>,
    pub rate_limit: Option<RateLimit>,
}

/// 业务侧 BenchSpec（强类型）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BenchSpec {
    pub fs_name: String,
    pub io_mode: IoMode,
    pub io_aligned: bool,
    pub direct_io: bool,
    pub block_size: u64,
    pub duration: Duration,
    pub warmup: Duration,
    /// 归一化后必 ≥ 1。
    pub target_workers: u32,
    pub read: Option<ReadConfig>,
    pub write: Option<WriteConfig>,
    pub metadata: Option<MetadataConfig>,
}

impl BenchSpec {
    /// 仅做字符串解析（`<num>(unit)` / 枚举 / 互斥项 shape），不跑 §9.4 全量校验。
    /// 完整校验请额外调用 [`validate::validate`]。
    pub fn try_from_proto(spec: pb::BenchSpec) -> Result<Self, SpecConvertError> {
        let pb::BenchSpec {
            fs_name,
            io_mode,
            io_aligned,
            direct_io,
            block_size,
            duration,
            warmup,
            read,
            write,
            metadata,
            target_workers,
        } = spec;

        let io_mode = IoMode::from_proto(&io_mode)?;
        let block_size = parse_required_size("block_size", &block_size)?;
        let duration = parse_required_duration("duration", &duration)?;
        let warmup = parse_optional_duration("warmup", &warmup)?.unwrap_or(Duration::ZERO);

        let read = read.map(read_from_proto).transpose()?;
        let write = write.map(write_from_proto).transpose()?;
        let metadata = metadata.map(metadata_from_proto).transpose()?;

        let target_workers = if target_workers <= 0 {
            1
        } else {
            u32::try_from(target_workers)
                .map_err(|_| SpecConvertError::bad("target_workers", "must fit in u32"))?
        };

        Ok(Self {
            fs_name,
            io_mode,
            io_aligned,
            direct_io,
            block_size,
            duration,
            warmup,
            target_workers,
            read,
            write,
            metadata,
        })
    }
}

fn parse_required_size(field: &'static str, raw: &str) -> Result<u64, SpecConvertError> {
    let v = parse_size(raw)
        .map_err(|source| SpecConvertError::Parse { field, source })?
        .ok_or_else(|| SpecConvertError::bad(field, "must be set"))?;
    Ok(v)
}

fn parse_required_duration(field: &'static str, raw: &str) -> Result<Duration, SpecConvertError> {
    let v = parse_duration(raw)
        .map_err(|source| SpecConvertError::Parse { field, source })?
        .ok_or_else(|| SpecConvertError::bad(field, "must be set"))?;
    Ok(v)
}

fn parse_optional_duration(
    field: &'static str,
    raw: &str,
) -> Result<Option<Duration>, SpecConvertError> {
    parse_duration(raw).map_err(|source| SpecConvertError::Parse { field, source })
}

fn parse_optional_rate(
    field: &'static str,
    raw: &str,
) -> Result<Option<RateLimit>, SpecConvertError> {
    parse_rate_limit(raw).map_err(|source| SpecConvertError::Parse { field, source })
}

fn convert_concurrency(field: &'static str, raw: i32) -> Result<u32, SpecConvertError> {
    if raw <= 0 {
        return Err(SpecConvertError::bad(
            field,
            format!("must be positive, got {raw}"),
        ));
    }
    u32::try_from(raw).map_err(|_| SpecConvertError::bad(field, "must fit in u32"))
}

fn read_from_proto(read: pb::ReadConfig) -> Result<ReadConfig, SpecConvertError> {
    let pb::ReadConfig {
        concurrency,
        file_manifest,
        dir_manifest,
        think_time,
        rate_limit,
        s3_consistency_check,
        loop_files,
    } = read;

    let source = match (file_manifest.is_empty(), dir_manifest.is_empty()) {
        (true, true) => {
            return Err(SpecConvertError::bad(
                "read",
                "either file_manifest or dir_manifest must be set",
            ));
        }
        (false, false) => {
            return Err(SpecConvertError::bad(
                "read",
                "file_manifest and dir_manifest are mutually exclusive",
            ));
        }
        (false, true) => ReadSource::FileManifest {
            path: file_manifest,
        },
        (true, false) => ReadSource::DirManifest { path: dir_manifest },
    };

    Ok(ReadConfig {
        concurrency: convert_concurrency("read.concurrency", concurrency)?,
        source,
        think_time: parse_optional_duration("read.think_time", &think_time)?,
        rate_limit: parse_optional_rate("read.rate_limit", &rate_limit)?,
        s3_consistency_check: s3_consistency_check.map(consistency_from_proto),
        loop_files,
    })
}

fn consistency_from_proto(c: pb::ConsistencyConfig) -> ConsistencyConfig {
    ConsistencyConfig {
        bucket_name: c.bucket_name,
        bucket_url: c.bucket_url,
        access_key: c.access_key,
        secret_key: c.secret_key,
        region: c.region,
        prefix: c.prefix,
        session_token: c.session_token,
    }
}

fn write_from_proto(write: pb::WriteConfig) -> Result<WriteConfig, SpecConvertError> {
    let pb::WriteConfig {
        concurrency,
        dir,
        file_size,
        file_size_range,
        fsync,
        cleanup,
        think_time,
        rate_limit,
        verify_after_write,
    } = write;

    let size = match (file_size.is_empty(), file_size_range.as_ref()) {
        (true, None) => {
            return Err(SpecConvertError::bad(
                "write",
                "either file_size or file_size_range must be set",
            ));
        }
        (false, Some(_)) => {
            return Err(SpecConvertError::bad(
                "write",
                "file_size and file_size_range are mutually exclusive",
            ));
        }
        (false, None) => WriteSize::Fixed {
            bytes: parse_required_size("write.file_size", &file_size)?,
        },
        (true, Some(range)) => WriteSize::Range {
            min: parse_required_size("write.file_size_range.min", &range.min)?,
            max: parse_required_size("write.file_size_range.max", &range.max)?,
            distribution: SizeDistribution::from_proto(&range.distribution)?,
        },
    };

    Ok(WriteConfig {
        concurrency: convert_concurrency("write.concurrency", concurrency)?,
        dir,
        size,
        fsync,
        cleanup,
        think_time: parse_optional_duration("write.think_time", &think_time)?,
        rate_limit: parse_optional_rate("write.rate_limit", &rate_limit)?,
        verify_after_write,
    })
}

fn metadata_from_proto(meta: pb::MetadataConfig) -> Result<MetadataConfig, SpecConvertError> {
    let pb::MetadataConfig {
        concurrency,
        dir,
        ops,
        depth,
        width,
        files_per_dir,
        think_time,
        rate_limit,
        layout_concurrency,
        read_only,
        read_only_scan_limit,
        dir_manifest,
    } = meta;

    let parsed_ops: Vec<MetadataOp> = ops
        .iter()
        .map(|s| MetadataOp::from_proto(s))
        .collect::<Result<_, _>>()?;

    let concurrency = convert_concurrency("metadata.concurrency", concurrency)?;
    let layout_concurrency = if layout_concurrency <= 0 {
        concurrency
    } else {
        u32::try_from(layout_concurrency)
            .map_err(|_| SpecConvertError::bad("metadata.layout_concurrency", "must fit in u32"))?
    };

    let depth =
        u32::try_from(depth).map_err(|_| SpecConvertError::bad("metadata.depth", "negative"))?;
    let width =
        u32::try_from(width).map_err(|_| SpecConvertError::bad("metadata.width", "negative"))?;
    let files_per_dir = u32::try_from(files_per_dir)
        .map_err(|_| SpecConvertError::bad("metadata.files_per_dir", "negative"))?;
    let read_only_scan_limit = u32::try_from(read_only_scan_limit)
        .map_err(|_| SpecConvertError::bad("metadata.read_only_scan_limit", "negative"))?;

    Ok(MetadataConfig {
        concurrency,
        dir,
        dir_manifest: if dir_manifest.is_empty() {
            None
        } else {
            Some(dir_manifest)
        },
        ops: parsed_ops,
        read_only,
        read_only_scan_limit,
        depth,
        width,
        files_per_dir,
        layout_concurrency,
        think_time: parse_optional_duration("metadata.think_time", &think_time)?,
        rate_limit: parse_optional_rate("metadata.rate_limit", &rate_limit)?,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn minimal_proto_spec() -> pb::BenchSpec {
        pb::BenchSpec {
            fs_name: "examplefs".into(),
            io_mode: "seq".into(),
            io_aligned: true,
            direct_io: false,
            block_size: "1M".into(),
            duration: "1h".into(),
            warmup: "5m".into(),
            read: None,
            write: None,
            metadata: None,
            target_workers: 4,
        }
    }

    #[test]
    fn parses_basic_spec() {
        let mut p = minimal_proto_spec();
        p.read = Some(pb::ReadConfig {
            concurrency: 16,
            file_manifest: "manifests/read.csv".into(),
            dir_manifest: String::new(),
            think_time: "10ms".into(),
            rate_limit: "1GB/s".into(),
            s3_consistency_check: None,
            loop_files: false,
        });
        let s = BenchSpec::try_from_proto(p).unwrap();
        assert_eq!(s.fs_name, "examplefs");
        assert_eq!(s.io_mode, IoMode::Seq);
        assert_eq!(s.block_size, 1_000_000);
        assert_eq!(s.duration, Duration::from_secs(3600));
        assert_eq!(s.warmup, Duration::from_secs(300));
        assert_eq!(s.target_workers, 4);
        let r = s.read.unwrap();
        assert_eq!(r.concurrency, 16);
        assert_eq!(
            r.source,
            ReadSource::FileManifest {
                path: "manifests/read.csv".into()
            }
        );
        assert_eq!(r.think_time, Some(Duration::from_millis(10)));
        assert_eq!(
            r.rate_limit,
            Some(RateLimit::Throughput {
                bytes_per_sec: 1_000_000_000
            })
        );
        assert!(!r.loop_files);
    }

    #[test]
    fn normalizes_target_workers() {
        let mut p = minimal_proto_spec();
        p.target_workers = 0;
        let s = BenchSpec::try_from_proto(p).unwrap();
        assert_eq!(s.target_workers, 1);

        let mut p = minimal_proto_spec();
        p.target_workers = -3;
        let s = BenchSpec::try_from_proto(p).unwrap();
        assert_eq!(s.target_workers, 1);
    }

    #[test]
    fn warmup_defaults_to_zero() {
        let mut p = minimal_proto_spec();
        p.warmup = String::new();
        let s = BenchSpec::try_from_proto(p).unwrap();
        assert_eq!(s.warmup, Duration::ZERO);
    }

    #[test]
    fn rejects_unknown_io_mode() {
        let mut p = minimal_proto_spec();
        p.io_mode = "stride".into();
        let err = BenchSpec::try_from_proto(p).unwrap_err();
        assert!(matches!(err, SpecConvertError::Bad { field, .. } if field == "io_mode"));
    }

    #[test]
    fn rejects_missing_block_size() {
        let mut p = minimal_proto_spec();
        p.block_size = String::new();
        let err = BenchSpec::try_from_proto(p).unwrap_err();
        assert!(matches!(err, SpecConvertError::Bad { field, .. } if field == "block_size"));
    }

    #[test]
    fn read_requires_one_manifest_kind() {
        let mut p = minimal_proto_spec();
        p.read = Some(pb::ReadConfig {
            concurrency: 1,
            file_manifest: String::new(),
            dir_manifest: String::new(),
            think_time: String::new(),
            rate_limit: String::new(),
            s3_consistency_check: None,
            loop_files: false,
        });
        let err = BenchSpec::try_from_proto(p).unwrap_err();
        assert!(matches!(err, SpecConvertError::Bad { field, .. } if field == "read"));
    }

    #[test]
    fn read_rejects_both_manifest_kinds() {
        let mut p = minimal_proto_spec();
        p.read = Some(pb::ReadConfig {
            concurrency: 1,
            file_manifest: "f.csv".into(),
            dir_manifest: "dirs.txt".into(),
            think_time: String::new(),
            rate_limit: String::new(),
            s3_consistency_check: None,
            loop_files: false,
        });
        let err = BenchSpec::try_from_proto(p).unwrap_err();
        assert!(matches!(err, SpecConvertError::Bad { field, .. } if field == "read"));
    }

    #[test]
    fn write_size_is_either_fixed_or_range() {
        let mut p = minimal_proto_spec();
        p.write = Some(pb::WriteConfig {
            concurrency: 1,
            dir: "bench/write".into(),
            file_size: String::new(),
            file_size_range: None,
            fsync: false,
            cleanup: true,
            think_time: String::new(),
            rate_limit: String::new(),
            verify_after_write: false,
        });
        let err = BenchSpec::try_from_proto(p).unwrap_err();
        assert!(matches!(err, SpecConvertError::Bad { field, .. } if field == "write"));

        let mut p = minimal_proto_spec();
        p.write = Some(pb::WriteConfig {
            concurrency: 1,
            dir: "bench/write".into(),
            file_size: "1G".into(),
            file_size_range: Some(pb::FileSizeRange {
                min: "1K".into(),
                max: "1M".into(),
                distribution: String::new(),
            }),
            fsync: false,
            cleanup: true,
            think_time: String::new(),
            rate_limit: String::new(),
            verify_after_write: false,
        });
        let err = BenchSpec::try_from_proto(p).unwrap_err();
        assert!(matches!(err, SpecConvertError::Bad { field, .. } if field == "write"));
    }

    #[test]
    fn metadata_layout_concurrency_falls_back() {
        let mut p = minimal_proto_spec();
        p.metadata = Some(pb::MetadataConfig {
            concurrency: 32,
            dir: "bench/meta".into(),
            ops: vec!["create".into(), "stat".into()],
            depth: 3,
            width: 4,
            files_per_dir: 100,
            think_time: "1ms".into(),
            rate_limit: "5000iops".into(),
            layout_concurrency: 0,
            read_only: false,
            read_only_scan_limit: 0,
            dir_manifest: String::new(),
        });
        let s = BenchSpec::try_from_proto(p).unwrap();
        let m = s.metadata.unwrap();
        assert_eq!(m.layout_concurrency, 32);
        assert_eq!(m.rate_limit, Some(RateLimit::Iops { ops_per_sec: 5000 }));
        assert_eq!(m.ops, vec![MetadataOp::Create, MetadataOp::Stat]);
    }

    #[test]
    fn metadata_rejects_unknown_op() {
        let mut p = minimal_proto_spec();
        p.metadata = Some(pb::MetadataConfig {
            concurrency: 1,
            dir: "bench/meta".into(),
            ops: vec!["chmod".into()],
            depth: 1,
            width: 1,
            files_per_dir: 1,
            think_time: String::new(),
            rate_limit: String::new(),
            layout_concurrency: 0,
            read_only: false,
            read_only_scan_limit: 0,
            dir_manifest: String::new(),
        });
        let err = BenchSpec::try_from_proto(p).unwrap_err();
        assert!(matches!(err, SpecConvertError::Bad { field, .. } if field == "metadata.ops"));
    }
}

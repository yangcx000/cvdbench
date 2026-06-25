//! Spec §9.4 BenchSpec 校验清单。
//!
//! 本模块只跑「无需 master 配置」的规则；`fs_name → mount_point`、manifest 文件
//! 是否可读这些与 `cvd-master.toml` 强耦合的项放在 master crate 完成。
//!
//! 校验是 fail-fast 友好但报错 collected：所有违规一次性返回，方便 CLI/user 一次
//! 看清全部问题。

use std::fmt;

use thiserror::Error;

use crate::parse::RateLimit;
use crate::path_safe::{self, PathSafeError};

use super::{
    BenchSpec, ConsistencyConfig, MetadataConfig, MetadataOp, ReadConfig, ReadSource, WriteConfig,
    WriteSize,
};

/// 单条违规。
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub struct Violation {
    pub field: String,
    pub message: String,
}

impl Violation {
    fn new(field: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            field: field.into(),
            message: message.into(),
        }
    }
}

impl fmt::Display for Violation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.field, self.message)
    }
}

/// 一次校验汇总的全部违规。
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct ValidationReport {
    pub violations: Vec<Violation>,
}

impl ValidationReport {
    pub fn is_empty(&self) -> bool {
        self.violations.is_empty()
    }

    fn push(&mut self, v: Violation) {
        self.violations.push(v);
    }

    fn into_result(self) -> Result<(), Self> {
        if self.is_empty() {
            Ok(())
        } else {
            Err(self)
        }
    }
}

impl fmt::Display for ValidationReport {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for (i, v) in self.violations.iter().enumerate() {
            if i > 0 {
                writeln!(f)?;
            }
            write!(f, "{v}")?;
        }
        Ok(())
    }
}

impl std::error::Error for ValidationReport {}

/// Spec §9.6 layout 软上限。Master 可通过自身配置覆盖默认值。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LayoutCaps {
    pub max_depth: u32,
    pub max_width: u32,
    pub max_files_per_dir: u32,
    pub max_total_dirs: u64,
    pub max_total_files: u64,
}

impl Default for LayoutCaps {
    fn default() -> Self {
        Self {
            max_depth: 8,
            max_width: 1_000,
            max_files_per_dir: 100_000,
            max_total_dirs: 100_000,
            max_total_files: 10_000_000,
        }
    }
}

/// 校验上下文。当前只承载 layout caps；后续可加 fs_name 白名单等。
#[derive(Debug, Default, Clone, Copy)]
pub struct ValidationContext {
    pub layout_caps: LayoutCaps,
    /// Allow redacted credential placeholders in `read.s3_consistency_check`.
    ///
    /// Master uses the default strict mode for CreateJob. Workers receive a
    /// redacted stored spec plus separate credential material in FetchJob, so
    /// they must validate the non-secret S3 fields without rejecting `***`.
    pub allow_redacted_consistency_credentials: bool,
}

const BLOCK_SIZE_ALIGNMENT: u64 = 512;

/// 主入口。
pub fn validate(spec: &BenchSpec, ctx: &ValidationContext) -> Result<(), ValidationReport> {
    let mut report = ValidationReport::default();
    validate_into(spec, ctx, &mut report);
    report.into_result()
}

fn validate_into(spec: &BenchSpec, ctx: &ValidationContext, r: &mut ValidationReport) {
    if spec.fs_name.is_empty() {
        r.push(Violation::new("fs_name", "must not be empty"));
    }

    if spec.duration.is_zero() {
        r.push(Violation::new("duration", "must be > 0"));
    }
    if !spec.warmup.is_zero() && spec.warmup >= spec.duration {
        r.push(Violation::new(
            "warmup",
            format!(
                "must be < duration ({} ms < {} ms)",
                spec.warmup.as_millis(),
                spec.duration.as_millis()
            ),
        ));
    }

    if spec.block_size == 0 {
        r.push(Violation::new("block_size", "must be > 0"));
    } else if spec.block_size % BLOCK_SIZE_ALIGNMENT != 0 {
        r.push(Violation::new(
            "block_size",
            format!("must be aligned to {BLOCK_SIZE_ALIGNMENT} bytes"),
        ));
    }

    if spec.direct_io && !spec.io_aligned {
        r.push(Violation::new(
            "direct_io",
            "O_DIRECT requires io_aligned=true",
        ));
    }

    if spec.read.is_none() && spec.write.is_none() && spec.metadata.is_none() {
        r.push(Violation::new(
            "spec",
            "at least one of `read` / `write` / `metadata` must be set",
        ));
    }

    if let Some(read) = &spec.read {
        validate_read(read, ctx.allow_redacted_consistency_credentials, r);
    }
    if let Some(write) = &spec.write {
        validate_write(write, r);
    }
    if let Some(meta) = &spec.metadata {
        validate_metadata(meta, &ctx.layout_caps, r);
    }
}

fn validate_read(read: &ReadConfig, allow_redacted_credentials: bool, r: &mut ValidationReport) {
    if read.concurrency == 0 {
        r.push(Violation::new("read.concurrency", "must be > 0"));
    }

    // manifest 路径是 master 本地文件路径（可绝对/相对），不属于 mount_point
    // 内的相对路径范畴；只校验非空，存在性由 master 在 CreateJob 时另行检查。
    match &read.source {
        ReadSource::FileManifest { path } => {
            if path.is_empty() {
                r.push(Violation::new("read.file_manifest", "must not be empty"));
            }
        }
        ReadSource::DirManifest { path } => {
            if path.is_empty() {
                r.push(Violation::new("read.dir_manifest", "must not be empty"));
            }
        }
    }

    if let Some(rl) = read.rate_limit {
        if !matches!(rl, RateLimit::Throughput { .. }) {
            r.push(Violation::new(
                "read.rate_limit",
                "must be a throughput rate (e.g. `1GB/s`)",
            ));
        }
    }

    if let Some(s3) = &read.s3_consistency_check {
        validate_consistency(s3, allow_redacted_credentials, r);
    }
}

fn validate_consistency(
    s3: &ConsistencyConfig,
    allow_redacted_credentials: bool,
    r: &mut ValidationReport,
) {
    if s3.bucket_name.is_empty() {
        r.push(Violation::new(
            "read.s3_consistency_check.bucket_name",
            "must not be empty",
        ));
    }
    if s3.bucket_url.is_empty() {
        r.push(Violation::new(
            "read.s3_consistency_check.bucket_url",
            "must not be empty",
        ));
    }
    if s3.region.is_empty() {
        r.push(Violation::new(
            "read.s3_consistency_check.region",
            "must not be empty",
        ));
    }
    if s3.access_key.is_empty()
        || (!allow_redacted_credentials && s3.access_key == crate::spec::REDACTED)
    {
        r.push(Violation::new(
            "read.s3_consistency_check.access_key",
            "must not be empty",
        ));
    }
    if s3.secret_key.is_empty()
        || (!allow_redacted_credentials && s3.secret_key == crate::spec::REDACTED)
    {
        r.push(Violation::new(
            "read.s3_consistency_check.secret_key",
            "must not be empty",
        ));
    }
}

fn validate_write(write: &WriteConfig, r: &mut ValidationReport) {
    if write.concurrency == 0 {
        r.push(Violation::new("write.concurrency", "must be > 0"));
    }
    check_path("write.dir", &write.dir, r);

    match &write.size {
        WriteSize::Fixed { bytes } => {
            if *bytes == 0 {
                r.push(Violation::new("write.file_size", "must be > 0"));
            }
        }
        WriteSize::Range { min, max, .. } => {
            if *min == 0 {
                r.push(Violation::new("write.file_size_range.min", "must be > 0"));
            }
            if max < min {
                r.push(Violation::new(
                    "write.file_size_range",
                    format!("min ({min}) must be <= max ({max})"),
                ));
            }
        }
    }

    if let Some(rl) = write.rate_limit {
        if !matches!(rl, RateLimit::Throughput { .. }) {
            r.push(Violation::new(
                "write.rate_limit",
                "must be a throughput rate (e.g. `500MB/s`)",
            ));
        }
    }
}

fn validate_metadata(meta: &MetadataConfig, caps: &LayoutCaps, r: &mut ValidationReport) {
    if meta.concurrency == 0 {
        r.push(Violation::new("metadata.concurrency", "must be > 0"));
    }
    if meta.layout_concurrency == 0 {
        r.push(Violation::new("metadata.layout_concurrency", "must be > 0"));
    }
    if let Some(path) = &meta.dir_manifest {
        if path.is_empty() {
            r.push(Violation::new("metadata.dir_manifest", "must not be empty"));
        }
    } else {
        check_path("metadata.dir", &meta.dir, r);
    }

    if meta.ops.is_empty() {
        r.push(Violation::new("metadata.ops", "must not be empty"));
    }

    if meta.dir_manifest.is_some() && meta.ops.iter().any(|op| !matches!(op, MetadataOp::Stat)) {
        r.push(Violation::new(
            "metadata.ops",
            "metadata.dir_manifest only supports `stat`",
        ));
    }

    if meta.read_only
        && meta
            .ops
            .iter()
            .any(|op| matches!(op, MetadataOp::Create | MetadataOp::Mkdir))
    {
        r.push(Violation::new(
            "metadata.ops",
            "read_only=true only supports `stat`, `open`, and `readdir`",
        ));
    }

    // ops 含 stat / open 时必须有可读文件——否则 runner 会在第一次随机选到这两种 op
    // 时立即 fail-fast（`pick_random` on empty list），让用户的合法配置变成假性 spec
    // §6.6 fail-fast。校验时拒绝。
    let needs_files = meta
        .ops
        .iter()
        .any(|op| matches!(op, MetadataOp::Stat | MetadataOp::Open));
    if meta.dir_manifest.is_none() && !meta.read_only && needs_files && meta.files_per_dir == 0 {
        r.push(Violation::new(
            "metadata.files_per_dir",
            "must be > 0 when metadata.ops contains `stat` or `open`",
        ));
    }

    if meta.dir_manifest.is_none() && !meta.read_only && meta.depth == 0 {
        r.push(Violation::new("metadata.depth", "must be > 0"));
    } else if meta.depth > caps.max_depth {
        r.push(Violation::new(
            "metadata.depth",
            format!("exceeds soft cap {}", caps.max_depth),
        ));
    }
    if meta.dir_manifest.is_none() && !meta.read_only && meta.width == 0 {
        r.push(Violation::new("metadata.width", "must be > 0"));
    } else if meta.width > caps.max_width {
        r.push(Violation::new(
            "metadata.width",
            format!("exceeds soft cap {}", caps.max_width),
        ));
    }
    if meta.files_per_dir > caps.max_files_per_dir {
        r.push(Violation::new(
            "metadata.files_per_dir",
            format!("exceeds soft cap {}", caps.max_files_per_dir),
        ));
    }

    if meta.depth > 0 && meta.width > 0 {
        let total_dirs = total_dirs(meta.depth, meta.width);
        if total_dirs > caps.max_total_dirs {
            r.push(Violation::new(
                "metadata.layout",
                format!(
                    "total directories {total_dirs} exceeds soft cap {}",
                    caps.max_total_dirs
                ),
            ));
        }
        let total_files = total_dirs.saturating_mul(u64::from(meta.files_per_dir));
        if total_files > caps.max_total_files {
            r.push(Violation::new(
                "metadata.layout",
                format!(
                    "total files {total_files} exceeds soft cap {}",
                    caps.max_total_files
                ),
            ));
        }
    }

    if let Some(rl) = meta.rate_limit {
        if !matches!(rl, RateLimit::Iops { .. }) {
            r.push(Violation::new(
                "metadata.rate_limit",
                "must be an iops rate (e.g. `5000iops`)",
            ));
        }
    }
}

/// `Σ width^i for i in 1..=depth`，`saturating` 防止溢出。
fn total_dirs(depth: u32, width: u32) -> u64 {
    let mut sum: u64 = 0;
    let mut layer: u64 = 1;
    let width = u64::from(width);
    for _ in 0..depth {
        layer = layer.saturating_mul(width);
        sum = sum.saturating_add(layer);
        if sum == u64::MAX {
            break;
        }
    }
    sum
}

fn check_path(field: &'static str, path: &str, r: &mut ValidationReport) {
    if let Err(err) = path_safe::validate_relative(path) {
        r.push(Violation::new(field, render_path_error(&err)));
    }
}

fn render_path_error(err: &PathSafeError) -> String {
    match err {
        PathSafeError::Empty => "must not be empty".into(),
        PathSafeError::NulByte { .. } => "must not contain NUL byte".into(),
        PathSafeError::MustBeRelative { .. } => "must be a relative path (no leading '/')".into(),
        PathSafeError::ForbiddenComponent { component, .. } => {
            format!("must not contain component {component:?}")
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use cvd_proto::cvdbench as pb;

    use super::*;
    use crate::spec::{BenchSpec, MetadataOp, SizeDistribution};

    fn good_spec() -> BenchSpec {
        BenchSpec::try_from_proto(pb::BenchSpec {
            fs_name: "examplefs".into(),
            io_mode: "seq".into(),
            io_aligned: true,
            direct_io: false,
            block_size: "1Mi".into(),
            duration: "1h".into(),
            warmup: "5m".into(),
            target_workers: 4,
            read: Some(pb::ReadConfig {
                concurrency: 4,
                file_manifest: "manifests/read.csv".into(),
                dir_manifest: String::new(),
                think_time: String::new(),
                rate_limit: "1GB/s".into(),
                s3_consistency_check: None,
                loop_files: false,
            }),
            write: None,
            metadata: None,
        })
        .unwrap()
    }

    #[test]
    fn good_spec_passes() {
        validate(&good_spec(), &ValidationContext::default()).unwrap();
    }

    #[test]
    fn rejects_zero_duration_and_warmup_overflow() {
        let mut s = good_spec();
        s.duration = Duration::ZERO;
        s.warmup = Duration::from_secs(60);
        let err = validate(&s, &ValidationContext::default()).unwrap_err();
        let fields: Vec<_> = err.violations.iter().map(|v| v.field.as_str()).collect();
        assert!(fields.contains(&"duration"));
    }

    #[test]
    fn rejects_warmup_geq_duration() {
        let mut s = good_spec();
        s.warmup = s.duration;
        let err = validate(&s, &ValidationContext::default()).unwrap_err();
        let fields: Vec<_> = err.violations.iter().map(|v| v.field.as_str()).collect();
        assert!(fields.contains(&"warmup"));
    }

    #[test]
    fn rejects_misaligned_block_size() {
        let mut s = good_spec();
        s.block_size = 100; // 不是 512 倍数
        let err = validate(&s, &ValidationContext::default()).unwrap_err();
        assert!(err
            .violations
            .iter()
            .any(|v| v.field == "block_size" && v.message.contains("aligned")));
    }

    #[test]
    fn rejects_direct_io_without_alignment() {
        let mut s = good_spec();
        s.direct_io = true;
        s.io_aligned = false;
        let err = validate(&s, &ValidationContext::default()).unwrap_err();
        assert!(err.violations.iter().any(|v| v.field == "direct_io"));
    }

    #[test]
    fn rejects_no_workload() {
        let mut s = good_spec();
        s.read = None;
        s.write = None;
        s.metadata = None;
        let err = validate(&s, &ValidationContext::default()).unwrap_err();
        assert!(err.violations.iter().any(|v| v.field == "spec"));
    }

    #[test]
    fn rejects_iops_rate_for_read() {
        let mut s = good_spec();
        if let Some(read) = s.read.as_mut() {
            read.rate_limit = Some(RateLimit::Iops { ops_per_sec: 100 });
        }
        let err = validate(&s, &ValidationContext::default()).unwrap_err();
        assert!(err.violations.iter().any(|v| v.field == "read.rate_limit"));
    }

    #[test]
    fn rejects_throughput_rate_for_metadata() {
        let mut s = good_spec();
        s.metadata = Some(MetadataConfig {
            concurrency: 1,
            dir: "bench/meta".into(),
            dir_manifest: None,
            ops: vec![MetadataOp::Stat],
            read_only: false,
            read_only_scan_limit: 0,
            depth: 1,
            width: 1,
            files_per_dir: 1,
            layout_concurrency: 1,
            think_time: None,
            rate_limit: Some(RateLimit::Throughput {
                bytes_per_sec: 1_000_000,
            }),
        });
        let err = validate(&s, &ValidationContext::default()).unwrap_err();
        assert!(err
            .violations
            .iter()
            .any(|v| v.field == "metadata.rate_limit"));
    }

    #[test]
    fn rejects_layout_softcap_overflow() {
        let mut s = good_spec();
        s.metadata = Some(MetadataConfig {
            concurrency: 1,
            dir: "bench/meta".into(),
            dir_manifest: None,
            ops: vec![MetadataOp::Stat],
            read_only: false,
            read_only_scan_limit: 0,
            depth: 8,
            width: 1000,
            files_per_dir: 1,
            layout_concurrency: 1,
            think_time: None,
            rate_limit: None,
        });
        let err = validate(&s, &ValidationContext::default()).unwrap_err();
        // 总目录数 = 1000 + 1000^2 + ... 远超 100_000
        assert!(err.violations.iter().any(|v| v.field == "metadata.layout"));
    }

    #[test]
    fn rejects_unsafe_paths() {
        let mut s = good_spec();
        s.write = Some(WriteConfig {
            concurrency: 1,
            dir: "/abs".into(),
            size: WriteSize::Fixed { bytes: 4096 },
            fsync: false,
            cleanup: false,
            think_time: None,
            rate_limit: None,
            verify_after_write: false,
        });
        let err = validate(&s, &ValidationContext::default()).unwrap_err();
        assert!(err.violations.iter().any(|v| v.field == "write.dir"));
    }

    #[test]
    fn rejects_size_range_min_gt_max() {
        let mut s = good_spec();
        s.write = Some(WriteConfig {
            concurrency: 1,
            dir: "bench/write".into(),
            size: WriteSize::Range {
                min: 1_000_000,
                max: 1_000,
                distribution: SizeDistribution::Uniform,
            },
            fsync: false,
            cleanup: false,
            think_time: None,
            rate_limit: None,
            verify_after_write: false,
        });
        let err = validate(&s, &ValidationContext::default()).unwrap_err();
        assert!(err
            .violations
            .iter()
            .any(|v| v.field == "write.file_size_range"));
    }

    #[test]
    fn rejects_consistency_with_empty_credentials() {
        let mut s = good_spec();
        if let Some(read) = s.read.as_mut() {
            read.s3_consistency_check = Some(ConsistencyConfig {
                bucket_name: "b".into(),
                bucket_url: String::new(),
                access_key: String::new(),
                secret_key: String::new(),
                region: "us-east-1".into(),
                prefix: String::new(),
                session_token: String::new(),
            });
        }
        let err = validate(&s, &ValidationContext::default()).unwrap_err();
        let fields: Vec<_> = err.violations.iter().map(|v| v.field.as_str()).collect();
        assert!(fields.contains(&"read.s3_consistency_check.bucket_url"));
        assert!(fields.contains(&"read.s3_consistency_check.access_key"));
        assert!(fields.contains(&"read.s3_consistency_check.secret_key"));
    }

    #[test]
    fn allows_redacted_consistency_credentials_for_worker_validation() {
        let mut s = good_spec();
        if let Some(read) = s.read.as_mut() {
            read.s3_consistency_check = Some(ConsistencyConfig {
                bucket_name: "b".into(),
                bucket_url: "http://127.0.0.1:9000".into(),
                access_key: crate::spec::REDACTED.into(),
                secret_key: crate::spec::REDACTED.into(),
                region: "us-east-1".into(),
                prefix: String::new(),
                session_token: String::new(),
            });
        }

        validate(
            &s,
            &ValidationContext {
                allow_redacted_consistency_credentials: true,
                ..ValidationContext::default()
            },
        )
        .unwrap();

        let err = validate(&s, &ValidationContext::default()).unwrap_err();
        assert!(err
            .violations
            .iter()
            .any(|v| v.field == "read.s3_consistency_check.access_key"));
    }

    #[test]
    fn rejects_metadata_stat_or_open_with_zero_files_per_dir() {
        let mut s = good_spec();
        s.read = None;
        s.metadata = Some(MetadataConfig {
            concurrency: 1,
            dir: "bench/meta".into(),
            dir_manifest: None,
            ops: vec![MetadataOp::Stat, MetadataOp::Mkdir],
            read_only: false,
            read_only_scan_limit: 0,
            depth: 1,
            width: 1,
            files_per_dir: 0,
            layout_concurrency: 1,
            think_time: None,
            rate_limit: None,
        });
        let err = validate(&s, &ValidationContext::default()).unwrap_err();
        assert!(err
            .violations
            .iter()
            .any(|v| v.field == "metadata.files_per_dir"));
    }

    #[test]
    fn read_only_metadata_allows_existing_layout_without_prebuild_shape() {
        let mut s = good_spec();
        s.read = None;
        s.metadata = Some(MetadataConfig {
            concurrency: 1,
            dir: "existing/meta".into(),
            dir_manifest: None,
            ops: vec![MetadataOp::Stat, MetadataOp::Open, MetadataOp::Readdir],
            read_only: true,
            read_only_scan_limit: 0,
            depth: 0,
            width: 0,
            files_per_dir: 0,
            layout_concurrency: 1,
            think_time: None,
            rate_limit: None,
        });
        validate(&s, &ValidationContext::default()).unwrap();
    }

    #[test]
    fn read_only_metadata_rejects_mutating_ops() {
        let mut s = good_spec();
        s.read = None;
        s.metadata = Some(MetadataConfig {
            concurrency: 1,
            dir: "existing/meta".into(),
            dir_manifest: None,
            ops: vec![MetadataOp::Stat, MetadataOp::Create],
            read_only: true,
            read_only_scan_limit: 0,
            depth: 0,
            width: 0,
            files_per_dir: 0,
            layout_concurrency: 1,
            think_time: None,
            rate_limit: None,
        });
        let err = validate(&s, &ValidationContext::default()).unwrap_err();
        assert!(err.violations.iter().any(|v| v.field == "metadata.ops"));
    }
}

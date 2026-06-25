//! 路径安全校验：拒绝绝对路径 / `..` / NUL 字节，并提供
//! `mount_point + 相对路径` 拼接 + canonicalize 兜底（spec §9.1 / §9.8）。

use std::path::{Path, PathBuf};

use thiserror::Error;

#[derive(Debug, Error, PartialEq, Eq)]
pub enum PathSafeError {
    #[error("path is empty")]
    Empty,
    #[error("path {path:?} contains NUL byte")]
    NulByte { path: String },
    #[error("path {path:?} must be relative (no leading '/')")]
    MustBeRelative { path: String },
    #[error("path {path:?} contains forbidden component {component:?}")]
    ForbiddenComponent { path: String, component: String },
}

/// 校验 spec 中相对路径字段（manifest 行、`write.dir`、`metadata.dir`）的安全性。
///
/// 规则：
/// 1. 非空；
/// 2. 不含 NUL；
/// 3. 必须是相对路径（不能以 `/` 开头、不能含 Windows-style root，本项目 Linux-only）；
/// 4. 任何 `..` / `.` / 根组件都拒绝（spec §9.1：「不允许包含 `..` 的路径」）。
pub fn validate_relative(path: &str) -> Result<(), PathSafeError> {
    if path.is_empty() {
        return Err(PathSafeError::Empty);
    }
    if path.contains('\0') {
        return Err(PathSafeError::NulByte {
            path: path.to_owned(),
        });
    }
    if path.starts_with('/') {
        return Err(PathSafeError::MustBeRelative {
            path: path.to_owned(),
        });
    }

    // 注意：不能使用 `Path::components()` 来检查 `.` —— 它会把中间的 `.` 规范化掉，
    // 因此 `a/./b` 会被当成 `a, b`。这里按原始字符串切段。
    for segment in path.split('/') {
        match segment {
            "" => continue, // 重复 `/` 或末尾斜杠（前导已在上面拒掉）
            "." => {
                return Err(PathSafeError::ForbiddenComponent {
                    path: path.to_owned(),
                    component: ".".to_owned(),
                });
            }
            ".." => {
                return Err(PathSafeError::ForbiddenComponent {
                    path: path.to_owned(),
                    component: "..".to_owned(),
                });
            }
            _ => {}
        }
    }

    Ok(())
}

/// 拼接 `mount_point + 相对路径`，返回字面拼接结果。
///
/// 不做 canonicalize（避免跨进程共享 IO）；
/// 只用于 spec 校验阶段产生的可读形态字符串。
/// canonicalize + escape 检测见 [`canonicalize_within_mount`]。
pub fn join_under_mount(mount_point: &Path, relative: &str) -> Result<PathBuf, PathSafeError> {
    validate_relative(relative)?;
    Ok(mount_point.join(relative))
}

/// 把相对路径规范化为统一的 `/` 分隔形式（spec §9.1）：
/// - 去除前导 `./`
/// - 折叠重复 `/`
/// - 拒绝 `\` 反斜杠（Linux 唯一目标平台）
/// - 拒绝 `.` / `..` / NUL（沿用 [`validate_relative`] 规则）
///
/// 输入已经合法时返回去重 `/` 后的字符串；非法返回 `PathSafeError`。
pub fn normalize_relative(path: &str) -> Result<String, PathSafeError> {
    if path.contains('\\') {
        return Err(PathSafeError::ForbiddenComponent {
            path: path.to_owned(),
            component: "\\".to_owned(),
        });
    }
    // 前导 `./` 在 validate_relative 中会被 `.` 拒掉；这里先 strip 一次让用户能写 `./foo`
    // 也走得通的 v1 行为可移到 dir_scan 调用前。但本函数语义是「严格 normalize」，
    // 所以保留拒绝。
    validate_relative(path)?;
    // 折叠重复 `/`
    let mut out = String::with_capacity(path.len());
    let mut prev_slash = false;
    for ch in path.chars() {
        if ch == '/' {
            if prev_slash {
                continue;
            }
            prev_slash = true;
        } else {
            prev_slash = false;
        }
        out.push(ch);
    }
    // 末尾 `/` 也去掉
    while out.ends_with('/') {
        out.pop();
    }
    if out.is_empty() {
        return Err(PathSafeError::Empty);
    }
    Ok(out)
}

/// 拼接并 canonicalize；要求结果仍处于 `mount_point` 之下，否则视为越界。
///
/// 注意：调用者必须保证 `mount_point` 已经是 canonical 的（master 启动时校验一次即可）。
pub fn canonicalize_within_mount(
    mount_point_canonical: &Path,
    relative: &str,
) -> Result<PathBuf, CanonicalizeError> {
    let joined =
        join_under_mount(mount_point_canonical, relative).map_err(CanonicalizeError::PathSafe)?;
    let canon = std::fs::canonicalize(&joined).map_err(|err| CanonicalizeError::Io {
        path: joined.clone(),
        source: err,
    })?;
    if !canon.starts_with(mount_point_canonical) {
        return Err(CanonicalizeError::EscapesMount {
            mount_point: mount_point_canonical.to_path_buf(),
            target: canon,
        });
    }
    Ok(canon)
}

#[derive(Debug, Error)]
pub enum CanonicalizeError {
    #[error(transparent)]
    PathSafe(PathSafeError),
    #[error("canonicalize {path:?} failed: {source}")]
    Io {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("path {target:?} escapes mount_point {mount_point:?}")]
    EscapesMount {
        mount_point: PathBuf,
        target: PathBuf,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepts_simple_relative() {
        validate_relative("bench/file001.dat").unwrap();
        validate_relative("foo").unwrap();
        validate_relative("a/b/c.txt").unwrap();
    }

    #[test]
    fn rejects_empty() {
        assert_eq!(validate_relative(""), Err(PathSafeError::Empty));
    }

    #[test]
    fn rejects_absolute() {
        assert!(matches!(
            validate_relative("/etc/passwd"),
            Err(PathSafeError::MustBeRelative { .. })
        ));
    }

    #[test]
    fn rejects_parent_dir() {
        assert!(matches!(
            validate_relative("../etc/passwd"),
            Err(PathSafeError::ForbiddenComponent { component, .. }) if component == ".."
        ));
        assert!(matches!(
            validate_relative("a/../b"),
            Err(PathSafeError::ForbiddenComponent { component, .. }) if component == ".."
        ));
    }

    #[test]
    fn rejects_current_dir_component() {
        assert!(matches!(
            validate_relative("./a"),
            Err(PathSafeError::ForbiddenComponent { component, .. }) if component == "."
        ));
        assert!(matches!(
            validate_relative("a/./b"),
            Err(PathSafeError::ForbiddenComponent { component, .. }) if component == "."
        ));
    }

    #[test]
    fn rejects_nul_byte() {
        assert!(matches!(
            validate_relative("a\0b"),
            Err(PathSafeError::NulByte { .. })
        ));
    }

    #[test]
    fn join_concatenates_without_canonicalize() {
        let mount = Path::new("/mnt/examplefs");
        let joined = join_under_mount(mount, "bench/x").unwrap();
        assert_eq!(joined, Path::new("/mnt/examplefs/bench/x"));
    }

    #[test]
    fn normalize_collapses_double_slashes() {
        assert_eq!(normalize_relative("a//b///c").unwrap(), "a/b/c");
    }

    #[test]
    fn normalize_rejects_backslash() {
        assert!(matches!(
            normalize_relative("a\\b"),
            Err(PathSafeError::ForbiddenComponent { component, .. }) if component == "\\"
        ));
    }

    #[test]
    fn normalize_strips_trailing_slash() {
        assert_eq!(normalize_relative("a/b/").unwrap(), "a/b");
    }
}

//! worker_id 生成与字符集校验。
//!
//! Spec §6.1：格式 `<hostname>-<pid>-<startup-uuid8>`，字符集限定
//! `[A-Za-z0-9._-]`，可直接作为路径组件而无需转义；同机多 worker 通过
//! `pid + uuid8` 区分；进程生命周期内不变。

use std::process;

use thiserror::Error;
use uuid::Uuid;

/// worker_id 长度上限（路径组件、日志 tag 友好）。
pub const MAX_LEN: usize = 128;

#[derive(Debug, Error, PartialEq, Eq)]
pub enum WorkerIdError {
    #[error("worker_id is empty")]
    Empty,
    #[error("worker_id length {0} exceeds {MAX_LEN}")]
    TooLong(usize),
    #[error("worker_id contains forbidden character {ch:?}")]
    BadChar { ch: char },
}

/// 生成新的 worker_id：`<hostname>-<pid>-<uuid8>`。
///
/// `hostname` 取 `gethostname()`，并把不在白名单的字符替换为 `_`，避免出现
/// 不能直接作为路径组件的字符；如果系统返回的 hostname 为空，使用字面量 `unknown`。
#[must_use]
pub fn generate() -> String {
    let host = sanitize_hostname(&hostname_or_default());
    let pid = process::id();
    let uuid8 = uuid8();
    format!("{host}-{pid}-{uuid8}")
}

/// 校验给定字符串是否符合 worker_id 字符集（`[A-Za-z0-9._-]`，非空，长度 ≤ MAX_LEN）。
pub fn validate(id: &str) -> Result<(), WorkerIdError> {
    if id.is_empty() {
        return Err(WorkerIdError::Empty);
    }
    if id.len() > MAX_LEN {
        return Err(WorkerIdError::TooLong(id.len()));
    }
    for ch in id.chars() {
        if !is_allowed(ch) {
            return Err(WorkerIdError::BadChar { ch });
        }
    }
    Ok(())
}

const fn is_allowed(ch: char) -> bool {
    ch.is_ascii_alphanumeric() || matches!(ch, '.' | '_' | '-')
}

fn sanitize_hostname(raw: &str) -> String {
    let cleaned: String = raw
        .chars()
        .map(|c| if is_allowed(c) { c } else { '_' })
        .collect();
    if cleaned.is_empty() {
        "unknown".to_owned()
    } else {
        cleaned
    }
}

fn uuid8() -> String {
    let uuid = Uuid::new_v4();
    let simple = uuid.simple().to_string();
    simple[..8].to_owned()
}

fn hostname_or_default() -> String {
    // 我们不引入额外 crate；直接读 /proc/sys/kernel/hostname（Linux 唯一目标平台）。
    std::fs::read_to_string("/proc/sys/kernel/hostname")
        .ok()
        .map(|s| s.trim_end_matches('\n').to_owned())
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "unknown".to_owned())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generate_matches_grammar_and_validates() {
        let id = generate();
        validate(&id).unwrap_or_else(|e| panic!("generated id {id:?} failed validate: {e}"));
        assert!(id.matches('-').count() >= 2, "missing dashes in {id}");
    }

    #[test]
    fn validate_accepts_canonical_examples() {
        validate("node12-3941-7af2c910").unwrap();
        validate("host.with.dots-1-abc12345").unwrap();
        validate("Host_Name-1-abc12345").unwrap();
    }

    #[test]
    fn validate_rejects_empty() {
        assert_eq!(validate(""), Err(WorkerIdError::Empty));
    }

    #[test]
    fn validate_rejects_forbidden_chars() {
        assert!(matches!(
            validate("node 12-3941-7af2c910"),
            Err(WorkerIdError::BadChar { ch: ' ' })
        ));
        assert!(matches!(
            validate("node/12-3941-7af2c910"),
            Err(WorkerIdError::BadChar { ch: '/' })
        ));
        assert!(matches!(
            validate("nodeα-3941-7af2c910"),
            Err(WorkerIdError::BadChar { .. })
        ));
    }

    #[test]
    fn validate_rejects_too_long() {
        let id = "a".repeat(MAX_LEN + 1);
        assert_eq!(validate(&id), Err(WorkerIdError::TooLong(MAX_LEN + 1)));
    }

    #[test]
    fn sanitize_hostname_replaces_disallowed() {
        assert_eq!(sanitize_hostname("foo.bar"), "foo.bar");
        assert_eq!(sanitize_hostname("foo bar"), "foo_bar");
        assert_eq!(sanitize_hostname("foo/bar:baz"), "foo_bar_baz");
        assert_eq!(sanitize_hostname(""), "unknown");
    }
}

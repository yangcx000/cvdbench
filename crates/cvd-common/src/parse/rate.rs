//! `<size>/s` 或 `<num>iops`。
//!
//! Spec §9.7：read/write 只允许 throughput rate，metadata 只允许 iops rate；
//! 上述 per-context 约束由 [`crate::spec::validate`] 校验，本解析器接受两种形态。

use super::{size::parse_size_field, ParseError};

const FIELD: &str = "rate_limit";

/// 解析后的 rate limit。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RateLimit {
    /// 单位：字节/秒。
    Throughput { bytes_per_sec: u64 },
    /// 单位：操作次数/秒。
    Iops { ops_per_sec: u64 },
}

/// 解析 spec §9.7 rate_limit 字符串。
///
/// 空字符串返回 `Ok(None)`；其它非法形态返回 [`ParseError`]。
pub fn parse_rate_limit(input: &str) -> Result<Option<RateLimit>, ParseError> {
    let trimmed = input.trim();
    if trimmed.is_empty() {
        return Ok(None);
    }

    if let Some(num_str) = trimmed.strip_suffix("iops") {
        let num_str = num_str.trim_end();
        if num_str.is_empty() {
            return Err(ParseError::MissingNumber {
                field: FIELD,
                input: input.to_owned(),
            });
        }
        let ops: u64 = num_str.parse().map_err(|_| ParseError::InvalidNumber {
            field: FIELD,
            number: num_str.to_owned(),
            input: input.to_owned(),
        })?;
        return Ok(Some(RateLimit::Iops { ops_per_sec: ops }));
    }

    if let Some(size_part) = trimmed.strip_suffix("/s") {
        let bytes =
            parse_size_field(size_part, FIELD)?.ok_or_else(|| ParseError::MissingNumber {
                field: FIELD,
                input: input.to_owned(),
            })?;
        return Ok(Some(RateLimit::Throughput {
            bytes_per_sec: bytes,
        }));
    }

    Err(ParseError::InvalidShape {
        field: FIELD,
        input: input.to_owned(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[track_caller]
    fn ok_throughput(input: &str, expect: u64) {
        assert_eq!(
            parse_rate_limit(input).unwrap(),
            Some(RateLimit::Throughput {
                bytes_per_sec: expect
            }),
            "parse_rate_limit({input:?})"
        );
    }

    #[track_caller]
    fn ok_iops(input: &str, expect: u64) {
        assert_eq!(
            parse_rate_limit(input).unwrap(),
            Some(RateLimit::Iops {
                ops_per_sec: expect
            }),
            "parse_rate_limit({input:?})"
        );
    }

    #[track_caller]
    fn err(input: &str) {
        let r = parse_rate_limit(input);
        assert!(r.is_err(), "expected err, got {r:?} for input {input:?}");
    }

    #[test]
    fn empty_is_unset() {
        assert_eq!(parse_rate_limit("").unwrap(), None);
        assert_eq!(parse_rate_limit("   ").unwrap(), None);
    }

    #[test]
    fn throughput_si_and_iec() {
        ok_throughput("1GB/s", 1_000_000_000);
        ok_throughput("500MiB/s", 500 * (1u64 << 20));
        ok_throughput("1024B/s", 1024);
        ok_throughput("1024/s", 1024);
    }

    #[test]
    fn iops_form() {
        ok_iops("5000iops", 5000);
        ok_iops("1iops", 1);
    }

    #[test]
    fn rejects_mixed_or_unknown_suffix() {
        err("5000IOPS"); // 必须小写
        err("1KB/min");
        err("1KB"); // 缺 /s
        err("/s");
        err("iops");
        err("1.5KB/s");
        err("-1iops");
    }
}

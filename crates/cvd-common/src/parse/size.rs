//! `<num>(K|M|G|T|Ki|Mi|Gi|Ti|B)`，1K=1000，1Ki=1024，无单位视为字节。
//!
//! Spec §9.7 字面文法只列了 `K/Ki/.../B`，但同节 rate_limit 示例出现 `1GB/s` /
//! `500MiB/s`，所以本解析器额外接受可选的尾随 `B`：
//! `K | KB`、`Ki | KiB`、`M | MB`、`Mi | MiB`、`G | GB`、`Gi | GiB`、`T | TB`、`Ti | TiB`。
//! 单独 `B` 仍代表字节。整数语义、不接受小数。

use super::ParseError;

const FIELD: &str = "size";

/// 解析 spec §9.7 size 字符串，返回字节数。
///
/// 空字符串返回 `Ok(None)`；其它非法形态返回 [`ParseError`]。
pub fn parse_size(input: &str) -> Result<Option<u64>, ParseError> {
    parse_size_field(input, FIELD)
}

/// 内部入口：允许 rate parser 复用，但报错时使用调用方字段名。
pub(crate) fn parse_size_field(
    input: &str,
    field: &'static str,
) -> Result<Option<u64>, ParseError> {
    let trimmed = input.trim();
    if trimmed.is_empty() {
        return Ok(None);
    }

    let (num_str, suffix) = split_leading_digits(trimmed);
    if num_str.is_empty() {
        return Err(ParseError::MissingNumber {
            field,
            input: input.to_owned(),
        });
    }
    let value: u64 = num_str.parse().map_err(|_| ParseError::InvalidNumber {
        field,
        number: num_str.to_owned(),
        input: input.to_owned(),
    })?;

    let multiplier = unit_multiplier(suffix).ok_or_else(|| ParseError::InvalidUnit {
        field,
        unit: suffix.to_owned(),
        input: input.to_owned(),
    })?;

    let bytes = value
        .checked_mul(multiplier)
        .ok_or_else(|| ParseError::Overflow {
            field,
            input: input.to_owned(),
        })?;

    Ok(Some(bytes))
}

fn split_leading_digits(s: &str) -> (&str, &str) {
    let end = s
        .as_bytes()
        .iter()
        .position(|b| !b.is_ascii_digit())
        .unwrap_or(s.len());
    (&s[..end], &s[end..])
}

fn unit_multiplier(unit: &str) -> Option<u64> {
    match unit {
        "" | "B" => Some(1),

        "K" | "KB" => Some(1_000),
        "M" | "MB" => Some(1_000_000),
        "G" | "GB" => Some(1_000_000_000),
        "T" | "TB" => Some(1_000_000_000_000),

        "Ki" | "KiB" => Some(1 << 10),
        "Mi" | "MiB" => Some(1 << 20),
        "Gi" | "GiB" => Some(1 << 30),
        "Ti" | "TiB" => Some(1 << 40),

        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[track_caller]
    fn ok(input: &str, expect: u64) {
        assert_eq!(
            parse_size(input).unwrap(),
            Some(expect),
            "parse_size({input:?})"
        );
    }

    #[track_caller]
    fn err(input: &str) {
        let r = parse_size(input);
        assert!(r.is_err(), "expected err, got {r:?} for input {input:?}");
    }

    #[test]
    fn empty_is_unset() {
        assert_eq!(parse_size("").unwrap(), None);
        assert_eq!(parse_size("   ").unwrap(), None);
    }

    #[test]
    fn no_unit_is_bytes() {
        ok("0", 0);
        ok("1024", 1024);
    }

    #[test]
    fn decimal_si_units() {
        ok("4K", 4_000);
        ok("4KB", 4_000);
        ok("1M", 1_000_000);
        ok("1MB", 1_000_000);
        ok("1G", 1_000_000_000);
        ok("1GB", 1_000_000_000);
        ok("2T", 2_000_000_000_000);
        ok("2TB", 2_000_000_000_000);
    }

    #[test]
    fn binary_iec_units() {
        ok("1Ki", 1024);
        ok("1KiB", 1024);
        ok("4Ki", 4 * 1024);
        ok("1Mi", 1 << 20);
        ok("1MiB", 1 << 20);
        ok("1Gi", 1 << 30);
        ok("1GiB", 1 << 30);
        ok("1Ti", 1u64 << 40);
        ok("1TiB", 1u64 << 40);
    }

    #[test]
    fn bare_b() {
        ok("1B", 1);
        ok("4096B", 4096);
    }

    #[test]
    fn rejects_unknown_unit() {
        err("1k"); // 小写
        err("1KIB"); // 全大写
        err("1Kib");
        err("1XB");
        err("KB");
    }

    #[test]
    fn rejects_garbage() {
        err("abc");
        err("-1");
        err("1.5K");
        err("1K2");
    }

    #[test]
    fn detects_overflow() {
        let too_many_ti = (u64::MAX / (1u64 << 40)) + 1;
        err(&format!("{too_many_ti}Ti"));
    }
}

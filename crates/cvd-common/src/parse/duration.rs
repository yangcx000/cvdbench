//! `<num>(h|m|s|ms)` 串联，例如 `1h30m`、`500ms`。空字符串视为 unset。
//!
//! 匹配优先级：`ms` > `h` / `m` / `s`，避免 `500ms` 被吃成 `500m + s`。

use std::time::Duration;

use super::ParseError;

const FIELD: &str = "duration";

/// 解析 spec §9.7 duration 字符串。
///
/// 空字符串返回 `Ok(None)`；其它非法形态返回 [`ParseError`]。
pub fn parse_duration(input: &str) -> Result<Option<Duration>, ParseError> {
    let trimmed = input.trim();
    if trimmed.is_empty() {
        return Ok(None);
    }

    let mut total_ms: u64 = 0;
    let mut rest = trimmed;

    while !rest.is_empty() {
        let (num_str, after_num) = split_leading_digits(rest);
        if num_str.is_empty() {
            return Err(ParseError::MissingNumber {
                field: FIELD,
                input: input.to_owned(),
            });
        }
        let value: u64 = num_str.parse().map_err(|_| ParseError::InvalidNumber {
            field: FIELD,
            number: num_str.to_owned(),
            input: input.to_owned(),
        })?;

        let (unit, after_unit) = split_leading_unit(after_num);
        let unit_ms: u64 = match unit {
            "ms" => 1,
            "s" => 1_000,
            "m" => 60_000,
            "h" => 3_600_000,
            "" => {
                return Err(ParseError::InvalidShape {
                    field: FIELD,
                    input: input.to_owned(),
                });
            }
            other => {
                return Err(ParseError::InvalidUnit {
                    field: FIELD,
                    unit: other.to_owned(),
                    input: input.to_owned(),
                });
            }
        };

        let segment_ms = value
            .checked_mul(unit_ms)
            .ok_or_else(|| ParseError::Overflow {
                field: FIELD,
                input: input.to_owned(),
            })?;
        total_ms = total_ms
            .checked_add(segment_ms)
            .ok_or_else(|| ParseError::Overflow {
                field: FIELD,
                input: input.to_owned(),
            })?;

        rest = after_unit;
    }

    Ok(Some(Duration::from_millis(total_ms)))
}

fn split_leading_digits(s: &str) -> (&str, &str) {
    let end = s
        .as_bytes()
        .iter()
        .position(|b| !b.is_ascii_digit())
        .unwrap_or(s.len());
    (&s[..end], &s[end..])
}

/// 在剩余串首匹配单位字符；优先匹配 `ms`，再降级匹配单字符 `h` / `m` / `s`，
/// 其它任意字母片段返回给调用方报错。
fn split_leading_unit(s: &str) -> (&str, &str) {
    if let Some(rest) = s.strip_prefix("ms") {
        return ("ms", rest);
    }
    let mut chars = s.chars();
    match chars.next() {
        Some(c) if c.is_ascii_alphabetic() => {
            let unit_end = c.len_utf8();
            (&s[..unit_end], &s[unit_end..])
        }
        // 数字 / 空 / 非字母字符：返回空 unit，由调用方判定
        _ => ("", s),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[track_caller]
    fn ok(input: &str, expect_ms: u64) {
        assert_eq!(
            parse_duration(input).unwrap(),
            Some(Duration::from_millis(expect_ms)),
            "parse_duration({input:?})"
        );
    }

    #[track_caller]
    fn err(input: &str) {
        let r = parse_duration(input);
        assert!(r.is_err(), "expected err, got {r:?} for input {input:?}");
    }

    #[test]
    fn empty_is_unset() {
        assert_eq!(parse_duration("").unwrap(), None);
        assert_eq!(parse_duration("   ").unwrap(), None);
    }

    #[test]
    fn single_units() {
        ok("500ms", 500);
        ok("0s", 0);
        ok("30s", 30_000);
        ok("30m", 30 * 60_000);
        ok("2h", 2 * 3_600_000);
    }

    #[test]
    fn concatenated() {
        ok("1h30m", 5_400_000);
        ok("1h30m45s", 5_445_000);
        ok("1h500ms", 3_600_500);
        ok("0h0m0s0ms", 0);
    }

    #[test]
    fn ms_priority_over_m() {
        // 关键：500ms 必须解析为 500 毫秒，而不是 500 分钟 + s
        ok("500ms", 500);
    }

    #[test]
    fn sums_repeated_units() {
        // 语法没禁止重复单位；解析器累加。
        ok("1h2h", 3 * 3_600_000);
    }

    #[test]
    fn rejects_missing_unit() {
        err("1");
        err("1h30");
    }

    #[test]
    fn rejects_unknown_unit() {
        err("1d");
        err("1H");
        err("1S");
    }

    #[test]
    fn rejects_missing_number() {
        err("h");
        err("1hms");
    }

    #[test]
    fn rejects_negative_or_garbage() {
        err("-1s");
        err("abc");
        err("1h-30m");
    }

    #[test]
    fn detects_overflow() {
        // (u64::MAX / 3_600_000) * 1h 还乘以单位会越界
        let too_many_hours = (u64::MAX / 3_600_000) + 1;
        err(&format!("{too_many_hours}h"));
    }
}

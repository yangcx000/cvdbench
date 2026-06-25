//! Spec §9.7 字符串字段语法：`duration`、`size`、`rate_limit`。
//!
//! 三种字段统一约定：**空字符串视为 unset**，由解析函数返回 `Ok(None)`，
//! 调用方自行判断 unset 的语义（不限速 / 不等待 / 使用默认值）。

use thiserror::Error;

pub mod duration;
pub mod rate;
pub mod size;

pub use duration::parse_duration;
pub use rate::{parse_rate_limit, RateLimit};
pub use size::parse_size;

/// 解析失败的统一错误类型。
///
/// 所有变体携带原始 `input`，方便错误信息直接定位到用户输入。
#[derive(Debug, Error, PartialEq, Eq)]
pub enum ParseError {
    #[error("{field} is missing a numeric prefix in {input:?}")]
    MissingNumber { field: &'static str, input: String },

    #[error("{field} number {number:?} is not a non-negative integer in {input:?}")]
    InvalidNumber {
        field: &'static str,
        number: String,
        input: String,
    },

    #[error("{field} unit {unit:?} is not recognised in {input:?}")]
    InvalidUnit {
        field: &'static str,
        unit: String,
        input: String,
    },

    #[error("{field} value {input:?} overflows u64")]
    Overflow { field: &'static str, input: String },

    #[error("{field} value {input:?} does not match expected grammar")]
    InvalidShape { field: &'static str, input: String },
}

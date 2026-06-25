//! 错误类型。后续按子模块逐步细化。

use thiserror::Error;

/// cvdbench 顶层错误（占位，后续按域细化）。
#[derive(Debug, Error)]
pub enum CvbError {
    #[error("validation failed: {0}")]
    Validate(String),
    #[error("parse error: {0}")]
    Parse(String),
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
}

/// 校验类错误（spec §9.4 / §9.7 / §9.6 共享）。
#[derive(Debug, Error)]
#[error("{message}")]
pub struct ValidateError {
    pub message: String,
}

impl ValidateError {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

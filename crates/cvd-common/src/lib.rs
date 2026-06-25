//! cvdbench 共享域类型与基础设施。
//!
//! 模块组织遵循 spec.md §1，对外暴露：
//! - [`error`]   ：错误类型；
//! - [`id`]      ：worker_id 生成与校验；
//! - [`path_safe`]：相对路径安全校验与 mount_point 拼接；
//! - [`parse`]   ：duration/size/rate_limit 字符串语法（spec §9.7）；
//! - [`spec`]    ：业务侧 BenchSpec、proto 转换、校验（§9.4）、凭据脱敏（§9.5）；
//! - [`metrics`] ：HDR histogram + 多 worker 聚合（§4.2）；
//! - [`output`]  ：JSON / CSV 终结结果输出（§9.3）。

pub mod error;
pub mod id;
pub mod metrics;
pub mod output;
pub mod parse;
pub mod path_safe;
pub mod spec;

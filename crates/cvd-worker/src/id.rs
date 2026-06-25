//! worker_id：薄封装 [`cvd_common::id`]，启动时生成一次，进程生命周期内不变。
//!
//! 见 spec §6.1。

pub use cvd_common::id::{generate, validate, WorkerIdError};

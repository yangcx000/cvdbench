//! cvdbench master：gRPC server + 内存状态机 + manifest 生产 + 结果聚合。

pub mod aggregate;
pub mod config;
pub mod events;
pub mod gc;
pub mod manifest;
pub mod metrics;
pub mod scheduler;
pub mod service;
pub mod state;

//! cvdbench worker daemon：纯 gRPC client，主循环常驻轮询 FetchJob。

pub mod backoff;
pub mod cancel;
pub mod client;
pub mod clock;
pub mod fs_io;
pub mod id;
pub mod lifecycle;
pub mod metrics;
pub mod pipeline;
pub mod prebuild;
pub mod progress;
pub mod rate_limit;
pub mod runner;

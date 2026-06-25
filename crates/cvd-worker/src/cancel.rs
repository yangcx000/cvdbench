//! Fail-fast abort signal：`AtomicBool` + `tokio::sync::Notify`，
//! 首次置位的 task 是赢家，错误信息写入 worker 维度 `error`（spec §6.7）。

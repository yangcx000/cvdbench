//! Fetcher：单 task 循环 `FetchFileBatch` → 写入有界 `local_queue`
//! （cap = batch_size × 4）。处理 cancelled / has_more=false / unknown_job。

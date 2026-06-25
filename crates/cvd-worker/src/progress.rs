//! 周期性 ReportProgress task：独立于 IO task，避免被 I/O 阻塞拖死活性判定。

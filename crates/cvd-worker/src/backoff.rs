//! 指数退避（spec §6.9）：所有 RPC 失败统一走 200ms → 30s 倍增，无限重试。
//!
//! 用法：
//! ```ignore
//! let mut backoff = ExponentialBackoff::default();
//! loop {
//!     match client.some_rpc(req).await {
//!         Ok(r) => { backoff.reset(); ... }
//!         Err(_) => tokio::time::sleep(backoff.next_delay()).await,
//!     }
//! }
//! ```

use std::time::Duration;

/// 默认起始 200ms，最大 30s，倍率 2.0。
const DEFAULT_INITIAL: Duration = Duration::from_millis(200);
const DEFAULT_MAX: Duration = Duration::from_secs(30);
const DEFAULT_FACTOR: f64 = 2.0;

#[derive(Debug, Clone)]
pub struct ExponentialBackoff {
    initial: Duration,
    max: Duration,
    factor: f64,
    current: Duration,
}

impl Default for ExponentialBackoff {
    fn default() -> Self {
        Self::new(DEFAULT_INITIAL, DEFAULT_MAX, DEFAULT_FACTOR)
    }
}

impl ExponentialBackoff {
    #[must_use]
    pub fn new(initial: Duration, max: Duration, factor: f64) -> Self {
        let factor = factor.max(1.0);
        let initial = if initial.is_zero() {
            DEFAULT_INITIAL
        } else {
            initial
        };
        let max = if max < initial { initial } else { max };
        Self {
            initial,
            max,
            factor,
            current: initial,
        }
    }

    /// 取下一次 sleep 时长，并把 current 倍增（封顶 max）。
    pub fn next_delay(&mut self) -> Duration {
        let d = self.current;
        // 倍增；用 f64 避免 Duration 自身的 Mul 溢出。
        #[allow(
            clippy::cast_precision_loss,
            clippy::cast_possible_truncation,
            clippy::cast_sign_loss
        )]
        let next_ms = (self.current.as_millis() as f64 * self.factor) as u64;
        let next = Duration::from_millis(next_ms.max(1));
        self.current = if next > self.max { self.max } else { next };
        d
    }

    /// 操作成功后调用，让下次失败重新从 initial 开始。
    pub fn reset(&mut self) {
        self.current = self.initial;
    }

    /// 当前未消费的 delay（通常仅用于测试）。
    #[must_use]
    pub fn peek(&self) -> Duration {
        self.current
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn doubles_each_call() {
        let mut b = ExponentialBackoff::default();
        assert_eq!(b.next_delay(), Duration::from_millis(200));
        assert_eq!(b.next_delay(), Duration::from_millis(400));
        assert_eq!(b.next_delay(), Duration::from_millis(800));
        assert_eq!(b.next_delay(), Duration::from_millis(1_600));
    }

    #[test]
    fn caps_at_max() {
        let mut b = ExponentialBackoff::new(Duration::from_secs(10), Duration::from_secs(30), 2.0);
        assert_eq!(b.next_delay(), Duration::from_secs(10));
        // 倍增后 20s
        assert_eq!(b.next_delay(), Duration::from_secs(20));
        // 再倍增本应 40s，被夹到 30s
        assert_eq!(b.next_delay(), Duration::from_secs(30));
        // 之后保持 30s
        assert_eq!(b.next_delay(), Duration::from_secs(30));
    }

    #[test]
    fn reset_returns_to_initial() {
        let mut b = ExponentialBackoff::default();
        let _ = b.next_delay();
        let _ = b.next_delay();
        b.reset();
        assert_eq!(b.next_delay(), Duration::from_millis(200));
    }

    #[test]
    fn zero_initial_falls_back_to_default() {
        let b = ExponentialBackoff::new(Duration::ZERO, Duration::from_secs(1), 2.0);
        assert_eq!(b.peek(), DEFAULT_INITIAL);
    }
}

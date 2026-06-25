//! Token-bucket 限速：throughput rate 与 iops rate 两种刻度，per-worker 语义。
//!
//! 接受以 token 为单位的「需求量」：throughput 模式下 1 token = 1 字节，
//! iops 模式下 1 token = 1 op。capacity 默认等于 1s 的产能，足以容纳偶发突发。

use std::sync::Mutex;
use std::time::{Duration, Instant};

use cvd_common::parse::RateLimit;

#[derive(Debug)]
pub struct TokenBucket {
    rate_per_sec: f64,
    capacity: f64,
    state: Mutex<State>,
}

#[derive(Debug)]
struct State {
    tokens: f64,
    last_refill: Instant,
}

impl TokenBucket {
    pub fn new(rate_per_sec: u64) -> Self {
        let rate_per_sec = rate_per_sec.max(1) as f64;
        Self {
            rate_per_sec,
            capacity: rate_per_sec, // 1s burst
            state: Mutex::new(State {
                tokens: rate_per_sec, // 启动即满桶
                last_refill: Instant::now(),
            }),
        }
    }

    /// 等到能取到 `n` 个 token 为止。`n` 超过 capacity 时单次也允许通过（避免死锁）。
    pub async fn acquire(&self, n: u64) {
        let n = n.max(1) as f64;
        loop {
            let need_wait = {
                let mut s = self.state.lock().expect("token bucket state");
                let now = Instant::now();
                let elapsed = now.duration_since(s.last_refill).as_secs_f64();
                s.tokens = (s.tokens + elapsed * self.rate_per_sec).min(self.capacity);
                s.last_refill = now;
                if s.tokens >= n.min(self.capacity) {
                    s.tokens -= n;
                    return;
                }
                let needed = n.min(self.capacity) - s.tokens;
                Duration::from_secs_f64(needed / self.rate_per_sec)
            };
            tokio::time::sleep(need_wait).await;
        }
    }
}

/// 根据 [`RateLimit`] 构造合适的 bucket；`None` / 不匹配模式时返回 `None`。
#[must_use]
pub fn make_throughput_bucket(rate: Option<RateLimit>) -> Option<TokenBucket> {
    match rate? {
        RateLimit::Throughput { bytes_per_sec } => Some(TokenBucket::new(bytes_per_sec)),
        RateLimit::Iops { .. } => None,
    }
}

/// 根据 [`RateLimit`] 构造 IOPS bucket；`None` / 不匹配模式时返回 `None`。
#[must_use]
pub fn make_iops_bucket(rate: Option<RateLimit>) -> Option<TokenBucket> {
    match rate? {
        RateLimit::Iops { ops_per_sec } => Some(TokenBucket::new(ops_per_sec)),
        RateLimit::Throughput { .. } => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn acquire_returns_immediately_when_tokens_available() {
        let b = TokenBucket::new(1_000_000);
        let start = Instant::now();
        b.acquire(100).await;
        assert!(start.elapsed() < Duration::from_millis(50));
    }

    #[tokio::test]
    async fn acquire_throttles_to_rate() {
        // 100 ops/s = 100 tokens/s。初始满桶 100 个，再要 100 应该等约 1s。
        let b = TokenBucket::new(100);
        let start = Instant::now();
        b.acquire(100).await; // 消耗满桶
        b.acquire(100).await; // 再要 100 等 ~1s
        let elapsed = start.elapsed();
        assert!(
            elapsed >= Duration::from_millis(900),
            "expected ~1s throttle, got {elapsed:?}"
        );
    }
}

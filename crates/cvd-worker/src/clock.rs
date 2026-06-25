//! Master clock offset：master clock 与本地 `Instant` 的对齐工具（spec §6.4）。
//!
//! 用法：
//! - `from_response(master_now_ms, local_recv)`：FetchJob 响应到达时构造一次估算；
//! - `refine_with(...)`：每次后续 RPC 响应（ReportReady / progress 等）都带回
//!   `master_now_ms`，结合本地发送/接收时刻做 RTT 加权估计，单调地刷新偏移；
//! - `master_to_local(t)`：把 master ms 换算回本地 Instant。

use std::time::{Duration, Instant};

/// 一次估算结果：master clock 与本地 `Instant` 之间的偏移。
#[derive(Debug, Clone, Copy)]
pub struct MasterClockOffset {
    /// 本地 `Instant` 与 master 「接收时间」对齐的参考点。
    pub local_anchor: Instant,
    /// 与 `local_anchor` 同时刻 master 的 unix ms。
    pub master_anchor_ms: i64,
}

impl MasterClockOffset {
    /// 用 master 响应中的 `master_now_ms` 与本地接收时间构造 offset。
    ///
    /// 这里没有考虑 RTT/2 修正；首次估算就按响应到达瞬间对齐，后续可调
    /// `refine_with` 修正。
    #[must_use]
    pub fn from_response(master_now_ms: i64, local_recv: Instant) -> Self {
        Self {
            local_anchor: local_recv,
            master_anchor_ms: master_now_ms,
        }
    }

    /// 用一次 RPC 的「本地发送时刻 / 收到响应时刻 / master_now_ms」做单边估计。
    ///
    /// 假设 master 在 `master_now_ms` 时刻刚把响应送出，本地在
    /// `local_recv - rtt/2` 时刻看到的 master 时间约等于 `master_now_ms`，
    /// 因此本地 `local_recv` 时刻对齐到的 master ms ≈ `master_now_ms + rtt/2`。
    ///
    /// 仅在 RTT 看起来正常（≤ 5s）时才更新 anchor，避免被异常网络抖动污染。
    pub fn refine_with(&mut self, master_now_ms: i64, local_send: Instant, local_recv: Instant) {
        let rtt = local_recv.saturating_duration_since(local_send);
        if rtt > Duration::from_secs(5) {
            // 抖动太大，不刷新；保留旧 anchor。
            return;
        }
        let half_rtt_ms = i64::try_from(rtt.as_millis() / 2).unwrap_or(0);
        let aligned_master_ms = master_now_ms.saturating_add(half_rtt_ms);
        self.local_anchor = local_recv;
        self.master_anchor_ms = aligned_master_ms;
    }

    /// 当前估算的最近一次 RTT（仅用于诊断）。
    #[must_use]
    pub fn anchor_age(&self, now: Instant) -> Duration {
        now.saturating_duration_since(self.local_anchor)
    }

    /// 把 master 时间换算成本地 `Instant`。
    #[must_use]
    pub fn master_to_local(&self, master_target_ms: i64) -> Instant {
        if master_target_ms >= self.master_anchor_ms {
            let delta = u64::try_from(master_target_ms - self.master_anchor_ms).unwrap_or(0);
            self.local_anchor + Duration::from_millis(delta)
        } else {
            let delta = u64::try_from(self.master_anchor_ms - master_target_ms).unwrap_or(0);
            self.local_anchor
                .checked_sub(Duration::from_millis(delta))
                .unwrap_or(self.local_anchor)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip_through_master_to_local() {
        let now = Instant::now();
        let off = MasterClockOffset::from_response(1_000_000, now);
        // master 时间向前 500ms，本地 Instant 也向前 500ms。
        let later = off.master_to_local(1_000_500);
        assert!(later >= now + Duration::from_millis(499));
        assert!(later <= now + Duration::from_millis(501));
    }

    #[test]
    fn handles_master_time_in_past() {
        let now = Instant::now() + Duration::from_secs(10);
        let off = MasterClockOffset::from_response(1_000_500, now);
        let earlier = off.master_to_local(1_000_000);
        assert!(earlier <= now);
    }

    #[test]
    fn refine_with_updates_anchor_with_half_rtt() {
        let send = Instant::now();
        let mut off = MasterClockOffset::from_response(1_000_000, send);
        // 模拟 RTT=100ms：发送在 send，收到在 send+100ms，master 报 1_000_500（中点视角）
        let recv = send + Duration::from_millis(100);
        off.refine_with(1_000_500, send, recv);
        // anchor 改到 recv，master_anchor_ms = 1_000_500 + 50 = 1_000_550
        assert_eq!(off.local_anchor, recv);
        assert_eq!(off.master_anchor_ms, 1_000_550);
    }

    #[test]
    fn refine_with_ignores_extreme_rtt() {
        let send = Instant::now();
        let mut off = MasterClockOffset::from_response(1_000_000, send);
        // 巨大 RTT（10s），跳过
        let recv = send + Duration::from_secs(10);
        off.refine_with(2_000_000, send, recv);
        // anchor 不变
        assert_eq!(off.master_anchor_ms, 1_000_000);
    }
}

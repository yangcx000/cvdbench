//! PENDING FIFO 队列辅助函数（spec §5.4）。
//!
//! `MasterState::pending_queue` 直接是 `Mutex<VecDeque<String>>`。本模块的函数
//! 把惯用操作集中起来，避免在 service 层散落 `lock().unwrap()`。

use std::collections::VecDeque;

/// 入队。
pub fn push_back(queue: &mut VecDeque<String>, job_id: String) {
    queue.push_back(job_id);
}

/// 取队首（不弹出）。
pub fn peek_front(queue: &VecDeque<String>) -> Option<&String> {
    queue.front()
}

/// 弹出队首，仅当当前队首与 `expected` 相等时弹（防御性）。
pub fn pop_if_front(queue: &mut VecDeque<String>, expected: &str) -> bool {
    if queue.front().is_some_and(|s| s == expected) {
        queue.pop_front();
        true
    } else {
        false
    }
}

/// 移除任意位置的 `job_id`（DeleteJob 在 PENDING 时使用）。
pub fn remove(queue: &mut VecDeque<String>, job_id: &str) -> bool {
    if let Some(pos) = queue.iter().position(|s| s == job_id) {
        queue.remove(pos);
        true
    } else {
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pop_if_front_only_pops_match() {
        let mut q = VecDeque::from(["a".to_owned(), "b".to_owned()]);
        assert!(!pop_if_front(&mut q, "b"));
        assert_eq!(q.len(), 2);
        assert!(pop_if_front(&mut q, "a"));
        assert_eq!(q.front(), Some(&"b".to_owned()));
    }

    #[test]
    fn remove_non_front() {
        let mut q = VecDeque::from(["a".to_owned(), "b".to_owned(), "c".to_owned()]);
        assert!(remove(&mut q, "b"));
        assert_eq!(q, VecDeque::from(["a".to_owned(), "c".to_owned()]));
        assert!(!remove(&mut q, "z"));
    }
}

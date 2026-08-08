//! Ping/Pong timeout state - the Rust counterpart of Cyclone.Unity's
//! `CycloneHeartbeat`, using [`std::time::Instant`] (monotonic, immune to
//! system clock changes) in place of `DateTime.UtcNow`.

use std::time::{Duration, Instant};

pub struct CycloneHeartbeat {
    interval: Duration,
    timeout: Duration,
    last_pong: Instant,
}

impl CycloneHeartbeat {
    pub fn new(interval: Duration, timeout: Duration) -> Self {
        Self {
            interval,
            timeout,
            last_pong: Instant::now(),
        }
    }

    pub fn is_timeout(&self) -> bool {
        self.last_pong.elapsed() > self.timeout
    }

    pub fn should_ping(&self) -> bool {
        self.last_pong.elapsed() >= self.interval
    }

    pub fn mark_pong(&mut self) {
        self.last_pong = Instant::now();
    }

    pub fn reset(&mut self) {
        self.last_pong = Instant::now();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fresh_heartbeat_does_not_ping_or_time_out() {
        let heartbeat = CycloneHeartbeat::new(Duration::from_secs(5), Duration::from_secs(15));
        assert!(!heartbeat.should_ping());
        assert!(!heartbeat.is_timeout());
    }

    #[test]
    fn should_ping_after_the_interval_elapses() {
        let heartbeat = CycloneHeartbeat::new(Duration::from_millis(20), Duration::from_secs(15));
        std::thread::sleep(Duration::from_millis(30));
        assert!(heartbeat.should_ping());
        assert!(!heartbeat.is_timeout());
    }

    #[test]
    fn times_out_after_the_timeout_elapses() {
        let heartbeat = CycloneHeartbeat::new(Duration::from_millis(5), Duration::from_millis(20));
        std::thread::sleep(Duration::from_millis(30));
        assert!(heartbeat.is_timeout());
    }

    #[test]
    fn mark_pong_resets_both_checks() {
        let mut heartbeat = CycloneHeartbeat::new(Duration::from_millis(5), Duration::from_millis(20));
        std::thread::sleep(Duration::from_millis(30));
        assert!(heartbeat.is_timeout());
        heartbeat.mark_pong();
        assert!(!heartbeat.is_timeout());
        assert!(!heartbeat.should_ping());
    }
}

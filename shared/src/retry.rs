use std::{future::Future, time::Duration};
use tracing::warn;

pub trait DelayStrategy {
    // starts with zero!
    fn delay(&self, attempt: u32) -> Duration;
}

#[derive(Debug, Clone, Copy)]
pub struct ExponentialBackoff {
    base: Duration,
    max_delay: Duration,
}

impl ExponentialBackoff {
    pub fn new(base: Duration, max_attempts: u32) -> Self {
        Self {
            base,
            max_delay: base * 2u32.pow(max_attempts),
        }
    }
}

impl Default for ExponentialBackoff {
    /// max with 16 secs in between retries
    fn default() -> Self {
        Self::new(Duration::from_secs(1), 4)
    }
}

impl DelayStrategy for ExponentialBackoff {
    // this may add randomness
    fn delay(&self, attempt: u32) -> Duration {
        let delay = self.base * 2u32.pow(attempt);
        if delay < self.max_delay {
            delay
        } else {
            self.max_delay
        }
    }
}

#[derive(Debug, Default, Clone, Copy)]
pub struct Retry<S>
where
    S: DelayStrategy,
{
    strategy: S,
}

impl<S> Retry<S>
where
    S: DelayStrategy,
{
    pub fn new(strategy: S) -> Self {
        Self { strategy }
    }
    pub async fn forever<T, E: std::fmt::Debug, Op, OpFac>(&self, future_factory: OpFac) -> T
    where
        Op: Future<Output = Result<T, E>>,
        OpFac: Fn() -> Op,
    {
        let mut attempt = 0;
        loop {
            match future_factory().await {
                Ok(x) => return x,
                Err(e) => {
                    let delay = self.strategy.delay(attempt);
                    warn!(
                        "Retrying in {}s (attempts {}) due to error: {:?}",
                        delay.as_secs(),
                        attempt + 1,
                        e
                    );
                    tokio::time::sleep(delay).await;
                    attempt += 1;
                }
            }
        }
    }
    pub async fn with_sleep_interrupt<T, E: std::fmt::Debug, Op, Int, OpFac, IntFac>(
        &self,
        future_factory: OpFac,
        sleep_interrupt_factory: IntFac,
    ) -> Option<T>
    where
        Op: Future<Output = Result<T, E>>,
        Int: Future<Output = ()>,
        OpFac: Fn() -> Op,
        IntFac: Fn() -> Int,
    {
        let mut attempt = 0;
        loop {
            match future_factory().await {
                Ok(x) => return Some(x),
                Err(e) => {
                    let delay = self.strategy.delay(attempt);
                    warn!(
                        "Retrying in {}s (attempts {}) due to error: {:?}",
                        delay.as_secs(),
                        attempt + 1,
                        e
                    );
                    tokio::select! {
                        _ = tokio::time::sleep(delay) => {},
                        _ = sleep_interrupt_factory() => {
                            return None;
                        }
                    }
                    attempt += 1;
                }
            }
        }
    }
    pub async fn with_max_attempts<T, E: std::fmt::Debug, Op, OpFac>(
        &self,
        future_factory: OpFac,
        max_attempts: u32,
    ) -> Result<T, E>
    where
        Op: Future<Output = Result<T, E>>,
        OpFac: Fn() -> Op,
    {
        let mut attempt = 0;
        loop {
            match future_factory().await {
                Ok(x) => return Ok(x),
                Err(e) => {
                    if attempt >= max_attempts {
                        return Err(e);
                    }
                    let delay = self.strategy.delay(attempt);
                    warn!(
                        "Retrying in {}s (attempts {}) due to error: {:?}",
                        delay.as_secs(),
                        attempt + 1,
                        e
                    );
                    tokio::time::sleep(delay).await;
                    attempt += 1;
                }
            }
        }
    }
}

use tokio::sync::{MutexGuard, Notify};

#[derive(Default)]
pub struct Condvar {
    inner: Notify,
}

impl Condvar {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn notify_all(&self) {
        self.inner.notify_waiters();
    }

    pub fn notify_one(&self) {
        // Not supported yet
        self.inner.notify_waiters();
    }

    pub async fn wait<'a, T>(&self, lock: MutexGuard<'a, T>) -> MutexGuard<'a, T> {
        let fut = self.inner.notified();
        tokio::pin!(fut);
        fut.as_mut().enable();

        let mutex = MutexGuard::mutex(&lock);
        drop(lock);

        fut.await;
        mutex.lock().await
    }
}

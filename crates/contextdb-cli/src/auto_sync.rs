use std::future::Future;
use std::time::Duration;
use tokio::sync::mpsc;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AutoSyncConfig {
    pub debounce: Duration,
    pub retry_backoff: Duration,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PushOutcome {
    pub conflicts: Vec<String>,
    pub caught_up: bool,
}

impl Default for AutoSyncConfig {
    fn default() -> Self {
        Self {
            debounce: Duration::from_millis(500),
            retry_backoff: Duration::from_millis(500),
        }
    }
}

pub async fn run_loop<P, Fut, R>(
    mut rx: mpsc::UnboundedReceiver<()>,
    config: AutoSyncConfig,
    mut push: P,
    mut report: R,
) where
    P: FnMut() -> Fut,
    Fut: Future<Output = Result<PushOutcome, String>>,
    R: FnMut(String),
{
    let mut next_delay = None;

    loop {
        let delay = match next_delay.take() {
            Some(delay) => delay,
            None => match rx.recv().await {
                Some(()) => config.debounce,
                None => break,
            },
        };

        let sleep = tokio::time::sleep(delay);
        tokio::pin!(sleep);

        loop {
            tokio::select! {
                _ = &mut sleep => break,
                maybe = rx.recv() => {
                    match maybe {
                        Some(()) => {
                            sleep.as_mut().reset(tokio::time::Instant::now() + config.debounce);
                        }
                        None => return,
                    }
                }
            }
        }

        match push().await {
            Ok(outcome) => {
                for reason in outcome.conflicts {
                    report(format!("sync conflict: {reason}"));
                }
                if !outcome.caught_up && !rx.is_closed() {
                    next_delay = Some(Duration::ZERO);
                }
            }
            Err(err) => {
                report(format!("auto-sync push failed: {err}"));
                if rx.is_closed() {
                    break;
                }
                next_delay = Some(config.retry_backoff);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};

    #[tokio::test]
    async fn auto_sync_retries_until_success_after_error() {
        let (tx, rx) = mpsc::unbounded_channel();
        let attempts = Arc::new(AtomicUsize::new(0));
        let reports = Arc::new(Mutex::new(Vec::<String>::new()));
        let attempts_for_task = attempts.clone();
        let reports_for_task = reports.clone();

        let handle = tokio::spawn(run_loop(
            rx,
            AutoSyncConfig {
                debounce: Duration::from_millis(1),
                retry_backoff: Duration::from_millis(1),
            },
            move || {
                let attempts = attempts_for_task.clone();
                async move {
                    let attempt = attempts.fetch_add(1, Ordering::SeqCst);
                    if attempt == 0 {
                        Err("transient".to_string())
                    } else {
                        Ok(PushOutcome {
                            conflicts: Vec::new(),
                            caught_up: true,
                        })
                    }
                }
            },
            move |msg| {
                reports_for_task.lock().unwrap().push(msg);
            },
        ));

        tx.send(()).unwrap();
        tokio::time::sleep(Duration::from_millis(25)).await;
        drop(tx);
        handle.await.unwrap();

        assert!(
            attempts.load(Ordering::SeqCst) >= 2,
            "auto-sync should retry after a failed push"
        );
        let reports = reports.lock().unwrap();
        assert!(
            reports
                .iter()
                .any(|msg| msg.contains("auto-sync push failed")),
            "auto-sync should surface push failures"
        );
    }

    #[tokio::test]
    async fn auto_sync_coalesces_burst_notifications() {
        let (tx, rx) = mpsc::unbounded_channel();
        let attempts = Arc::new(AtomicUsize::new(0));
        let attempts_for_task = attempts.clone();

        let handle = tokio::spawn(run_loop(
            rx,
            AutoSyncConfig {
                debounce: Duration::from_millis(10),
                retry_backoff: Duration::from_millis(1),
            },
            move || {
                let attempts = attempts_for_task.clone();
                async move {
                    attempts.fetch_add(1, Ordering::SeqCst);
                    Ok(PushOutcome {
                        conflicts: Vec::new(),
                        caught_up: true,
                    })
                }
            },
            |_msg| {},
        ));

        tx.send(()).unwrap();
        tx.send(()).unwrap();
        tx.send(()).unwrap();
        tokio::time::sleep(Duration::from_millis(30)).await;
        drop(tx);
        handle.await.unwrap();

        assert_eq!(
            attempts.load(Ordering::SeqCst),
            1,
            "burst notifications should collapse into a single background push"
        );
    }

    #[tokio::test]
    async fn auto_sync_reports_conflicts() {
        let (tx, rx) = mpsc::unbounded_channel();
        let reports = Arc::new(Mutex::new(Vec::<String>::new()));
        let reports_for_task = reports.clone();

        let handle = tokio::spawn(run_loop(
            rx,
            AutoSyncConfig {
                debounce: Duration::from_millis(1),
                retry_backoff: Duration::from_millis(1),
            },
            || async {
                Ok(PushOutcome {
                    conflicts: vec!["edge_wins".to_string()],
                    caught_up: true,
                })
            },
            move |msg| {
                reports_for_task.lock().unwrap().push(msg);
            },
        ));

        tx.send(()).unwrap();
        tokio::time::sleep(Duration::from_millis(10)).await;
        drop(tx);
        handle.await.unwrap();

        let reports = reports.lock().unwrap();
        assert!(
            reports
                .iter()
                .any(|msg| msg.contains("sync conflict: edge_wins")),
            "auto-sync should surface conflict reasons"
        );
    }

    #[tokio::test]
    async fn auto_sync_retries_until_caught_up() {
        let (tx, rx) = mpsc::unbounded_channel();
        let attempts = Arc::new(AtomicUsize::new(0));
        let attempts_for_task = attempts.clone();

        let handle = tokio::spawn(run_loop(
            rx,
            AutoSyncConfig {
                debounce: Duration::from_millis(1),
                retry_backoff: Duration::from_millis(1),
            },
            move || {
                let attempts = attempts_for_task.clone();
                async move {
                    let attempt = attempts.fetch_add(1, Ordering::SeqCst);
                    Ok(PushOutcome {
                        conflicts: Vec::new(),
                        caught_up: attempt > 0,
                    })
                }
            },
            |_msg| {},
        ));

        tx.send(()).unwrap();
        tokio::time::sleep(Duration::from_millis(25)).await;
        drop(tx);
        handle.await.unwrap();

        assert!(
            attempts.load(Ordering::SeqCst) >= 2,
            "auto-sync must keep pushing until the latest local LSN is covered"
        );
    }
}

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
    pub conflicts: Vec<serde_json::Value>,
    pub caught_up: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AutoSyncReport {
    Conflict(serde_json::Value),
    Message(String),
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
    R: FnMut(AutoSyncReport),
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
                for conflict in outcome.conflicts {
                    report(AutoSyncReport::Conflict(conflict));
                }
                if !outcome.caught_up && !rx.is_closed() {
                    next_delay = Some(config.retry_backoff);
                }
            }
            Err(err) => {
                report(AutoSyncReport::Message(format!(
                    "Background auto-sync could not push pending local changes: {err}. It will retry automatically; run `.sync status` to check connectivity."
                )));
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
        let reports = Arc::new(Mutex::new(Vec::<AutoSyncReport>::new()));
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
                .any(|report| matches!(report, AutoSyncReport::Message(message) if message.contains("could not push pending local changes"))),
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

    #[tokio::test(start_paused = true)]
    async fn auto_sync_reports_conflicts() {
        let (tx, rx) = mpsc::unbounded_channel();
        let (report_tx, mut report_rx) = mpsc::unbounded_channel();

        let handle = tokio::spawn(run_loop(
            rx,
            AutoSyncConfig {
                debounce: Duration::from_millis(1),
                retry_backoff: Duration::from_millis(1),
            },
            || async {
                Ok(PushOutcome {
                    conflicts: vec![serde_json::json!({ "reason": "keep_first" })],
                    caught_up: true,
                })
            },
            move |msg| {
                report_tx
                    .send(msg)
                    .expect("test report receiver remains open")
            },
        ));

        tx.send(()).unwrap();
        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_millis(1)).await;
        let report = report_rx
            .recv()
            .await
            .expect("auto-sync reports the conflict after the virtual debounce");
        drop(tx);
        handle.await.unwrap();

        assert_eq!(
            report,
            AutoSyncReport::Conflict(serde_json::json!({ "reason": "keep_first" })),
            "auto-sync should surface the complete conflict document"
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

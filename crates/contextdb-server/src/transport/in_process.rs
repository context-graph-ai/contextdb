use super::{
    ClientTransport, HandlerRegistration, IncomingRequest, Responder, ServerTransport,
    TransportError, TransportFuture,
};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::oneshot;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RecordedExchange {
    pub subject: String,
    pub request_bytes: Vec<u8>,
    pub response_bytes: Vec<u8>,
}

/// Links a client and a server through an in-memory hub so the full
/// push / pull / status / batch-split / apply / conflict path runs with no
/// broker. `.client()` and `.server()` hand out the two ends.
pub struct InProcessBroker {
    inner: Arc<Inner>,
}

struct Inner {
    routes: Mutex<HashMap<String, super::RequestHandler>>,
    exchanges: Mutex<Vec<RecordedExchange>>,
    /// The identity the served side presents, set by
    /// [`InProcessBroker::server_as`]. A client dialing this broker knows it
    /// the same way the real transport knows the endpoint it dialed by key.
    server_node_id: Mutex<Option<String>>,
}

struct InProcessClient {
    inner: Arc<Inner>,
    /// The authenticated node identity this client presents to the server,
    /// modelling the real transport's per-connection authenticated peer id.
    /// `None` for callers that do not exercise per-node hub behavior.
    node_id: Option<String>,
}

struct InProcessServer {
    inner: Arc<Inner>,
}

impl ClientTransport for InProcessClient {
    fn peer_node_id(&self) -> Option<String> {
        self.inner
            .server_node_id
            .lock()
            .unwrap_or_else(|err| err.into_inner())
            .clone()
    }

    fn request<'a>(
        &'a self,
        subject: &'a str,
        request_bytes: Vec<u8>,
        timeout: Duration,
    ) -> TransportFuture<'a, Vec<u8>> {
        Box::pin(async move {
            let subject = subject.to_string();
            let request_for_log = request_bytes.clone();
            let inner = self.inner.clone();
            tokio::time::timeout(timeout, async move {
                tokio::task::yield_now().await;
                let handler = {
                    let routes = inner.routes.lock().unwrap_or_else(|err| err.into_inner());
                    routes.get(&subject).cloned()
                };
                let Some(handler) = handler else {
                    return Err(TransportError::NoResponder);
                };

                let (reply_tx, reply_rx) = oneshot::channel::<Vec<u8>>();
                let responder: Responder = Box::new(move |response_bytes| {
                    Box::pin(async move {
                        let _ = reply_tx.send(response_bytes);
                        Ok(())
                    })
                });

                handler(IncomingRequest {
                    bytes: request_bytes,
                    responder,
                    node_id: self.node_id.clone(),
                })
                .await?;

                let response_bytes = reply_rx.await.map_err(|_| {
                    TransportError::Other("responder dropped without replying".to_string())
                })?;

                inner
                    .exchanges
                    .lock()
                    .unwrap_or_else(|err| err.into_inner())
                    .push(RecordedExchange {
                        subject,
                        request_bytes: request_for_log,
                        response_bytes: response_bytes.clone(),
                    });
                Ok(response_bytes)
            })
            .await
            .map_err(|_| TransportError::Timeout)?
        })
    }
}

impl ServerTransport for InProcessServer {
    fn serve<'a>(
        &'a self,
        handlers: Vec<HandlerRegistration>,
        shutdown: Arc<AtomicBool>,
    ) -> TransportFuture<'a, ()> {
        Box::pin(async move {
            let subjects = handlers
                .iter()
                .map(|handler| handler.subject.clone())
                .collect::<Vec<_>>();
            {
                let mut routes = self
                    .inner
                    .routes
                    .lock()
                    .unwrap_or_else(|err| err.into_inner());
                for handler in handlers {
                    routes.insert(handler.subject, handler.handler);
                }
            }

            while !shutdown.load(Ordering::SeqCst) {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }

            let mut routes = self
                .inner
                .routes
                .lock()
                .unwrap_or_else(|err| err.into_inner());
            for subject in subjects {
                routes.remove(&subject);
            }
            Ok(())
        })
    }
}

impl InProcessBroker {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Inner {
                routes: Mutex::new(HashMap::new()),
                exchanges: Mutex::new(Vec::new()),
                server_node_id: Mutex::new(None),
            }),
        }
    }

    pub fn client(&self) -> Arc<dyn ClientTransport> {
        Arc::new(InProcessClient {
            inner: self.inner.clone(),
            node_id: None,
        })
    }

    /// A client that presents `node_id` as its authenticated identity, so the
    /// served requests carry it exactly as the real sync path carries the
    /// dialing endpoint's authenticated peer id. Used to exercise the hub's
    /// per-node last-contact recording without a real transport.
    pub fn client_as(&self, node_id: &str) -> Arc<dyn ClientTransport> {
        Arc::new(InProcessClient {
            inner: self.inner.clone(),
            node_id: Some(node_id.to_string()),
        })
    }

    pub fn server(&self) -> Arc<dyn ServerTransport> {
        Arc::new(InProcessServer {
            inner: self.inner.clone(),
        })
    }

    /// A server that presents `node_id` as its own authenticated identity, the
    /// mirror of [`Self::client_as`]: every client of this broker then knows
    /// the hub it is talking to, exactly as a client that dialed a real
    /// endpoint by key does.
    pub fn server_as(&self, node_id: &str) -> Arc<dyn ServerTransport> {
        *self
            .inner
            .server_node_id
            .lock()
            .unwrap_or_else(|err| err.into_inner()) = Some(node_id.to_string());
        Arc::new(InProcessServer {
            inner: self.inner.clone(),
        })
    }

    pub fn recorded_exchanges(&self) -> Vec<RecordedExchange> {
        self.inner
            .exchanges
            .lock()
            .unwrap_or_else(|err| err.into_inner())
            .clone()
    }
}

impl Default for InProcessBroker {
    fn default() -> Self {
        Self::new()
    }
}

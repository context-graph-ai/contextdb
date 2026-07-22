use super::{
    ClientTransport, HandlerRegistration, IncomingRequest, Responder, ServerTransport,
    TransportError, TransportFuture, TransportResult, TransportStatusFuture,
};
use crate::protocol::{ChunkAck, MessageType, PROTOCOL_VERSION, decode, encode};
use futures_util::StreamExt;
use futures_util::stream::SelectAll;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

const CONNECT_ATTEMPTS: u32 = 10;
const MAX_CHUNK_SESSIONS: usize = 64;
const CHUNK_BUFFER_TTL: Duration = Duration::from_secs(30);
const CHUNK_BUFFER_CLEANUP_INTERVAL: Duration = Duration::from_secs(10);
const CHUNK_COLLECT_TIMEOUT: Duration = Duration::from_secs(60);
const SHUTDOWN_POLL_INTERVAL: Duration = Duration::from_millis(50);
const RECONNECT_BACKOFF: Duration = Duration::from_millis(200);

type RequestHandlers = HashMap<String, super::RequestHandler>;
type ChunkBuffer = HashMap<uuid::Uuid, (Instant, Vec<crate::protocol::ChunkMessage>)>;

struct RegisteredSubscription {
    subject: String,
    handler: super::RequestHandler,
    response_policy: ResponsePolicy,
    stream: async_nats::Subscriber,
}

#[derive(Clone, Copy)]
struct ResponsePolicy {
    allow_chunked: bool,
    flush_plain: bool,
}

fn response_policy_for_subject(subject: &str) -> ResponsePolicy {
    let is_pull = subject.ends_with(".pull");
    ResponsePolicy {
        allow_chunked: is_pull,
        flush_plain: !is_pull,
    }
}

fn should_chunk_response(response_policy: ResponsePolicy, response: &[u8]) -> bool {
    response_policy.allow_chunked && crate::chunking::needs_chunking(response)
}

pub(super) fn client(endpoint: &str) -> std::sync::Arc<dyn ClientTransport> {
    std::sync::Arc::new(Client {
        endpoint: endpoint.to_string(),
        connection: std::sync::Mutex::new(None),
    })
}

pub(super) fn server(endpoint: &str) -> std::sync::Arc<dyn ServerTransport> {
    std::sync::Arc::new(Server {
        endpoint: endpoint.to_string(),
    })
}

struct Client {
    endpoint: String,
    connection: std::sync::Mutex<Option<async_nats::Client>>,
}

struct Server {
    endpoint: String,
}

impl Client {
    async fn get_connection(&self) -> TransportResult<async_nats::Client> {
        if let Some(client) = self
            .connection
            .lock()
            .unwrap_or_else(|err| err.into_inner())
            .clone()
        {
            return Ok(client);
        }

        let mut last_err = None;
        for attempt in 0..CONNECT_ATTEMPTS {
            if attempt > 0 {
                tokio::time::sleep(Duration::from_millis(200 * u64::from(attempt))).await;
            }
            match async_nats::connect(&self.endpoint).await {
                Ok(client) => {
                    *self
                        .connection
                        .lock()
                        .unwrap_or_else(|err| err.into_inner()) = Some(client.clone());
                    return Ok(client);
                }
                Err(err) => last_err = Some(err.to_string()),
            }
        }

        let err = last_err.unwrap_or_else(|| "unknown error".to_string());
        Err(TransportError::Unreachable(format!(
            "cannot connect to {}: {err}",
            self.endpoint
        )))
    }
}

impl ClientTransport for Client {
    fn ensure_connected<'a>(&'a self) -> TransportFuture<'a, ()> {
        Box::pin(async move {
            self.get_connection().await?;
            Ok(())
        })
    }

    fn reconnect<'a>(&'a self) -> TransportFuture<'a, ()> {
        Box::pin(async move {
            *self
                .connection
                .lock()
                .unwrap_or_else(|err| err.into_inner()) = None;
            let client = async_nats::connect(&self.endpoint).await.ok();
            let connected = client.is_some();
            *self
                .connection
                .lock()
                .unwrap_or_else(|err| err.into_inner()) = client;
            if connected {
                Ok(())
            } else {
                Err(TransportError::Unreachable(format!(
                    "cannot connect to {}",
                    self.endpoint
                )))
            }
        })
    }

    fn is_connected<'a>(&'a self) -> TransportStatusFuture<'a> {
        Box::pin(async move {
            self.connection
                .lock()
                .unwrap_or_else(|err| err.into_inner())
                .is_some()
        })
    }

    fn request<'a>(
        &'a self,
        subject: &'a str,
        request_bytes: Vec<u8>,
        timeout: Duration,
    ) -> TransportFuture<'a, Vec<u8>> {
        Box::pin(async move {
            let connection = self.get_connection().await?;
            if crate::chunking::needs_chunking(&request_bytes) {
                request_chunked(&connection, subject, request_bytes, timeout).await
            } else {
                request_plain(&connection, subject, request_bytes, timeout).await
            }
        })
    }

    fn request_single_reply<'a>(
        &'a self,
        subject: &'a str,
        request_bytes: Vec<u8>,
        timeout: Duration,
    ) -> TransportFuture<'a, Vec<u8>> {
        Box::pin(async move {
            let connection = self.get_connection().await?;
            request_single_reply(&connection, subject, request_bytes, timeout).await
        })
    }

    fn ensure_single_reply_retry_safe(&self, request_bytes: &[u8]) -> TransportResult<()> {
        if crate::chunking::needs_chunking(request_bytes) {
            return Err(TransportError::RetryUnsafe(
                "request requires transport fragmentation".to_string(),
            ));
        }
        Ok(())
    }
}

async fn request_plain(
    connection: &async_nats::Client,
    subject: &str,
    request_bytes: Vec<u8>,
    timeout: Duration,
) -> TransportResult<Vec<u8>> {
    let inbox = connection.new_inbox();
    let mut inbox_stream = connection
        .subscribe(inbox.clone())
        .await
        .map_err(|err| TransportError::Other(err.to_string()))?;
    connection
        .publish_with_reply(subject.to_string(), inbox, request_bytes.into())
        .await
        .map_err(|err| TransportError::Other(err.to_string()))?;
    let msg = tokio::time::timeout(timeout, inbox_stream.next())
        .await
        .map_err(|_| TransportError::Timeout)?
        .ok_or_else(|| TransportError::Other("reply stream closed".to_string()))?;
    read_response(msg, &mut inbox_stream).await
}

async fn request_chunked(
    connection: &async_nats::Client,
    subject: &str,
    request_bytes: Vec<u8>,
    timeout: Duration,
) -> TransportResult<Vec<u8>> {
    let inbox = connection.new_inbox();
    let mut inbox_stream = connection
        .subscribe(inbox.clone())
        .await
        .map_err(|err| TransportError::Other(err.to_string()))?;

    let chunks = crate::chunking::chunk(&request_bytes);
    let first = chunks
        .first()
        .ok_or_else(|| TransportError::Other("empty fragmented request".to_string()))?;
    for chunk_msg in &chunks {
        let chunk_encoded = encode(MessageType::Chunk, chunk_msg)
            .map_err(|err| TransportError::Other(err.to_string()))?;
        connection
            .publish(subject.to_string(), chunk_encoded.into())
            .await
            .map_err(|err| TransportError::Other(err.to_string()))?;
    }
    let ack = ChunkAck {
        chunk_id: first.chunk_id,
        total_chunks: first.total_chunks,
        reply_inbox: inbox.clone(),
    };
    let ack_encoded = encode(MessageType::ChunkAck, &ack)
        .map_err(|err| TransportError::Other(err.to_string()))?;
    connection
        .publish(subject.to_string(), ack_encoded.into())
        .await
        .map_err(|err| TransportError::Other(err.to_string()))?;
    connection
        .flush()
        .await
        .map_err(|err| TransportError::Other(err.to_string()))?;

    let msg = tokio::time::timeout(timeout, inbox_stream.next())
        .await
        .map_err(|_| TransportError::Timeout)?
        .ok_or_else(|| TransportError::Other("reply stream closed".to_string()))?;
    read_response(msg, &mut inbox_stream).await
}

async fn request_single_reply(
    connection: &async_nats::Client,
    subject: &str,
    request_bytes: Vec<u8>,
    timeout: Duration,
) -> TransportResult<Vec<u8>> {
    let msg = tokio::time::timeout(
        timeout,
        connection.request(subject.to_string(), request_bytes.into()),
    )
    .await
    .map_err(|_| TransportError::Timeout)?
    .map_err(map_request_error)?;
    if let Some(status) = msg.status {
        if status == async_nats::StatusCode::NO_RESPONDERS {
            return Err(TransportError::NoResponder);
        }
        return Err(TransportError::Status(format!("{status:?}")));
    }
    Ok(msg.payload.to_vec())
}

fn map_request_error(err: async_nats::RequestError) -> TransportError {
    match err.kind() {
        async_nats::RequestErrorKind::TimedOut => TransportError::Timeout,
        async_nats::RequestErrorKind::NoResponders => TransportError::NoResponder,
        _ => TransportError::Other(err.to_string()),
    }
}

async fn read_response(
    msg: async_nats::Message,
    inbox_stream: &mut async_nats::Subscriber,
) -> TransportResult<Vec<u8>> {
    if let Some(status) = msg.status {
        if status == async_nats::StatusCode::NO_RESPONDERS {
            return Err(TransportError::NoResponder);
        }
        return Err(TransportError::Status(format!("{status:?}")));
    }

    if !matches!(message_type_hint(&msg.payload), Some(MessageType::Chunk)) {
        return Ok(msg.payload.to_vec());
    }

    let envelope =
        decode(&msg.payload).map_err(|err| TransportError::IncompleteReply(err.to_string()))?;
    let first_chunk: crate::protocol::ChunkMessage = rmp_serde::from_slice(&envelope.payload)
        .map_err(|err| TransportError::IncompleteReply(err.to_string()))?;
    let total = first_chunk.total_chunks;
    let mut collected = vec![first_chunk];
    let deadline = tokio::time::Instant::now() + CHUNK_COLLECT_TIMEOUT;
    while collected.len() < total as usize {
        let next_msg = tokio::time::timeout_at(deadline, inbox_stream.next())
            .await
            .map_err(|_| {
                TransportError::IncompleteReply(format!(
                    "timeout collecting fragments ({}/{total})",
                    collected.len()
                ))
            })?
            .ok_or_else(|| {
                TransportError::IncompleteReply("reply stream closed mid-fragment".to_string())
            })?;
        if let Some(status) = next_msg.status {
            return Err(TransportError::IncompleteReply(format!(
                "status reply while collecting fragments: {status:?}"
            )));
        }
        let envelope = decode(&next_msg.payload)
            .map_err(|err| TransportError::IncompleteReply(err.to_string()))?;
        if !matches!(envelope.message_type, MessageType::Chunk) {
            return Err(TransportError::IncompleteReply(
                "unexpected response while collecting fragments".to_string(),
            ));
        }
        let chunk_msg: crate::protocol::ChunkMessage = rmp_serde::from_slice(&envelope.payload)
            .map_err(|err| TransportError::IncompleteReply(err.to_string()))?;
        collected.push(chunk_msg);
    }
    Ok(crate::chunking::reassemble(&mut collected))
}

impl ServerTransport for Server {
    fn serve<'a>(
        &'a self,
        handlers: Vec<HandlerRegistration>,
        shutdown: std::sync::Arc<AtomicBool>,
    ) -> TransportFuture<'a, ()> {
        Box::pin(async move {
            while !shutdown.load(Ordering::SeqCst) {
                match serve_once(&self.endpoint, &handlers, shutdown.clone()).await {
                    Ok(()) => return Ok(()),
                    Err(err) => {
                        tracing::error!(error = %err, "sync transport bootstrap failed; retrying");
                        tokio::time::sleep(RECONNECT_BACKOFF).await;
                    }
                }
            }
            Ok(())
        })
    }
}

async fn serve_once(
    endpoint: &str,
    handlers: &[HandlerRegistration],
    shutdown: std::sync::Arc<AtomicBool>,
) -> TransportResult<()> {
    let connection = connect_until_shutdown(endpoint, shutdown.clone()).await?;
    let mut subscriptions = Vec::with_capacity(handlers.len());
    for registration in handlers {
        let stream = connection
            .subscribe(registration.subject.clone())
            .await
            .map_err(|err| TransportError::Other(err.to_string()))?;
        subscriptions.push(RegisteredSubscription {
            subject: registration.subject.clone(),
            handler: registration.handler.clone(),
            response_policy: response_policy_for_subject(&registration.subject),
            stream,
        });
    }
    connection
        .flush()
        .await
        .map_err(|err| TransportError::Other(err.to_string()))?;

    if subscriptions.len() == 3 {
        let first = subscriptions.remove(0);
        let second = subscriptions.remove(0);
        let third = subscriptions.remove(0);
        serve_three_subscriptions(&connection, shutdown, first, second, third).await
    } else {
        serve_merged_subscriptions(&connection, shutdown, subscriptions).await
    }
}

async fn serve_three_subscriptions(
    connection: &async_nats::Client,
    shutdown: std::sync::Arc<AtomicBool>,
    first: RegisteredSubscription,
    second: RegisteredSubscription,
    third: RegisteredSubscription,
) -> TransportResult<()> {
    let RegisteredSubscription {
        subject: _,
        handler: first_handler,
        response_policy: first_response_policy,
        stream: mut first_stream,
    } = first;
    let RegisteredSubscription {
        subject: _,
        handler: second_handler,
        response_policy: second_response_policy,
        stream: mut second_stream,
    } = second;
    let RegisteredSubscription {
        subject: _,
        handler: third_handler,
        response_policy: third_response_policy,
        stream: mut third_stream,
    } = third;
    let mut buffer = ChunkBuffer::new();
    let mut cleanup = tokio::time::interval(CHUNK_BUFFER_CLEANUP_INTERVAL);
    let mut shutdown_poll = tokio::time::interval(SHUTDOWN_POLL_INTERVAL);

    while !shutdown.load(Ordering::SeqCst) {
        tokio::select! {
            maybe_msg = first_stream.next() => {
                handle_subscription_message(
                    connection,
                    maybe_msg,
                    &first_handler,
                    first_response_policy,
                    &mut buffer,
                    &shutdown,
                ).await?;
            }
            maybe_msg = second_stream.next() => {
                handle_subscription_message(
                    connection,
                    maybe_msg,
                    &second_handler,
                    second_response_policy,
                    &mut buffer,
                    &shutdown,
                ).await?;
            }
            maybe_msg = third_stream.next() => {
                handle_subscription_message(
                    connection,
                    maybe_msg,
                    &third_handler,
                    third_response_policy,
                    &mut buffer,
                    &shutdown,
                ).await?;
            }
            _ = cleanup.tick() => cleanup_buffer(&mut buffer),
            _ = shutdown_poll.tick() => {}
        }
    }

    Ok(())
}

async fn serve_merged_subscriptions(
    connection: &async_nats::Client,
    shutdown: std::sync::Arc<AtomicBool>,
    subscriptions: Vec<RegisteredSubscription>,
) -> TransportResult<()> {
    let mut streams = SelectAll::new();
    let mut handler_map = RequestHandlers::new();
    for subscription in subscriptions {
        handler_map.insert(subscription.subject, subscription.handler);
        streams.push(subscription.stream);
    }

    let mut buffer = ChunkBuffer::new();
    let mut cleanup = tokio::time::interval(CHUNK_BUFFER_CLEANUP_INTERVAL);
    let mut shutdown_poll = tokio::time::interval(SHUTDOWN_POLL_INTERVAL);

    while !shutdown.load(Ordering::SeqCst) {
        tokio::select! {
            maybe_msg = streams.next() => {
                let Some(msg) = maybe_msg else {
                    if shutdown.load(Ordering::SeqCst) {
                        return Ok(());
                    }
                    return Err(TransportError::Other(
                        "all subscription streams closed".to_string(),
                    ));
                };
                handle_incoming_logged(connection, msg, &handler_map, &mut buffer).await;
            }
            _ = cleanup.tick() => cleanup_buffer(&mut buffer),
            _ = shutdown_poll.tick() => {}
        }
    }

    Ok(())
}

async fn handle_subscription_message(
    connection: &async_nats::Client,
    maybe_msg: Option<async_nats::Message>,
    handler: &super::RequestHandler,
    response_policy: ResponsePolicy,
    buffer: &mut ChunkBuffer,
    shutdown: &AtomicBool,
) -> TransportResult<()> {
    let Some(msg) = maybe_msg else {
        if shutdown.load(Ordering::SeqCst) {
            return Ok(());
        }
        return Err(TransportError::Other(
            "subscription stream closed".to_string(),
        ));
    };
    handle_incoming_logged_direct(connection, msg, handler, response_policy, buffer).await;
    Ok(())
}

async fn handle_incoming_logged_direct(
    connection: &async_nats::Client,
    msg: async_nats::Message,
    handler: &super::RequestHandler,
    response_policy: ResponsePolicy,
    buffer: &mut ChunkBuffer,
) {
    if let Err(err) =
        handle_incoming_direct(connection, msg, handler, response_policy, buffer).await
    {
        tracing::error!(error = %err, "sync transport request failed");
    }
}

async fn handle_incoming_logged(
    connection: &async_nats::Client,
    msg: async_nats::Message,
    handlers: &RequestHandlers,
    buffer: &mut ChunkBuffer,
) {
    if let Err(err) = handle_incoming(connection, msg, handlers, buffer).await {
        tracing::error!(error = %err, "sync transport request failed");
    }
}

async fn connect_until_shutdown(
    endpoint: &str,
    shutdown: std::sync::Arc<AtomicBool>,
) -> TransportResult<async_nats::Client> {
    while !shutdown.load(Ordering::SeqCst) {
        match async_nats::connect(endpoint).await {
            Ok(client) => return Ok(client),
            Err(_) => tokio::time::sleep(RECONNECT_BACKOFF).await,
        }
    }
    Err(TransportError::Unreachable(format!(
        "shutdown before connecting to {endpoint}"
    )))
}

async fn handle_incoming(
    connection: &async_nats::Client,
    msg: async_nats::Message,
    handlers: &RequestHandlers,
    buffer: &mut ChunkBuffer,
) -> TransportResult<()> {
    let subject = msg.subject.to_string();
    let handler = handlers
        .get(&subject)
        .ok_or_else(|| TransportError::Other(format!("no handler for {subject}")))?;
    let response_policy = response_policy_for_subject(&subject);

    handle_incoming_direct(connection, msg, handler, response_policy, buffer).await
}

async fn handle_incoming_direct(
    connection: &async_nats::Client,
    msg: async_nats::Message,
    handler: &super::RequestHandler,
    response_policy: ResponsePolicy,
    buffer: &mut ChunkBuffer,
) -> TransportResult<()> {
    match message_type_hint(&msg.payload) {
        Some(MessageType::Chunk) => {
            let envelope =
                decode(&msg.payload).map_err(|err| TransportError::Other(err.to_string()))?;
            let chunk_msg: crate::protocol::ChunkMessage = rmp_serde::from_slice(&envelope.payload)
                .map_err(|err| TransportError::Other(err.to_string()))?;
            if !buffer.contains_key(&chunk_msg.chunk_id) && buffer.len() >= MAX_CHUNK_SESSIONS {
                tracing::warn!(
                    chunk_id = %chunk_msg.chunk_id,
                    active_sessions = buffer.len(),
                    "fragment buffer full, rejecting new session"
                );
                return Err(TransportError::Other("fragment buffer full".to_string()));
            }
            buffer
                .entry(chunk_msg.chunk_id)
                .or_insert_with(|| (Instant::now(), Vec::new()))
                .1
                .push(chunk_msg);
            Ok(())
        }
        Some(MessageType::ChunkAck) => {
            let envelope =
                decode(&msg.payload).map_err(|err| TransportError::Other(err.to_string()))?;
            let ack: ChunkAck = rmp_serde::from_slice(&envelope.payload)
                .map_err(|err| TransportError::Other(err.to_string()))?;
            let (_created_at, mut chunks) = buffer.remove(&ack.chunk_id).ok_or_else(|| {
                TransportError::Other(format!("no buffered fragments for {}", ack.chunk_id))
            })?;
            if chunks.len() != ack.total_chunks as usize {
                return Err(TransportError::Other(format!(
                    "expected {} fragments for {}, got {}",
                    ack.total_chunks,
                    ack.chunk_id,
                    chunks.len()
                )));
            }
            let assembled = crate::chunking::reassemble(&mut chunks);
            forward_to_handler(
                connection,
                handler,
                assembled,
                Some(ack.reply_inbox.into()),
                response_policy,
            )
            .await
        }
        _ => {
            forward_to_handler(
                connection,
                handler,
                msg.payload.to_vec(),
                msg.reply,
                response_policy,
            )
            .await
        }
    }
}

fn message_type_hint(payload: &[u8]) -> Option<MessageType> {
    if payload.len() < 3 || payload[0] != 0x93 || payload[1] != PROTOCOL_VERSION {
        return None;
    }
    let (variant_start, variant_len) = match payload[2] {
        fixstr if fixstr & 0xe0 == 0xa0 => (3usize, (fixstr & 0x1f) as usize),
        0xd9 if payload.len() >= 4 => (4usize, payload[3] as usize),
        _ => return None,
    };
    let variant_end = variant_start.checked_add(variant_len)?;
    let variant = payload.get(variant_start..variant_end)?;
    match variant {
        b"Chunk" => Some(MessageType::Chunk),
        b"ChunkAck" => Some(MessageType::ChunkAck),
        _ => None,
    }
}

async fn forward_to_handler(
    connection: &async_nats::Client,
    handler: &super::RequestHandler,
    bytes: Vec<u8>,
    reply: Option<async_nats::Subject>,
    response_policy: ResponsePolicy,
) -> TransportResult<()> {
    let connection = connection.clone();
    let responder: Responder = Box::new(move |response_bytes| {
        Box::pin(async move {
            let Some(reply) = reply else {
                return Ok(());
            };
            publish_response(&connection, reply, response_bytes, response_policy).await
        })
    });
    // The deprecated broker path authenticates no peer identity; the hub
    // simply records no last-contact for exchanges arriving this way.
    handler(IncomingRequest {
        bytes,
        responder,
        node_id: None,
    })
    .await
}

async fn publish_response(
    connection: &async_nats::Client,
    reply: async_nats::Subject,
    response: Vec<u8>,
    response_policy: ResponsePolicy,
) -> TransportResult<()> {
    if should_chunk_response(response_policy, &response) {
        for chunk_msg in crate::chunking::chunk(&response) {
            let chunk_encoded = encode(MessageType::Chunk, &chunk_msg)
                .map_err(|err| TransportError::Other(err.to_string()))?;
            connection
                .publish(reply.clone(), chunk_encoded.into())
                .await
                .map_err(|err| TransportError::Other(err.to_string()))?;
        }
        connection
            .flush()
            .await
            .map_err(|err| TransportError::Other(err.to_string()))?;
    } else {
        connection
            .publish(reply, response.into())
            .await
            .map_err(|err| TransportError::Other(err.to_string()))?;
        if response_policy.flush_plain {
            connection
                .flush()
                .await
                .map_err(|err| TransportError::Other(err.to_string()))?;
        }
    }
    Ok(())
}

fn cleanup_buffer(buffer: &mut ChunkBuffer) {
    buffer.retain(|id, (created_at, _chunks)| {
        let keep = created_at.elapsed() < CHUNK_BUFFER_TTL;
        if !keep {
            tracing::warn!(%id, "evicting stale transport fragment session");
        }
        keep
    });
}

#[cfg(test)]
mod tests {
    use super::{message_type_hint, response_policy_for_subject, should_chunk_response};
    use crate::protocol::{ChunkAck, ChunkMessage, MessageType, encode};

    #[derive(serde::Serialize)]
    struct OpaquePayload {
        value: u8,
    }

    fn message_type_from_wire_name(parts: &[&str]) -> MessageType {
        let wire_name = parts.concat();
        let encoded = rmp_serde::to_vec(&wire_name).unwrap();
        rmp_serde::from_slice(&encoded).unwrap()
    }

    #[test]
    fn message_type_hint_matches_protocol_encoder_for_chunk_control_messages() {
        let chunk = ChunkMessage {
            chunk_id: uuid::Uuid::new_v4(),
            sequence: 0,
            total_chunks: 1,
            payload: vec![1, 2, 3],
        };
        let encoded = encode(MessageType::Chunk, &chunk).unwrap();
        assert_eq!(message_type_hint(&encoded), Some(MessageType::Chunk));

        let ack = ChunkAck {
            chunk_id: chunk.chunk_id,
            total_chunks: 1,
            reply_inbox: "_INBOX.test".to_string(),
        };
        let encoded = encode(MessageType::ChunkAck, &ack).unwrap();
        assert_eq!(message_type_hint(&encoded), Some(MessageType::ChunkAck));
    }

    #[test]
    fn message_type_hint_ignores_regular_sync_messages_and_garbage() {
        let msg_type = message_type_from_wire_name(&["Push", "Request"]);
        let encoded = encode(msg_type, &OpaquePayload { value: 1 }).unwrap();
        assert_eq!(message_type_hint(&encoded), None);

        let msg_type = message_type_from_wire_name(&["Pull", "Response"]);
        let encoded = encode(msg_type, &OpaquePayload { value: 2 }).unwrap();
        assert_eq!(message_type_hint(&encoded), None);

        assert_eq!(message_type_hint(&[]), None);
        assert_eq!(message_type_hint(&[0x93, 0x04]), None);
        assert_eq!(message_type_hint(b"not-msgpack"), None);
    }

    #[test]
    fn response_chunking_is_pull_only() {
        let push = response_policy_for_subject("sync.tenant.push");
        assert!(!push.allow_chunked);
        assert!(push.flush_plain);

        let status = response_policy_for_subject("sync.tenant.status");
        assert!(!status.allow_chunked);
        assert!(status.flush_plain);

        let pull = response_policy_for_subject("sync.tenant.pull");
        assert!(pull.allow_chunked);
        assert!(!pull.flush_plain);

        let oversized_response = vec![0u8; 900 * 1024 + 1];
        assert!(!should_chunk_response(push, &oversized_response));
        assert!(!should_chunk_response(status, &oversized_response));
        assert!(should_chunk_response(pull, &oversized_response));
    }
}

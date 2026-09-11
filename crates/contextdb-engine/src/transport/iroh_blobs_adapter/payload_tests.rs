use super::*;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll, Waker};
use tokio::io::{AsyncWrite, AsyncWriteExt};

enum AfterPrefix {
    Fail,
    Pending,
    Complete,
}

struct PartialWriter {
    remaining: usize,
    after: AfterPrefix,
    written: Vec<u8>,
    resets: Vec<VarInt>,
}

impl AsyncWrite for PartialWriter {
    fn poll_write(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        let n = if self.remaining != 0 {
            self.remaining.min(bytes.len())
        } else {
            match self.after {
                AfterPrefix::Fail => {
                    return Poll::Ready(Err(std::io::ErrorKind::BrokenPipe.into()));
                }
                AfterPrefix::Pending => return Poll::Pending,
                AfterPrefix::Complete => bytes.len(),
            }
        };
        self.remaining = self.remaining.saturating_sub(n);
        self.written.extend_from_slice(&bytes[..n]);
        Poll::Ready(Ok(n))
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

impl SendStream for PartialWriter {
    async fn send_bytes(&mut self, bytes: bytes::Bytes) -> std::io::Result<()> {
        self.write_all(&bytes).await
    }

    async fn send(&mut self, bytes: &[u8]) -> std::io::Result<()> {
        self.write_all(bytes).await
    }

    async fn sync(&mut self) -> std::io::Result<()> {
        self.flush().await
    }

    fn reset(&mut self, code: VarInt) -> std::io::Result<()> {
        self.resets.push(code);
        Ok(())
    }

    async fn stopped(&mut self) -> std::io::Result<Option<VarInt>> {
        Ok(self.resets.last().copied())
    }

    fn id(&self) -> u64 {
        0
    }
}

fn sender(after: AfterPrefix) -> CountingSend<PartialWriter> {
    CountingSend {
        inner: PartialWriter {
            remaining: 3,
            after,
            written: Vec::new(),
            resets: Vec::new(),
        },
        payload_sent: 0,
        reset_after: None,
        emitted: Arc::new(AtomicU64::new(0)),
        tamper: None,
    }
}

#[tokio::test]
async fn partial_payload_is_counted_before_write_failure() {
    for reset_after in [None, Some(5)] {
        let mut send = sender(AfterPrefix::Fail);
        send.reset_after = reset_after;
        let error = send
            .send_bytes(bytes::Bytes::from_static(b"payload"))
            .await
            .unwrap_err();
        assert_eq!(error.kind(), std::io::ErrorKind::BrokenPipe);
        assert_eq!(send.inner.written, b"pay");
        assert_eq!(send.payload_sent, 3);
        assert_eq!(send.emitted.load(Ordering::SeqCst), 3);
        assert!(send.inner.resets.is_empty());
    }
}

#[test]
fn partial_payload_is_counted_before_send_cancellation() {
    for reset_after in [None, Some(5)] {
        let mut send = sender(AfterPrefix::Pending);
        send.reset_after = reset_after;
        let emitted = send.emitted.clone();
        let mut future = Box::pin(send.send_bytes(bytes::Bytes::from_static(b"payload")));
        let mut cx = Context::from_waker(Waker::noop());
        assert!(future.as_mut().poll(&mut cx).is_pending());
        assert_eq!(emitted.load(Ordering::SeqCst), 3);
        drop(future);
        assert_eq!(send.inner.written, b"pay");
        assert_eq!(send.payload_sent, 3);
        assert_eq!(emitted.load(Ordering::SeqCst), 3);
        assert!(send.inner.resets.is_empty());
    }
}

#[tokio::test]
async fn partial_payload_keeps_tamper_drop_and_metadata_accounting() {
    let mut send = sender(AfterPrefix::Complete);
    send.send(&[0; 8]).await.unwrap();
    send.send(&[1; 64]).await.unwrap();
    assert_eq!(send.emitted.load(Ordering::SeqCst), 0);
    assert_eq!(send.payload_sent, 0);
    send.inner.remaining = 3;
    send.tamper = Some(bytes::Bytes::from_static(b"TAMPER!"));
    send.reset_after = Some(5);
    send.send_bytes(bytes::Bytes::from_static(b"payl"))
        .await
        .unwrap();
    assert_eq!(send.payload_sent, 4);
    send.send_bytes(bytes::Bytes::from_static(b"oad"))
        .await
        .unwrap_err();
    assert_eq!(&send.inner.written[72..], b"TAMPE");
    assert_eq!(send.payload_sent, 5);
    assert_eq!(send.emitted.load(Ordering::SeqCst), 5);
    assert_eq!(send.inner.resets, [VarInt::from(RESET_DROPPED)]);

    // Another stream shares the holder counter, not the failed stream's offset
    // or consumed transient fault.
    let mut retry = sender(AfterPrefix::Complete);
    retry.emitted = send.emitted.clone();
    retry
        .send_bytes(bytes::Bytes::from_static(b"payload"))
        .await
        .unwrap();
    assert_eq!(retry.inner.written, b"payload");
    assert_eq!(retry.payload_sent, 7);
    assert_eq!(retry.emitted.load(Ordering::SeqCst), 12);
    assert!(retry.inner.resets.is_empty());
}

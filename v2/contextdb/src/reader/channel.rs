//! One bounded opaque exchange per connection; four listeners belong to the writer.
use super::ReadLimits;
use crate::memory::{Budget, Reservation};
use crate::system::locked;
use crate::system::{self, Clock};
use crate::types::{Error, Result};
use std::fs::{File, Permissions};
use std::io::{Read, Write};
use std::net::Shutdown;
use std::os::unix::fs::{DirBuilderExt, MetadataExt, PermissionsExt};
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};

/// Host code invoked outside database and channel locks.
pub trait OwnerRequestHandler: Send + Sync + 'static {
    /// Returns opaque bytes, or a refusal. Cooperate with cancellation before returning.
    fn handle(
        &self,
        namespace: &str,
        request: &[u8],
        cancel: &OwnerReadCancellation,
    ) -> Result<Vec<u8>>;
}
type Wake = Arc<dyn Fn() + Send + Sync>;
/// Cooperative cancellation of a writer's dispatched requests.
#[derive(Clone)]
pub struct OwnerReadCancellation(Arc<Mutex<(bool, Vec<Wake>, Reservation)>>);
/// What [`OwnerReadCancellation::tell_on_cancel`] returns. Holding it keeps the callback
/// registered and its bytes charged to the channel's budget; dropping it unregisters the
/// callback, which then never runs, and lets the charge go.
pub struct CancellationListener(OwnerReadCancellation, Wake, u64);
impl Drop for CancellationListener {
    fn drop(&mut self) {
        let mut state = locked(&self.0.0);
        state.1.retain(|wake| !Arc::ptr_eq(wake, &self.1));
        state.2.shrink(self.2);
    }
}
impl OwnerReadCancellation {
    /// Whether this writer is closing.
    pub fn is_cancelled(&self) -> bool {
        locked(&self.0).0
    }
    /// Registers a wake charged to the channel's budget, or calls it when closing already began.
    pub fn tell_on_cancel(
        &self,
        wake: impl Fn() + Send + Sync + 'static,
    ) -> Result<CancellationListener> {
        let mut state = locked(&self.0);
        let (closing, wakes, held) = &mut *state;
        let bytes = (2 * size_of::<usize>() + size_of_val(&wake)) as u64;
        held.room(wakes, usize::MAX)?;
        held.grow(bytes)?;
        let wake: Wake = Arc::new(wake);
        let listener = CancellationListener(self.clone(), wake.clone(), bytes);
        if *closing {
            drop(state);
            wake();
        } else {
            wakes.push(wake);
        }
        Ok(listener)
    }
    fn stop(&self) {
        let wakes = {
            let mut state = locked(&self.0);
            state.0 = true;
            std::mem::take(&mut state.1)
        };
        for wake in wakes {
            wake();
        }
    }
}
struct Endpoint {
    address: PathBuf,
    _directory: File,
}
impl Endpoint {
    fn new(directory: &Path, store: &Path) -> std::io::Result<Self> {
        let data = std::fs::metadata(store.join("data"))?;
        let name = format!("{}-{}.sock", data.dev(), data.ino());
        let directory = std::fs::canonicalize(directory)?;
        let file = File::open(&directory)?;
        #[cfg(target_os = "linux")]
        let address = {
            use std::os::fd::AsRawFd;
            PathBuf::from(format!("/proc/self/fd/{}/{name}", file.as_raw_fd()))
        };
        #[cfg(not(target_os = "linux"))]
        let address = {
            use std::hash::{Hash, Hasher};
            let mut bytes = [0; 32];
            for seed in 0..4 {
                let mut hash = std::collections::hash_map::DefaultHasher::new();
                (seed, &directory, &name).hash(&mut hash);
                bytes[seed * 8..seed * 8 + 8].copy_from_slice(&hash.finish().to_be_bytes());
            }
            let alphabet = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
            let mut name = String::new();
            for chunk in bytes.chunks(3) {
                let word = chunk
                    .iter()
                    .fold(0_u32, |word, byte| (word << 8) | u32::from(*byte))
                    << (8 * (3 - chunk.len()));
                for shift in (0..=chunk.len()).map(|i| 18 - i * 6) {
                    name.push(alphabet[((word >> shift) & 63) as usize] as char);
                }
            }
            let address = std::env::temp_dir().join(name);
            if address.as_os_str().len() > 103 {
                return Err(std::io::Error::other("channel address exceeds 103 bytes"));
            }
            address
        };
        Ok(Self {
            address,
            _directory: file,
        })
    }
    fn connect(&self) -> std::io::Result<UnixStream> {
        UnixStream::connect(&self.address)
    }
}
fn io(error: std::io::Error) -> Error {
    Error::StoreIo {
        operation: "channel",
        path: PathBuf::new(),
        reason: crate::types::excerpt(error),
    }
}
fn disconnected(_: std::io::Error) -> Error {
    Error::OwnerDisconnected
}
type Wait<'a> = Option<(SystemTime, &'a dyn Clock)>;
fn charge(budget: &Budget, bytes: usize) -> Result<Reservation> {
    budget.reserve(bytes as u64, "writer request channel")
}
fn wait_io(n: usize, w: Wait<'_>, f: impl FnMut(usize) -> std::io::Result<usize>) -> Result<()> {
    if w.is_some_and(|(deadline, clock)| clock.now() >= deadline) {
        return Err(Error::QueryCanceled {
            statement_timeout: true,
        });
    }
    system::wait_for(n, w, None, f)
        .map_err(disconnected)?
        .map_err(|_| Error::QueryCanceled {
            statement_timeout: true,
        })
}
fn write_frame(stream: &mut UnixStream, bytes: &[u8], wait: Wait<'_>) -> Result<()> {
    let length = u32::try_from(bytes.len()).map_err(|_| Error::OwnerLimitExceeded)?;
    for bytes in [&length.to_be_bytes()[..], bytes] {
        wait_io(bytes.len(), wait, |at| stream.write(&bytes[at..]))?;
    }
    Ok(())
}
fn read_frame(
    stream: &mut UnixStream,
    cap: u64,
    budget: &Budget,
    wait: Wait<'_>,
) -> Result<(Vec<u8>, Reservation)> {
    let mut header = [0; 4];
    wait_io(4, wait, |at| stream.read(&mut header[at..]))?;
    let length = u32::from_be_bytes(header) as usize;
    if length as u64 > cap {
        return Err(Error::OwnerLimitExceeded);
    }
    let held = charge(budget, length)?;
    let mut bytes = vec![0; length];
    wait_io(length, wait, |at| stream.read(&mut bytes[at..]))?;
    Ok((bytes, held))
}
struct Pool {
    cancel: Arc<OwnerReadCancellation>,
    streams: Mutex<[Option<UnixStream>; 4]>,
    handler: Arc<dyn OwnerRequestHandler>,
    namespace: String,
    _namespace: Reservation,
    budget: Budget,
}
/// Owns the endpoint and joins its listeners before releasing writer ownership.
pub(crate) struct Channel {
    listener: UnixListener,
    endpoint: Endpoint,
    location: PathBuf,
    pool: Arc<Pool>,
    threads: Vec<std::thread::JoinHandle<()>>,
}
impl Channel {
    pub(crate) fn start(
        folder: &Path,
        directory: &Path,
        namespace: &str,
        handler: Arc<dyn OwnerRequestHandler>,
        budget: &Budget,
    ) -> Result<Self> {
        if namespace.len() > u16::MAX as usize {
            return Err(Error::OwnerLimitExceeded);
        }
        let mut made = std::fs::DirBuilder::new();
        made.recursive(true)
            .mode(0o700)
            .create(directory)
            .map_err(io)?;
        let endpoint = Endpoint::new(directory, folder).map_err(io)?;
        match std::fs::remove_file(&endpoint.address) {
            Ok(()) => (),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => (),
            Err(error) => return Err(io(error)),
        }
        let namespace_held = charge(budget, namespace.len())?;
        let cancel = (false, Vec::new(), charge(budget, 0)?);
        let listener = UnixListener::bind(&endpoint.address).map_err(io)?;
        std::fs::set_permissions(&endpoint.address, Permissions::from_mode(0o600)).map_err(io)?;
        let mut channel = Self {
            listener,
            endpoint,
            location: folder.join("writer.channel"),
            threads: Vec::new(),
            pool: Arc::new(Pool {
                cancel: Arc::new(OwnerReadCancellation(Arc::new(Mutex::new(cancel)))),
                streams: Mutex::new([None, None, None, None]),
                handler,
                namespace: namespace.into(),
                _namespace: namespace_held,
                budget: budget.clone(),
            }),
        };
        for slot in 0..4 {
            let listener = channel.listener.try_clone().map_err(io)?;
            let pool = channel.pool.clone();
            channel
                .threads
                .push(crate::system::launch(move || serve(listener, pool, slot)).map_err(io)?);
        }
        let _ = std::fs::remove_file(&channel.location);
        std::os::unix::fs::symlink(
            std::fs::canonicalize(directory).map_err(io)?,
            &channel.location,
        )
        .map_err(io)?;
        Ok(channel)
    }
}
impl Drop for Channel {
    fn drop(&mut self) {
        self.pool.cancel.stop();
        // Unblock a read that has not reached the handler. The write half stays open so a
        // request already in a handler, and one cut off while its bytes were still arriving,
        // can still be answered with owner_not_serving instead of a severed connection.
        for stream in locked(&self.pool.streams).iter().flatten() {
            let _ = stream.shutdown(Shutdown::Read);
        }
        for _ in &self.threads {
            let _ = self.endpoint.connect();
        }
        // Wake accepts while the name exists, then close admission before draining.
        let _ = std::fs::remove_file(&self.location);
        let _ = std::fs::remove_file(&self.endpoint.address);
        for thread in self.threads.drain(..) {
            let _ = thread.join();
        }
        if self.listener.set_nonblocking(true).is_ok() {
            while let Ok((mut stream, _)) = self.listener.accept() {
                let _ = stream.write_all(&[1, 0, 0, 0, 0]);
            }
        }
    }
}
fn serve(listener: UnixListener, pool: Arc<Pool>, slot: usize) {
    while let Ok((mut stream, _)) = listener.accept() {
        {
            let mut streams = locked(&pool.streams);
            if pool.cancel.is_cancelled() {
                let _ = stream.write_all(&[1, 0, 0, 0, 0]);
                return;
            }
            let Ok(copy) = stream.try_clone() else {
                continue;
            };
            streams[slot] = Some(copy);
        }
        let sent = (|| {
            let (status, _held, bytes) = match handle(&mut stream, &pool) {
                Ok(bytes) => (0, Some(charge(&pool.budget, bytes.len())?), bytes),
                Err(Error::OwnerNotServing) => (1, None, Vec::new()),
                Err(Error::OwnerLimitExceeded) => (3, None, Vec::new()),
                Err(error @ Error::MemoryBudgetExceeded { .. }) => return Err(error),
                Err(error) => refusal(&error, &pool.budget).map(|(b, h)| (2, Some(h), b))?,
            };
            stream.write_all(&[status]).map_err(disconnected)?;
            write_frame(&mut stream, &bytes, None)
        })();
        if let Err(Error::MemoryBudgetExceeded {
            requested_bytes: a,
            available_bytes: b,
            budget_limit_bytes: c,
            ..
        }) = sent
        {
            let nums = [a, b, c].map(u64::to_be_bytes);
            let _ = (stream.write_all(&[4, 0, 0, 0, 24]))
                .and_then(|()| stream.write_all(nums.as_flattened()));
        }
        locked(&pool.streams)[slot] = None;
    }
}
// Host error formatting is bounded too: truncating after to_string would first allocate
// the host's entire message. The four fixed scratch buffers fit the opening allowance.
fn refusal(error: &Error, budget: &Budget) -> Result<(Vec<u8>, Reservation)> {
    struct Reason([u8; u16::MAX as usize], usize);
    impl std::fmt::Write for Reason {
        fn write_str(&mut self, text: &str) -> std::fmt::Result {
            let mut end = text.len().min(self.0.len() - self.1);
            while !text.is_char_boundary(end) {
                end -= 1;
            }
            self.0[self.1..self.1 + end].copy_from_slice(&text.as_bytes()[..end]);
            self.1 += end;
            Ok(())
        }
    }
    let mut reason = Reason([0; u16::MAX as usize], 0);
    let _ = std::fmt::write(&mut reason, format_args!("{error}"));
    let held = charge(budget, reason.1)?;
    Ok((reason.0[..reason.1].to_vec(), held))
}
fn read_while_open(
    stream: &mut UnixStream,
    pool: &Pool,
    cap: u64,
) -> Result<(Vec<u8>, Reservation)> {
    read_frame(stream, cap, &pool.budget, None).map_err(|error| {
        // Close stops the read. That answer is the writer closing, not a severed connection.
        if pool.cancel.is_cancelled() && matches!(error, Error::OwnerDisconnected) {
            Error::OwnerNotServing
        } else {
            error
        }
    })
}
fn handle(stream: &mut UnixStream, pool: &Pool) -> Result<Vec<u8>> {
    let (namespace, _name) = read_while_open(stream, pool, u16::MAX.into())?;
    let (bytes, _held) = read_while_open(stream, pool, u32::MAX.into())?;
    if pool.cancel.is_cancelled() {
        return Err(Error::OwnerNotServing);
    }
    if namespace != pool.namespace.as_bytes() {
        return Err(Error::OwnerRefused {
            reason: "namespace has no handler".into(),
        });
    }
    pool.handler.handle(&pool.namespace, &bytes, &pool.cancel)
}
/// Sends opaque bytes to the writer found through the store path, under this request's limits.
/// No writer returns `OwnerNotRunning`; a cut connection returns `OwnerDisconnected`.
pub fn request_owner(
    path: impl AsRef<Path>,
    namespace: &str,
    bytes: &[u8],
    limits: &ReadLimits,
) -> Result<Vec<u8>> {
    let size = namespace
        .len()
        .checked_add(bytes.len())
        .ok_or(Error::OwnerLimitExceeded)?;
    if namespace.len() > u16::MAX as usize
        || bytes.len() > u32::MAX as usize
        || size as u64 > limits.memory
    {
        return Err(Error::OwnerLimitExceeded);
    }
    let budget = Budget::new(None)?;
    let _request = charge(&budget, size)?;
    let directory = std::fs::read_link(path.as_ref().join("writer.channel"))
        .unwrap_or_else(|_| path.as_ref().into());
    let endpoint = Endpoint::new(&directory, path.as_ref()).map_err(|_| Error::OwnerNotRunning)?;
    let stream = endpoint.connect().map_err(|_| Error::OwnerNotRunning)?;
    exchange(stream, namespace, bytes, limits, &budget)
}
fn exchange(
    mut stream: UnixStream,
    namespace: &str,
    bytes: &[u8],
    limits: &ReadLimits,
    budget: &Budget,
) -> Result<Vec<u8>> {
    stream.set_nonblocking(true).map_err(disconnected)?;
    let clock = system::machine_clock();
    let allowance = Duration::from_millis(limits.active_ms);
    let wait = clock.now().checked_add(allowance).map(|at| (at, &*clock));
    // A writer can refuse before reading our frames. A failed write must still read
    // its status: orderly closure is distinct from a connection with no reply.
    let sent = write_frame(&mut stream, namespace.as_bytes(), wait)
        .and_then(|()| write_frame(&mut stream, bytes, wait));
    if matches!(sent, Err(Error::QueryCanceled { .. })) {
        return sent.map(|()| Vec::new());
    }
    let mut status = [0];
    wait_io(1, wait, |at| stream.read(&mut status[at..]))?;
    if status[0] == 4 {
        let mut control = [0; 28];
        wait_io(28, wait, |at| stream.read(&mut control[at..]))?;
        if control[..4] != 24_u32.to_be_bytes() {
            return Err(Error::OwnerDisconnected);
        }
        let num = |at| u64::from_be_bytes(control[at..at + 8].try_into().expect("8"));
        return Err(Error::MemoryBudgetExceeded {
            subsystem: "writer request channel",
            operation: "writer request",
            requested_bytes: num(4),
            available_bytes: num(12),
            budget_limit_bytes: num(20),
            hint: "raise the writer memory limit or reduce the request",
        });
    }
    let cap = limits.memory - (namespace.len() + bytes.len()) as u64;
    let (answer, _answer) = read_frame(
        &mut stream,
        cap.min(if status[0] == 0 {
            limits.result_bytes
        } else {
            u16::MAX.into()
        }),
        budget,
        wait,
    )?;
    match status[0] {
        0 => sent.map(|()| answer),
        1 => Err(Error::OwnerNotServing),
        2 => Err(Error::OwnerRefused {
            reason: String::from_utf8_lossy(&answer).into(),
        }),
        3 => Err(Error::OwnerLimitExceeded),
        _ => Err(Error::OwnerDisconnected),
    }
}

#[cfg(test)]
mod tests;

use std::fs::File;
use std::io::{self, Seek, SeekFrom, Write};

/// Sets file length, reserving the extension with native allocation or zero writes.
/// A failed reservation leaves the existing contents unchanged.
pub fn set_len(mut file: &File, len: u64) -> io::Result<()> {
    let mut from = file.metadata()?.len();
    if len > from {
        #[cfg(any(
            target_os = "linux",
            target_os = "android",
            target_os = "freebsd",
            target_os = "dragonfly",
            target_os = "solaris",
            target_os = "illumos",
            target_os = "haiku",
            target_os = "aix",
            target_os = "nto",
            target_os = "emscripten",
            target_os = "hurd"
        ))]
        {
            use std::os::fd::AsRawFd;
            let start = from.try_into().map_err(|_| io::ErrorKind::InvalidInput)?;
            let amount = (len - from)
                .try_into()
                .map_err(|_| io::ErrorKind::InvalidInput)?;
            loop {
                // SAFETY: the file descriptor and checked positive extent are valid.
                let result = unsafe { libc::posix_fallocate(file.as_raw_fd(), start, amount) };
                match result {
                    0 => return file.set_len(len),
                    libc::EINTR => {}
                    libc::EINVAL | libc::EOPNOTSUPP => break,
                    other => return Err(io::Error::from_raw_os_error(other)),
                }
            }
        }
        file.seek(SeekFrom::Start(from))?;
        let zeros = [0; 8192];
        while from < len {
            let size = usize::try_from((len - from).min(zeros.len() as u64)).unwrap();
            file.write_all(&zeros[..size])?;
            from += size as u64;
        }
    }
    file.set_len(len)
}

//! Fabric-owned node identity: the ed25519 keypair a machine holds for the
//! work fabric. The fabric generates and stores this keypair and hands it to
//! the transport; the transport never mints it. `node_id` written anywhere in
//! the stack derives from this module, so identity survives a transport swap.
//!
//! This module is transport-neutral by design: it must not name any concrete
//! transport. The work ledger consumes `node_id` from here; the transport
//! adapter consumes the secret seed from here.

use contextdb_core::Error;
use std::io::Write;
use std::path::Path;

/// A machine's fabric identity: a persisted ed25519 keypair.
pub struct FabricIdentity {
    seed: [u8; 32],
}

impl FabricIdentity {
    /// Load the identity stored at `path`, or generate one and persist it
    /// there (owner-only file permissions). The same path always yields the
    /// same identity.
    pub fn load_or_generate(path: &Path) -> contextdb_core::Result<Self> {
        if path.exists() {
            return Self::load(path);
        }
        let identity = Self::generate();
        identity.persist(path)?;
        Ok(identity)
    }

    fn load(path: &Path) -> contextdb_core::Result<Self> {
        let bytes = std::fs::read(path).map_err(|err| {
            Error::SyncError(format!("cannot read fabric identity at {path:?}: {err}"))
        })?;
        let seed: [u8; 32] = bytes.as_slice().try_into().map_err(|_| {
            Error::SyncError(format!(
                "fabric identity file {path:?} is corrupt: expected 32 secret-seed bytes, found {}",
                bytes.len()
            ))
        })?;
        Ok(Self { seed })
    }

    fn persist(&self, path: &Path) -> contextdb_core::Result<()> {
        if let Some(parent) = path.parent()
            && !parent.as_os_str().is_empty()
        {
            std::fs::create_dir_all(parent).map_err(|err| {
                Error::SyncError(format!(
                    "cannot create fabric identity directory {parent:?}: {err}"
                ))
            })?;
        }
        let mut options = std::fs::OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let mut file = options.open(path).map_err(|err| {
            Error::SyncError(format!("cannot create fabric identity at {path:?}: {err}"))
        })?;
        file.write_all(&self.seed).map_err(|err| {
            Error::SyncError(format!("cannot write fabric identity at {path:?}: {err}"))
        })?;
        Ok(())
    }

    /// Generate a fresh identity without persisting it (ephemeral).
    pub fn generate() -> Self {
        let mut seed = [0u8; 32];
        getrandom::fill(&mut seed).expect("operating system randomness must be available");
        Self { seed }
    }

    /// The node's public identity as a lowercase hex string of the ed25519
    /// public key (64 chars). This is the `node_id` the fabric records.
    pub fn node_id(&self) -> String {
        let mut out = String::with_capacity(64);
        for byte in self.public_key_bytes() {
            use std::fmt::Write as _;
            let _ = write!(out, "{byte:02x}");
        }
        out
    }

    /// The raw ed25519 public key bytes.
    pub fn public_key_bytes(&self) -> [u8; 32] {
        ed25519_dalek::SigningKey::from_bytes(&self.seed)
            .verifying_key()
            .to_bytes()
    }

    /// The raw ed25519 secret seed, handed to the transport adapter as the
    /// endpoint's identity. Never logged, never synced.
    pub fn secret_seed(&self) -> [u8; 32] {
        self.seed
    }
}

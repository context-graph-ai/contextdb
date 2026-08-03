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
        let bytes = std::fs::read(path).map_err(|_| {
            Error::SyncError("cannot read fabric identity file".to_string())
        })?;
        let seed: [u8; 32] = bytes.as_slice().try_into().map_err(|_| {
            Error::SyncError(format!(
                "fabric identity file is corrupt: expected 32 secret-seed bytes, found {}",
                bytes.len()
            ))
        })?;
        Ok(Self { seed })
    }

    fn persist(&self, path: &Path) -> contextdb_core::Result<()> {
        if let Some(parent) = path.parent()
            && !parent.as_os_str().is_empty()
        {
            std::fs::create_dir_all(parent).map_err(|_| {
                Error::SyncError("cannot create fabric identity directory".to_string())
            })?;
        }
        let mut options = std::fs::OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let mut file = options.open(path).map_err(|_| {
            Error::SyncError("cannot create fabric identity file".to_string())
        })?;
        file.write_all(&self.seed).map_err(|_| {
            Error::SyncError("cannot write fabric identity file".to_string())
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

    /// Sign immutable row-lineage bytes.  The byte construction remains in
    /// the sync boundary; this type only owns the fabric key material.
    pub(crate) fn sign_lineage(&self, bytes: &[u8]) -> Vec<u8> {
        use ed25519_dalek::Signer as _;

        ed25519_dalek::SigningKey::from_bytes(&self.seed)
            .sign(bytes)
            .to_bytes()
            .to_vec()
    }

    /// Verify a lineage signature using the creator identity carried on the
    /// wire.  `node_id` is the lowercase hex Ed25519 public key used by the
    /// fabric, so verification never needs a local key registry.
    pub(crate) fn verify_lineage_by_node_id(
        node_id: &str,
        bytes: &[u8],
        signature: &[u8],
    ) -> contextdb_core::Result<()> {
        use ed25519_dalek::Verifier as _;

        let public_bytes = hex_node_id(node_id)?;
        let verifying = ed25519_dalek::VerifyingKey::from_bytes(&public_bytes).map_err(|err| {
            Error::SyncError(format!("invalid lineage author node identity: {err}"))
        })?;
        let signature = ed25519_dalek::Signature::from_slice(signature).map_err(|err| {
            Error::SyncError(format!("invalid lineage signature encoding: {err}"))
        })?;
        verifying.verify(bytes, &signature).map_err(|_| {
            Error::SyncError(
                "wire row lineage signature does not verify for its author".to_string(),
            )
        })
    }

    /// The raw ed25519 secret seed, handed only to this crate's transport
    /// adapter. Never logged, synced, or exposed to embedding callers.
    pub(crate) fn secret_seed(&self) -> [u8; 32] {
        self.seed
    }
}

fn hex_node_id(node_id: &str) -> contextdb_core::Result<[u8; 32]> {
    if node_id.len() != 64
        || !node_id
            .bytes()
            .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
    {
        return Err(Error::SyncError(
            "lineage author node identity must be 32 lowercase hex bytes".to_string(),
        ));
    }
    let mut bytes = [0u8; 32];
    for (index, pair) in node_id.as_bytes().chunks_exact(2).enumerate() {
        let text = std::str::from_utf8(pair).map_err(|_| {
            Error::SyncError("lineage author node identity is not valid hex".to_string())
        })?;
        bytes[index] = u8::from_str_radix(text, 16).map_err(|_| {
            Error::SyncError("lineage author node identity is not valid hex".to_string())
        })?;
    }
    Ok(bytes)
}

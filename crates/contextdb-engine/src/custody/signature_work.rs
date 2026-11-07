//! Successful signature mathematics scoped to one synchronous delivery phase.
//! No authorization, canonical decoding, root, or freshness decision is cached.
use super::native_selection::{NativeSelectionHold, Selected};
use contextdb_core::read_memory::ReadMemoryOwner;
use std::cell::RefCell;

struct Memo {
    owner: ReadMemoryOwner,
    signatures: Option<NativeSelectionHold<[u8; 32]>>,
}
thread_local! {
    static CURRENT: RefCell<Option<Memo>> = const { RefCell::new(None) };
}
struct Restore(Option<Memo>);
impl Drop for Restore {
    fn drop(&mut self) {
        CURRENT.with(|current| *current.borrow_mut() = self.0.take());
    }
}
/// A closure cannot suspend: its memo never crosses an await or changes threads.
/// Nested phases get independent owners and restore the outer phase on unwind.
pub(crate) fn scoped<T>(owner: &ReadMemoryOwner, run: impl FnOnce() -> T) -> T {
    let previous = CURRENT.with(|current| {
        current.replace(Some(Memo {
            owner: owner.clone(),
            signatures: None,
        }))
    });
    let _restore = Restore(previous);
    run()
}
pub(crate) fn digest(public: &[u8; 32], bytes: &[u8], signature: &[u8; 64]) -> [u8; 32] {
    let mut hash = blake3::Hasher::new_derive_key("contextdb.delivery.ed25519-verification.v1");
    hash.update(public);
    hash.update(&(bytes.len() as u64).to_be_bytes());
    hash.update(bytes);
    hash.update(signature);
    *hash.finalize().as_bytes()
}
pub(crate) fn known(digest: &[u8; 32]) -> bool {
    CURRENT.with(|current| {
        current.borrow().as_ref().is_some_and(|memo| {
            memo.signatures
                .as_ref()
                .is_some_and(|slots| slots.as_slice().contains(digest))
        })
    })
}
#[expect(
    clippy::result_large_err,
    reason = "Native refusal is retained inline."
)]
pub(crate) fn remember(digest: [u8; 32]) -> Selected<()> {
    CURRENT.with(|current| {
        let mut current = current.borrow_mut();
        let Some(memo) = current.as_mut() else {
            return Ok(());
        };
        let len = memo
            .signatures
            .as_ref()
            .map_or(0, |slots| slots.as_slice().len());
        // Exact slots are paid before allocation; old storage stays paid until
        // every digest has moved. No successful proof is evicted within a phase.
        if len == 0 || len.is_power_of_two() && len >= 16 {
            let capacity = if len == 0 {
                16
            } else {
                len.checked_mul(2)
                    .ok_or(super::native_decode::Invalid::Codec)?
            };
            let mut replacement = NativeSelectionHold::try_new(&memo.owner, capacity)?;
            if let Some(old) = &memo.signatures {
                for digest in old.as_slice() {
                    replacement.retain(*digest)?;
                }
            }
            memo.signatures = Some(replacement);
        }
        memo.signatures
            .as_mut()
            .expect("memo slots admitted")
            .retain(digest)
    })
}
#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        custody::{
            native_decode::{Bytes, Decode, NodeView},
            native_record_codec,
        },
        identity::FabricIdentity,
    };
    use contextdb_core::read_memory::{NativeRefusal, ReadMemoryBudget};
    use std::sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    };

    #[derive(Debug, Default)]
    struct Funds {
        held: AtomicUsize,
        refuse: AtomicBool,
    }
    impl ReadMemoryBudget for Funds {
        fn try_reserve(
            &self,
            bytes: usize,
            _: &'static str,
            _: &'static str,
            _: &'static str,
        ) -> contextdb_core::Result<()> {
            self.held.fetch_add(bytes, Ordering::Relaxed);
            Ok(())
        }
        fn try_reserve_native(
            &self,
            bytes: usize,
            _: &'static str,
            _: &'static str,
            _: &'static str,
        ) -> Result<(), NativeRefusal> {
            if self.refuse.load(Ordering::Relaxed) {
                return Err(NativeRefusal::Other("proof test refusal"));
            }
            self.held.fetch_add(bytes, Ordering::Relaxed);
            Ok(())
        }
        fn native_reserve_supported(&self) -> bool {
            true
        }
        fn release(&self, bytes: usize) {
            self.held.fetch_sub(bytes, Ordering::Relaxed);
        }
    }
    fn owner() -> (Arc<Funds>, ReadMemoryOwner) {
        let funds = Arc::new(Funds::default());
        let owner = ReadMemoryOwner::from(funds.clone());
        (funds, owner)
    }
    #[test]
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    fn cached_success_still_rejects_tampered_bytes_swapped_key_and_swapped_signature() {
        let (funds, owner) = owner();
        let signer = FabricIdentity::generate();
        let other = FabricIdentity::generate();
        let message = b"original signed page component";
        let signature = signer.sign_lineage_fixed(message);
        let swapped = other.sign_lineage_fixed(message);
        let proof = digest(&signer.public_key_bytes(), message, &signature);
        let native = |node: &str, message: &[u8], signature: &[u8; 64]| {
            let encoded = rmp_serde::to_vec(signature.as_slice()).unwrap();
            let signature = Bytes::decode(NodeView::parse(&encoded).unwrap()).unwrap();
            native_record_codec::verify_signature(&owner, node, signature, |e| {
                e.raw(message);
                Ok(())
            })
        };
        let before = crate::identity::PAGING_VERIFICATIONS.load(Ordering::Relaxed);
        scoped(&owner, || {
            FabricIdentity::verify_lineage_by_node_id(&signer.node_id(), message, &signature)
                .unwrap();
            assert!(known(&proof));
            native(&signer.node_id(), message, &signature).unwrap();
            FabricIdentity::verify_lineage_by_node_id(&signer.node_id(), message, &signature)
                .unwrap();
            assert_eq!(
                crate::identity::PAGING_VERIFICATIONS.load(Ordering::Relaxed) - before,
                1,
                "both verification routes reuse the same successful signature math"
            );
            for (node, bytes, sig) in [
                (
                    signer.node_id(),
                    b"Original signed page component".as_slice(),
                    &signature,
                ),
                (other.node_id(), message.as_slice(), &signature),
                (signer.node_id(), message.as_slice(), &swapped),
            ] {
                assert!(FabricIdentity::verify_lineage_by_node_id(&node, bytes, sig).is_err());
                assert!(native(&node, bytes, sig).is_err());
                assert!(
                    known(&proof),
                    "original success remains cached during each negative control"
                );
                assert!(!known(&digest(
                    &crate::custody::native_record_codec::node_bytes(&node).unwrap(),
                    bytes,
                    sig
                )));
            }
            native(&signer.node_id(), message, &signature).unwrap();
            assert_eq!(
                crate::identity::PAGING_VERIFICATIONS.load(Ordering::Relaxed) - before,
                7,
                "six invalid attempts still execute signature verification"
            );
            assert!(funds.held.load(Ordering::Relaxed) > 0);
        });
        assert!(!known(&proof));
        assert_eq!(funds.held.load(Ordering::Relaxed), 0);
        scoped(&owner, || {
            assert!(!known(&proof), "next delivery/retry starts cold");
            native(&signer.node_id(), message, &signature).unwrap();
            assert!(known(&proof));
        });
        assert_eq!(funds.held.load(Ordering::Relaxed), 0);
    }
    #[test]
    fn memo_growth_never_evicts_and_unwind_releases_original_owner() {
        let (funds, owner) = owner();
        let first = [1; 32];
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            scoped(&owner, || {
                for n in 1..=130u8 {
                    remember([n; 32]).unwrap();
                }
                for n in 1..=130u8 {
                    assert!(known(&[n; 32]));
                }
                scoped(&owner, || {
                    assert!(!known(&first));
                    remember([255; 32]).unwrap();
                });
                assert!(known(&first));
                assert!(!known(&[255; 32]));
                panic!("cancel phase by unwinding");
            })
        }));
        assert!(!known(&first));
        assert_eq!(funds.held.load(Ordering::Relaxed), 0);
    }
    #[test]
    fn proof_admission_refusal_retains_no_success_and_refunds_scratch() {
        let (funds, owner) = owner();
        let signer = FabricIdentity::generate();
        let message = b"signed component";
        let signature = signer.sign_lineage_fixed(message);
        let proof = digest(&signer.public_key_bytes(), message, &signature);
        funds.refuse.store(true, Ordering::Relaxed);
        scoped(&owner, || {
            assert!(
                FabricIdentity::verify_lineage_by_node_id(&signer.node_id(), message, &signature)
                    .is_err()
            );
            assert!(!known(&proof));
            funds.refuse.store(false, Ordering::Relaxed);
            FabricIdentity::verify_lineage_by_node_id(&signer.node_id(), message, &signature)
                .unwrap();
            assert!(known(&proof));
        });
        assert_eq!(funds.held.load(Ordering::Relaxed), 0);
    }
}

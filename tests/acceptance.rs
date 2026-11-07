#![allow(clippy::assertions_on_constants)]

// Allocator counter wrapper -- installed binary-wide so t29_display_impl_does_not_allocate
// can measure allocator activity directly. Counting is gated by a thread-local
// flag, which the Display probes flip on around measured writes only; unrelated
// test threads cannot contaminate the delta.
use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};

pub static ALLOC_COUNT: AtomicUsize = AtomicUsize::new(0);

thread_local! {
    pub static COUNT_ENABLED: Cell<bool> = const { Cell::new(false) };
}

pub struct AllocCounter;

unsafe impl GlobalAlloc for AllocCounter {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        if COUNT_ENABLED
            .try_with(|enabled| enabled.get())
            .unwrap_or(false)
        {
            ALLOC_COUNT.fetch_add(1, AtomicOrdering::SeqCst);
        }
        unsafe { System.alloc(layout) }
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) };
    }
}

#[global_allocator]
static GLOBAL: AllocCounter = AllocCounter;

#[path = "acceptance/auto_stamped_txid.rs"]
mod auto_stamped_txid;
#[path = "support/binary_path.rs"]
mod binary_path;
#[path = "acceptance/cli_ux.rs"]
mod cli_ux;
#[path = "acceptance/common.rs"]
mod common;
#[cfg(feature = "test-seams")]
#[path = "acceptance/common_tracing.rs"]
mod common_tracing;
#[path = "acceptance/composite_foreign_key.rs"]
mod composite_foreign_key;
#[path = "acceptance/composite_index_routing.rs"]
mod composite_index_routing;
#[path = "acceptance/composite_unique_concurrent_update.rs"]
mod composite_unique_concurrent_update;
#[path = "acceptance/concurrent_unique_revalidation.rs"]
mod concurrent_unique_revalidation;
#[path = "acceptance/conditional_update_serializable.rs"]
mod conditional_update_serializable;
#[path = "acceptance/contributing.rs"]
mod contributing;
#[path = "acceptance/data_integrity.rs"]
mod data_integrity;
#[path = "acceptance/db_lock_visibility.rs"]
mod db_lock_visibility;
#[path = "acceptance/deployment.rs"]
mod deployment;
#[path = "acceptance/dev_experience.rs"]
mod dev_experience;
#[path = "acceptance/disk_budget.rs"]
mod disk_budget;
#[path = "acceptance/docs_immutable_column.rs"]
mod docs_immutable_column;
#[path = "acceptance/docs_query_language_auto_indexes.rs"]
mod docs_query_language_auto_indexes;
#[path = "acceptance/embedding.rs"]
mod embedding;
#[path = "acceptance/engine_cron.rs"]
mod engine_cron;
#[path = "acceptance/event_bus.rs"]
mod event_bus;
#[path = "acceptance/event_bus_self_join.rs"]
mod event_bus_self_join;
#[path = "acceptance/foreign_key_delete_race.rs"]
mod foreign_key_delete_race;
#[path = "acceptance/graph_adjacency_routing.rs"]
mod graph_adjacency_routing;
#[path = "acceptance/handle_scope_constraint.rs"]
mod handle_scope_constraint;
#[path = "acceptance/infra_failures.rs"]
mod infra_failures;
#[path = "acceptance/memory_accounting.rs"]
mod memory_accounting;
#[path = "acceptance/multi_edge.rs"]
mod multi_edge;
#[path = "acceptance/multi_table_atomic_visibility.rs"]
mod multi_table_atomic_visibility;
#[path = "acceptance/persistence.rs"]
mod persistence;
#[path = "acceptance/precommit_context_guard.rs"]
mod precommit_context_guard;
#[path = "acceptance/principal_scoped_handle.rs"]
mod principal_scoped_handle;
#[cfg(not(feature = "test-seams"))]
#[path = "acceptance/production_layout_inventory.rs"]
mod production_layout_inventory;
#[path = "acceptance/query_surface.rs"]
mod query_surface;
#[path = "acceptance/schema_evolution.rs"]
mod schema_evolution;
#[path = "acceptance/schema_lifecycle.rs"]
mod schema_lifecycle;
#[path = "acceptance/server_owner_reads.rs"]
mod server_owner_reads;
#[path = "acceptance/single_pk_concurrent_update.rs"]
mod single_pk_concurrent_update;
#[path = "acceptance/snapshot_read_stability.rs"]
mod snapshot_read_stability;
#[path = "acceptance/state_machine_concurrent_legal_transition.rs"]
mod state_machine_concurrent_legal_transition;
#[path = "acceptance/state_machine_self_edge_check.rs"]
mod state_machine_self_edge_check;
#[path = "acceptance/storage_scale_hardening.rs"]
mod storage_scale_hardening;
#[path = "acceptance/test_alloc_counter.rs"]
pub mod test_alloc_counter;
#[path = "acceptance/trigger.rs"]
mod trigger;
#[path = "acceptance/trigger_concurrency_panic_freedom.rs"]
mod trigger_concurrency_panic_freedom;
#[path = "acceptance/trigger_same_db_progress.rs"]
mod trigger_same_db_progress;
#[path = "acceptance/txid_monotonic_concurrent_commits.rs"]
mod txid_monotonic_concurrent_commits;
#[path = "acceptance/vector_reindex_ordering.rs"]
mod vector_reindex_ordering;

#[cfg(not(feature = "test-seams"))]
#[path = "acceptance/production_layout.rs"]
mod production_layout;

#[cfg(target_os = "linux")]
#[test]
fn a_server_fixture_cannot_survive_an_unwinding_or_killed_test() {
    use common::{ServerChild, wait_for_child_stdout_contains, with_parent_death};
    use std::io::Write;
    use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
    use std::process::{Command, Stdio};
    use std::time::Duration;
    const CHILD: &str = "CONTEXTDB_SERVER_FIXTURE_LIFETIME_CHILD";
    fn spawn(mode: &str) -> ServerChild {
        ServerChild::new(
            with_parent_death(Command::new(
                std::env::current_exe().expect("test executable"),
            ))
            .args([
                "--exact",
                "a_server_fixture_cannot_survive_an_unwinding_or_killed_test",
                "--nocapture",
            ])
            .env(CHILD, mode)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()
            .expect("spawn guarded fixture child"),
        )
    }
    match std::env::var(CHILD).ok().as_deref() {
        Some("child") => {
            println!("server fixture child ready");
            std::io::stdout().flush().expect("publish readiness");
            loop {
                std::thread::park();
            }
        }
        Some("parent") => {
            let mut child = spawn("child");
            let ready = wait_for_child_stdout_contains(
                &mut child,
                "server fixture child ready",
                Duration::from_secs(15),
            );
            assert!(ready.contains("server fixture child ready"));
            println!("server fixture descendant={}", child.id());
            std::io::stdout()
                .flush()
                .expect("publish descendant identity");
            // The test process will be killed while this child remains live.
            loop {
                std::thread::park();
            }
        }
        None => {}
        Some(mode) => panic!("unknown fixture mode {mode}"),
    }
    let mut child = spawn("child");
    let ready = wait_for_child_stdout_contains(
        &mut child,
        "server fixture child ready",
        Duration::from_secs(15),
    );
    assert!(ready.contains("server fixture child ready"));
    let pid = child.id() as libc::pid_t;
    let unwound = std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || {
        let _child = child;
        panic!("exercise fixture cleanup during unwind");
    }));
    assert!(unwound.is_err());
    let mut status = 0;
    assert_eq!(
        unsafe { libc::waitpid(pid, &mut status, libc::WNOHANG) },
        -1
    );
    assert_eq!(
        std::io::Error::last_os_error().raw_os_error(),
        Some(libc::ECHILD),
        "the guard killed and reaped the child during unwind"
    );

    // Adopt the descendant so this test also reaps it after its parent dies.
    assert_eq!(unsafe { libc::prctl(libc::PR_SET_CHILD_SUBREAPER, 1) }, 0);
    let mut parent = spawn("parent");
    let ready = wait_for_child_stdout_contains(
        &mut parent,
        "server fixture descendant=",
        Duration::from_secs(15),
    );
    let descendant: libc::pid_t = ready
        .lines()
        .find_map(|line| {
            line.split_once("server fixture descendant=")
                .map(|(_, pid)| pid)
        })
        .expect("parent reported its live descendant")
        .parse()
        .expect("descendant pid");
    let fd = unsafe { libc::syscall(libc::SYS_pidfd_open, descendant, 0) } as i32;
    assert!(
        fd >= 0,
        "open the exact live descendant: {}",
        std::io::Error::last_os_error()
    );
    let fd = unsafe { OwnedFd::from_raw_fd(fd) };
    parent.kill().expect("simulate a killed test process");
    parent.wait().expect("reap the killed test process");
    let mut poll = libc::pollfd {
        fd: fd.as_raw_fd(),
        events: libc::POLLIN,
        revents: 0,
    };
    let exited = unsafe { libc::poll(&mut poll, 1, 15_000) };
    // Even a failed assertion cannot leave this fixture running on the host.
    unsafe {
        libc::syscall(
            libc::SYS_pidfd_send_signal,
            fd.as_raw_fd(),
            libc::SIGKILL,
            std::ptr::null::<libc::siginfo_t>(),
            0,
        );
    }
    assert_eq!(
        unsafe { libc::waitpid(descendant, &mut status, 0) },
        descendant
    );
    assert_eq!(
        exited, 1,
        "the kernel ended the child when its test parent died"
    );
    assert!(libc::WIFSIGNALED(status));
    assert_eq!(libc::WTERMSIG(status), libc::SIGKILL);
}

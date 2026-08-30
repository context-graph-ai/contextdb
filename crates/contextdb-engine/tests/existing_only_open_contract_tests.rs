#![cfg(all(unix, feature = "test-seams"))]
//! Editing an existing store never creates one.
//!
//! A consumer that means to CHANGE a store it already has must be able to say
//! so in the open itself. Deciding open-versus-create on whether the path
//! happens to exist at that instant leaves that consumer two bad choices: look
//! the path up first and race whoever moves it between the look and the open,
//! or accept that a typo, an unmounted volume, or a link whose target is gone
//! silently materializes a brand-new empty deployment and reports success. The
//! second choice is the dangerous one, because the caller is told everything
//! worked and goes on to write its data into a store nobody will ever look for.
//!
//! `OpenDisposition::ExistingOnly` removes the choice. The open is ATTEMPTED --
//! there is no separate existence question whose answer could go stale -- and a
//! store that is not there comes back as a typed missing refusal that names the
//! path. A store that is there but cannot be looked at is a DIFFERENT answer:
//! unreadable, not missing, because "it is not there" would send an operator
//! off to restore a backup when the real fix is a permission or a mount.
//!
//! The refusal is total. After any `ExistingOnly` refusal the directory holding
//! the store is byte-for-byte what it was: no store, no companion beside it, no
//! stray anything for the next caller to trip over. And the disposition is
//! additive -- an open that does not ask for it creates an absent store exactly
//! as it always has.

use contextdb_core::{Error, Value};
use contextdb_engine::read_session::{ReadSessionEvent, ReadSessionTestObserver};
use contextdb_engine::{Database, DatabaseOpenOptions, OpenDisposition};
use std::collections::{BTreeMap, HashMap};
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

/// The open a consumer writes when it means to change a store it already has.
fn existing_only() -> DatabaseOpenOptions {
    DatabaseOpenOptions {
        open_disposition: OpenDisposition::ExistingOnly,
        ..DatabaseOpenOptions::default()
    }
}

/// The same open, watched at its own milestones.
fn existing_only_watched(observer: Arc<dyn ReadSessionTestObserver>) -> DatabaseOpenOptions {
    DatabaseOpenOptions {
        open_disposition: OpenDisposition::ExistingOnly,
        test_observer: Some(observer),
        ..DatabaseOpenOptions::default()
    }
}

/// One entry in a directory, with enough of it to notice any change.
///
/// A link is recorded by what it points at rather than by what is at the other
/// end, so a directory holding a dangling link is still comparable.
#[derive(Debug, PartialEq, Eq)]
enum EntryImage {
    File(Vec<u8>),
    Symlink(PathBuf),
    Directory,
}

/// Everything a directory holds, by name, down to the bytes.
///
/// Two of these being equal is what "the refusal left the directory untouched"
/// means: not merely that no store appeared, but that no companion, no lock,
/// and no residue of any kind did either.
fn directory_image(directory: &Path) -> BTreeMap<String, EntryImage> {
    std::fs::read_dir(directory)
        .expect("read the directory holding the store")
        .map(|entry| {
            let entry = entry.expect("read one directory entry");
            let name = entry.file_name().to_string_lossy().into_owned();
            let kind = entry.file_type().expect("classify one directory entry");
            let image = if kind.is_symlink() {
                EntryImage::Symlink(
                    std::fs::read_link(entry.path()).expect("read one link's target"),
                )
            } else if kind.is_dir() {
                EntryImage::Directory
            } else {
                EntryImage::File(std::fs::read(entry.path()).expect("read one file's bytes"))
            };
            (name, image)
        })
        .collect()
}

/// A scratch directory named the way every open below will resolve it, so a
/// refusal that names a path can be compared against one written here.
fn scratch_root(directory: &tempfile::TempDir) -> PathBuf {
    std::fs::canonicalize(directory.path()).expect("resolve the scratch directory")
}

/// A real store with data in it, closed and let go.
fn seeded_store(directory: &Path, name: &str) -> PathBuf {
    let path = directory.join(name);
    let database = Database::open(&path).expect("the first run creates and takes the store");
    database
        .execute(
            "CREATE TABLE kept (id INTEGER PRIMARY KEY, body TEXT)",
            &HashMap::new(),
        )
        .expect("create the fixture table");
    database
        .execute(
            "INSERT INTO kept (id, body) VALUES ($id, $body)",
            &HashMap::from([
                ("id".to_owned(), Value::Int64(1)),
                ("body".to_owned(), Value::Text("first".to_owned())),
            ]),
        )
        .expect("insert the fixture row");
    database.close().expect("the first run lets the store go");
    path
}

/// An open that must refuse, with its refusal.
fn expect_refusal(result: contextdb_core::Result<Database>, context: &str) -> Error {
    match result {
        Ok(database) => {
            let _ = database.close();
            panic!("{context}: an existing-only open answered instead of refusing");
        }
        Err(error) => error,
    }
}

/// The refusal a caller can branch on, naming the store it could not open.
///
/// Either the name the caller handed in or the one it resolves to identifies
/// the store; what a caller must never get is a refusal that leaves them
/// guessing which path was meant.
fn assert_refused_as_missing(error: &Error, named_by: &[PathBuf], context: &str) {
    let Error::StoreMissing { path } = error else {
        panic!("{context}: expected the typed missing refusal, got: {error}");
    };
    let named = PathBuf::from(path);
    assert!(
        named_by.contains(&named),
        "{context}: the refusal must name the store it could not open; it named {named:?}, none \
         of {named_by:?}",
    );
}

#[test]
fn an_absent_path_is_refused_and_leaves_the_directory_untouched() {
    let directory = tempfile::tempdir().expect("scratch directory");
    let root = scratch_root(&directory);
    let store = root.join("never-created.db");
    let before = directory_image(&root);

    let refusal = expect_refusal(
        Database::open_with_options(&store, existing_only()),
        "absent path",
    );

    assert_refused_as_missing(&refusal, std::slice::from_ref(&store), "absent path");
    assert!(
        !store.exists(),
        "a refused existing-only open must never leave a store at {store:?}",
    );
    assert_eq!(
        before,
        directory_image(&root),
        "the refusal must leave the directory byte-for-byte as it found it -- no store, no \
         companion, no lock",
    );
}

#[test]
fn a_dangling_symlink_is_refused_rather_than_created_through() {
    let directory = tempfile::tempdir().expect("scratch directory");
    let root = scratch_root(&directory);
    let target = root.join("target-that-is-gone.db");
    let link = root.join("store-link.db");
    std::os::unix::fs::symlink(&target, &link)
        .expect("plant a link where a store the consumer expects used to be");
    let before = directory_image(&root);

    let refusal = expect_refusal(
        Database::open_with_options(&link, existing_only()),
        "dangling link",
    );

    assert_refused_as_missing(&refusal, &[link.clone(), target.clone()], "dangling link");
    assert!(
        !target.exists(),
        "a link whose target is gone must never be created through: nothing may appear at \
         {target:?}",
    );
    assert!(
        std::fs::symlink_metadata(&link)
            .expect("the link itself survives the refusal")
            .file_type()
            .is_symlink(),
        "the refusal must leave the caller's own path as it found it",
    );
    assert_eq!(
        before,
        directory_image(&root),
        "the refusal must leave the directory byte-for-byte as it found it",
    );
}

#[test]
fn an_unreadable_parent_is_refused_as_unreadable_never_as_missing() {
    let directory = tempfile::tempdir().expect("scratch directory");
    let root = scratch_root(&directory);
    let vault = root.join("vault");
    std::fs::create_dir(&vault).expect("create the directory the store lives in");
    let store = seeded_store(&vault, "sealed.db");
    let before = directory_image(&vault);

    std::fs::set_permissions(&vault, std::fs::Permissions::from_mode(0o000))
        .expect("seal the directory the store lives in");
    let refusal = Database::open_with_options(&store, existing_only());
    let sealed_answer = match refusal {
        Ok(database) => {
            let _ = database.close();
            None
        }
        Err(error) => Some(error),
    };
    std::fs::set_permissions(&vault, std::fs::Permissions::from_mode(0o700))
        .expect("unseal the directory so the rest of the journey can look at it");

    let refusal = sealed_answer
        .expect("an existing-only open of a store this process cannot look at must refuse");
    assert!(
        !matches!(refusal, Error::StoreMissing { .. }),
        "a store that cannot be looked at is UNREADABLE, not missing -- calling it missing sends \
         an operator to restore a backup when the fix is a permission: {refusal}",
    );
    assert_eq!(
        before,
        directory_image(&vault),
        "the refusal must leave the directory byte-for-byte as it found it",
    );
}

/// A store taken away underneath the claim the writer just took on it.
///
/// `CompanionClaimTaken` is the real milestone the admission window opens at,
/// and the observer runs on the opening thread, so the store is gone before the
/// open reads a byte of it. Nothing here waits on another thread and nothing
/// here depends on timing.
struct StoreThatVanishesUnderTheClaim {
    store: PathBuf,
    vanished: AtomicBool,
    image_after_vanishing: Mutex<Option<BTreeMap<String, EntryImage>>>,
}

impl StoreThatVanishesUnderTheClaim {
    fn watching(store: PathBuf) -> Self {
        Self {
            store,
            vanished: AtomicBool::new(false),
            image_after_vanishing: Mutex::new(None),
        }
    }

    fn has_vanished(&self) -> bool {
        self.vanished.load(Ordering::SeqCst)
    }

    fn image_after_vanishing(&self) -> BTreeMap<String, EntryImage> {
        self.image_after_vanishing
            .lock()
            .expect("the recorded directory image")
            .take()
            .expect("the journey recorded the directory the moment the store vanished")
    }
}

impl ReadSessionTestObserver for StoreThatVanishesUnderTheClaim {
    fn observe_event(&self, event: ReadSessionEvent) {
        if event != ReadSessionEvent::CompanionClaimTaken
            || self.vanished.swap(true, Ordering::SeqCst)
        {
            return;
        }
        std::fs::remove_file(&self.store).expect("take the store away under the claim");
        let directory = self
            .store
            .parent()
            .expect("the store has a parent directory");
        *self
            .image_after_vanishing
            .lock()
            .expect("the recorded directory image") = Some(directory_image(directory));
    }
}

#[test]
fn a_store_that_vanishes_under_the_claim_is_refused_never_recreated() {
    let directory = tempfile::tempdir().expect("scratch directory");
    let root = scratch_root(&directory);
    let store = seeded_store(&root, "vanishing.db");
    let vanishing = Arc::new(StoreThatVanishesUnderTheClaim::watching(store.clone()));

    let refusal = expect_refusal(
        Database::open_with_options(&store, existing_only_watched(vanishing.clone())),
        "store taken away under the claim",
    );

    assert!(
        vanishing.has_vanished(),
        "the journey never reached the claim it was written to interrupt",
    );
    assert_refused_as_missing(
        &refusal,
        std::slice::from_ref(&store),
        "store taken away under the claim",
    );
    assert!(
        !store.exists(),
        "a store that disappeared mid-admission must be refused, never recreated at {store:?}",
    );
    assert_eq!(
        vanishing.image_after_vanishing(),
        directory_image(&root),
        "the refusal must leave the directory exactly as the disappearance left it",
    );
}

#[test]
fn an_existing_store_opens_for_change_and_the_writes_land() {
    let directory = tempfile::tempdir().expect("scratch directory");
    let root = scratch_root(&directory);
    let store = seeded_store(&root, "kept.db");

    let database = Database::open_with_options(&store, existing_only())
        .expect("an existing store opens for change under the existing-only disposition");
    database
        .execute(
            "INSERT INTO kept (id, body) VALUES ($id, $body)",
            &HashMap::from([
                ("id".to_owned(), Value::Int64(2)),
                ("body".to_owned(), Value::Text("second".to_owned())),
            ]),
        )
        .expect("the change a consumer opened the store to make");
    database.close().expect("let the store go");

    let reopened = Database::open_with_options(&store, existing_only())
        .expect("the changed store opens again under the same disposition");
    let answer = reopened
        .execute("SELECT body FROM kept ORDER BY id", &HashMap::new())
        .expect("read the store back");
    reopened.close().expect("let the store go again");

    assert_eq!(
        answer.rows.len(),
        2,
        "both the seeded row and the row written through the existing-only open must be there",
    );
    assert_eq!(
        answer.rows[1][0],
        Value::Text("second".to_owned()),
        "the write made through the existing-only open must have landed",
    );
}

#[test]
fn the_default_disposition_still_creates_an_absent_store() {
    assert_eq!(
        DatabaseOpenOptions::default().open_disposition,
        OpenDisposition::CreateIfMissing,
        "the disposition is additive: an open that says nothing about it keeps creating",
    );

    let directory = tempfile::tempdir().expect("scratch directory");
    let root = scratch_root(&directory);

    let through_options = root.join("created-through-options.db");
    let database = Database::open_with_options(&through_options, DatabaseOpenOptions::default())
        .expect("the default disposition creates an absent store");
    database
        .execute(
            "CREATE TABLE kept (id INTEGER PRIMARY KEY, body TEXT)",
            &HashMap::new(),
        )
        .expect("the created store takes a schema");
    database.close().expect("let the created store go");
    assert!(
        through_options.exists(),
        "the existing promise stays: a default open of an absent path creates it",
    );

    let through_plain_open = root.join("created-through-plain-open.db");
    let database = Database::open(&through_plain_open)
        .expect("the plain opener creates an absent store as it always has");
    database.close().expect("let the created store go");
    assert!(
        through_plain_open.exists(),
        "the existing promise stays for every caller that never mentions a disposition",
    );
}

//! A second process: this test program started again in a role, and spoken to over its pipes.

use std::fmt;
use std::io::{self, BufRead, BufReader, Write};
use std::os::fd::OwnedFd;
use std::os::unix::process::ExitStatusExt;
use std::path::{Path, PathBuf};
use std::process::{Command, ExitStatus, Stdio};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{self, Receiver};

// ---- a second process: the parent's side ----

/// A second process: this test program started again to run one test in a role. It announces
/// what it has done in lines that start with `@ `, and is told what to do next in lines on its
/// standard input. When the value is dropped the process is killed.
pub struct Child {
    process: std::process::Child,
    lines: Option<Receiver<String>>,
    announced: Vec<String>,
    status: Option<ExitStatus>,
}

impl Child {
    /// Starts the child. `test` is the name of the calling test function, which must begin with
    /// `if let Some(role) = support::role() { return child(role); }`. `role` is the role's name,
    /// then any settings as `key=number` words.
    pub fn start(test: &str, role: &str, folder: &Path) -> Child {
        Child::started(test, role, folder, Stdio::inherit())
    }

    /// [`Child::start`] for a child that is handed an open file, which it keeps open for as
    /// long as it runs: the file is its standard error, the one channel the roles do not use.
    pub fn start_holding(test: &str, role: &str, folder: &Path, held: OwnedFd) -> Child {
        Child::started(test, role, folder, Stdio::from(held))
    }

    fn started(test: &str, role: &str, folder: &Path, stderr: Stdio) -> Child {
        let exe = std::env::current_exe().expect("the path of this test program");
        let mut command = Command::new(&exe);
        if role.starts_with("lock-io") {
            // strace makes the first lock call fail with ENOLCK, which nothing in-process can do.
            command = Command::new("strace");
            command
                .arg("-f")
                .args(["-e", "inject=flock:error=ENOLCK:when=1", "-o"])
                .arg(folder.join(format!("{role}.trace")))
                .arg(&exe);
        }
        let process = command
            .args(["--exact", test, "--nocapture"])
            .env_remove("RUST_TEST_THREADS")
            .env("CONTEXTDB_TEST_ROLE", role)
            .env("CONTEXTDB_TEST_FOLDER", folder)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(stderr)
            .spawn()
            .expect("start this test program a second time");
        Child {
            process,
            lines: None,
            announced: Vec::new(),
            status: None,
        }
    }

    pub fn ask(&mut self, word: &str, answer: &str) {
        self.tell(word);
        self.expect(answer);
    }

    /// The child's process id.
    pub fn id(&self) -> u32 {
        self.process.id()
    }

    #[cfg(target_os = "linux")]
    pub fn limit(&self, name: &str, soft: &str) -> String {
        let pid = self.id().to_string();
        // prlimit (util-linux) sets a limit on a running child from outside; no skip, an unwrap.
        let run = |arg: &str| {
            Command::new("prlimit")
                .args(["--pid", &pid, "--raw", "--noheadings", "--output=SOFT", arg])
                .output()
                .unwrap()
        };
        let old = run(&format!("--{name}"));
        assert!(old.status.success() && run(&format!("--{name}={soft}:")).status.success());
        String::from_utf8(old.stdout).unwrap().trim().to_owned()
    }
    #[cfg(target_os = "linux")]
    pub fn blocked_on(&mut self, path: &Path) {
        use std::os::unix::fs::MetadataExt;
        let (pid, inode) = (
            self.id().to_string(),
            format!(":{}", path.metadata().unwrap().ino()),
        );
        loop {
            assert!(
                self.process.try_wait().unwrap().is_none(),
                "child exited before blocking"
            );
            // A blocked process cannot announce it: the kernel's own lock list says so, no time bound.
            if std::fs::read_to_string("/proc/locks")
                .unwrap()
                .lines()
                .any(|line| {
                    let f: Vec<_> = line.split_whitespace().collect();
                    f.get(1) == Some(&"->")
                        && f.get(5) == Some(&pid.as_str())
                        && f.get(6).is_some_and(|v| v.ends_with(&inode))
                })
            {
                return;
            }
            std::thread::yield_now();
        }
    }

    /// The next line the child announced, or `None` once it has ended.
    ///
    /// This is the one function in the tests that waits. It gives up after 20 seconds and fails
    /// with every line the child announced and how it ended.
    pub fn line(&mut self) -> Option<String> {
        if self.lines.is_none() {
            let pipe = self
                .process
                .stdout
                .take()
                .expect("the child's standard output");
            let (sender, receiver) = mpsc::channel();
            #[allow(
                clippy::disallowed_methods,
                reason = "the one thread in the tests: it moves lines from the child's pipe to a \
                          channel so that the wait below can give up; nothing a test asserts \
                          depends on when it runs"
            )]
            std::thread::spawn(move || {
                for line in BufReader::new(pipe).lines().map_while(Result::ok) {
                    if sender.send(line).is_err() {
                        break;
                    }
                }
            });
            self.lines = Some(receiver);
        }
        loop {
            // No time bound: the runner's own limit ends a child that truly hangs.
            match self.lines.as_ref().expect("set above").recv() {
                Ok(line) => {
                    // Everything else on the pipe is the test harness talking.
                    if let Some(said) = line.strip_prefix("@ ") {
                        self.announced.push(said.to_owned());
                        return Some(said.to_owned());
                    }
                }
                Err(_) => {
                    self.status = Some(self.process.wait().expect("wait for the child"));
                    return None;
                }
            }
        }
    }

    /// Waits for the child to announce exactly this line next.
    pub fn expect(&mut self, wanted: &str) {
        let line = self.line();
        if line.as_deref() != Some(wanted) {
            self.fail(&format!("was to announce {wanted:?} and gave {line:?}"));
        }
    }

    /// Waits for the child to announce `point`, and returns the lines it announced before it.
    pub fn until(&mut self, point: &str) -> Vec<String> {
        let mut before = Vec::new();
        loop {
            match self.line() {
                Some(line) if line == point => return before,
                Some(line) => before.push(line),
                None => self.fail(&format!("ended before it announced {point:?}")),
            }
        }
    }

    /// Writes one line to the child's standard input.
    pub fn tell(&mut self, line: &str) {
        let pipe = self
            .process
            .stdin
            .as_mut()
            .expect("the child's standard input");
        if writeln!(pipe, "{line}")
            .and_then(|()| pipe.flush())
            .is_err()
        {
            self.fail(&format!("was gone when it was told {line:?}"));
        }
    }

    /// Kills the child where it stands. It must still be running: a child that is to be killed
    /// has announced a point and is waiting there.
    ///
    /// The child's standard input stays open until the child is gone. Waiting for a process
    /// closes the input this side holds for it, and a kill has only been sent when it returns:
    /// a child that read the end of its input before the kill reached it would leave the
    /// point by itself.
    pub fn kill(&mut self) {
        let input = self.process.stdin.take();
        self.process.kill().expect("kill the child");
        let status = self.process.wait().expect("wait for the child");
        drop(input);
        if status.signal().is_none() {
            self.fail("had already ended by itself when it was to be killed");
        }
    }

    /// Closes the child's standard input and waits for it to end by itself, which it must do
    /// without a failure. Returns every line it announced.
    pub fn end(&mut self) -> Vec<String> {
        drop(self.process.stdin.take());
        while self.line().is_some() {}
        if !self.status.is_some_and(|status| status.success()) {
            self.fail("did not end well");
        }
        self.announced.clone()
    }

    fn fail(&mut self, what: &str) -> ! {
        let _ = self.process.kill();
        let status = self.process.wait();
        panic!(
            "the child {what}\nit had announced: {:#?}\nit ended with: {status:?}",
            self.announced
        )
    }
}

impl Drop for Child {
    fn drop(&mut self) {
        // A failing test must not leave a process behind that holds its store.
        let _ = self.process.kill();
        let _ = self.process.wait();
    }
}

/// The last number announced under `key`, in lines of the form `<key> <number>`.
pub fn last_number(lines: &[String], key: &str) -> Option<u64> {
    lines.iter().rev().find_map(|line| {
        let (name, number) = line.split_once(' ')?;
        if name == key {
            number.parse().ok()
        } else {
            None
        }
    })
}

// ---- a second process: the child's side ----

/// What a child was started as.
pub struct Role {
    pub name: String,
    pub folder: PathBuf,
    settings: Vec<String>,
}

impl Role {
    /// The number of a setting given as `key=number`.
    pub fn number(&self, key: &str) -> u64 {
        self.settings
            .iter()
            .find_map(|setting| setting.strip_prefix(key)?.strip_prefix('=')?.parse().ok())
            .unwrap_or_else(|| panic!("the role {} was not given {key}=<number>", self.name))
    }
}

/// The role this process was started in, or `None` in the process that runs the test itself.
pub fn role() -> Option<Role> {
    let words = std::env::var("CONTEXTDB_TEST_ROLE").ok()?;
    let mut words = words.split_whitespace().map(str::to_owned);
    Some(Role {
        name: words.next().expect("a role has a name"),
        settings: words.collect(),
        folder: std::env::var_os("CONTEXTDB_TEST_FOLDER")
            .expect("a child is given a folder")
            .into(),
    })
}

/// Announces one line to the parent.
pub fn say(line: impl fmt::Display) {
    println!("@ {line}");
}

/// Waits for the parent's next line. `None` when the parent has closed the pipe or is gone.
pub fn hear() -> Option<String> {
    let mut line = String::new();
    match io::stdin().lock().read_line(&mut line) {
        Ok(read) if read > 0 => Some(line.trim_end().to_owned()),
        _ => None,
    }
}

/// Announces a point and stays there until the parent kills this process. A kill therefore
/// lands at a point the child named, never after an amount of time.
pub fn wait_to_be_killed(point: &str) -> ! {
    say(point);
    hear();
    // Reached only when the parent went away without killing.
    std::process::exit(1)
}

/// Runs the child `role` of `test` over `folder` to its end, and returns what it announced: the
/// storage calls made when the work under test began (announced under the word `begins`, none
/// for 0), the calls in all, and every line.
pub fn learned(
    test: &str,
    role: &str,
    folder: &Path,
    begins: Option<&str>,
) -> (u64, u64, Vec<String>) {
    let lines = Child::start(test, &format!("{role} stop_at=0"), folder).end();
    let announced = |word: &str| {
        super::last_number(&lines, word).unwrap_or_else(|| panic!("the {role} announces {word}"))
    };
    (begins.map_or(0, announced), announced("calls"), lines)
}

/// For each storage call in `calls`: copies one closed store, runs the child `role` over it until
/// it stops, kills it, and gives `after` the call, the store and the lines announced before it.
pub fn for_each_stop(
    test: &str,
    role: &str,
    calls: impl IntoIterator<Item = u64>,
    source: PathBuf,
    after: impl FnMut(u64, &Path, &[String]),
) {
    assert!(for_each_trial(test, role, calls, source, true, after));
}
pub fn for_each_trial(
    test: &str,
    role: &str,
    calls: impl IntoIterator<Item = u64>,
    source: PathBuf,
    kill: bool,
    mut after: impl FnMut(u64, &Path, &[String]),
) -> bool {
    let mut passed = true;
    for k in calls {
        passed &= std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let folder = super::folder(&format!("{test}-{role}-at-{k}"));
            std::fs::copy(source.join("data"), folder.join("data")).expect("copy the closed store");
            let mut child = Child::start(test, &format!("{role} stop_at={k}"), &folder);
            let announced = if kill {
                let lines = child.until("stopped");
                child.kill();
                lines
            } else {
                child.end()
            };
            after(k, &folder, &announced);
            std::fs::remove_dir_all(&folder).expect("remove the compared trial store");
        }))
        .is_ok();
    }
    passed
}

/// Recovery's storage observer, also used for error and resize sweeps.
pub fn faulting_before_call(
    stop_at: u64,
    kill: bool,
    all: bool,
) -> (super::WrapStorage, Arc<AtomicU64>) {
    let calls = Arc::new(AtomicU64::new(0));
    let storage = super::failing_storage({
        let calls = calls.clone();
        move |call| {
            if all || matches!(call, super::Call::Write(..) | super::Call::Sync) {
                let at = calls.fetch_add(1, Ordering::SeqCst) + 1;
                if let super::Call::Resize(len) = call {
                    say(format!("growth {at} {len}"));
                }
                if at == stop_at {
                    if kill {
                        wait_to_be_killed("stopped");
                    }
                    return Err(io::Error::other("Nth storage call"));
                }
            }
            Ok(())
        }
    });
    (storage, calls)
}

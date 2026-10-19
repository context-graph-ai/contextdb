//! The inherited process ceiling is discovered from the kernel's own cgroup
//! membership and mount tables, v2 and v1, as the tightest limit along the
//! process's hierarchy. These fixtures drive the discovery with fixed tables
//! and limit files.
use super::{CeilingUnavailable, LimitRead, cgroup_limit_from, parse_limit_value};
use std::collections::HashMap;

/// Limit files by path. A missing path sets no limit at its level; the value
/// `EIO` stands for a file that exists and cannot be read.
fn limits(files: &[(&str, &str)]) -> impl FnMut(&[u8]) -> LimitRead {
    let files: HashMap<Vec<u8>, String> = files
        .iter()
        .map(|(path, value)| (path.as_bytes().to_vec(), (*value).to_string()))
        .collect();
    move |path| match files.get(path).map(String::as_str) {
        None => Ok(None),
        Some("EIO") => Err(CeilingUnavailable),
        Some(value) => parse_limit_value(value.as_bytes()),
    }
}

/// A discovery that must succeed, and what it found.
fn discover(
    membership: &[u8],
    mounts: &[u8],
    read: &mut dyn FnMut(&[u8]) -> LimitRead,
) -> Option<usize> {
    cgroup_limit_from(membership, mounts, read).expect("discovery succeeds")
}

const V2_MOUNTS: &str = "30 25 0:26 / /sys/fs/cgroup rw,nosuid,nodev,noexec,relatime shared:4 - cgroup2 cgroup2 rw,nsdelegate\n";

#[test]
fn a_v2_limit_is_the_smallest_along_the_ancestors() {
    let mut read = limits(&[
        ("/sys/fs/cgroup/user.slice/memory.max", "1073741824"),
        ("/sys/fs/cgroup/user.slice/app.scope/memory.max", "max"),
    ]);
    assert_eq!(
        discover(
            b"0::/user.slice/app.scope\n",
            V2_MOUNTS.as_bytes(),
            &mut read
        ),
        Some(1 << 30)
    );
}

#[test]
fn an_unlimited_v2_hierarchy_has_no_cgroup_limit() {
    let mut read = limits(&[("/sys/fs/cgroup/app/memory.max", "max")]);
    assert_eq!(
        discover(b"0::/app\n", V2_MOUNTS.as_bytes(), &mut read),
        None
    );
}

#[test]
fn a_v1_memory_controller_is_found_by_its_mount_and_membership() {
    // A container whose v1 memory hierarchy is mounted at its own cgroup.
    let mounts = "\
40 30 0:35 /docker/abc /sys/fs/cgroup/memory ro,nosuid master:17 - cgroup cgroup rw,memory\n\
41 30 0:36 /docker/abc /sys/fs/cgroup/cpu,cpuacct ro,nosuid master:18 - cgroup cgroup rw,cpu,cpuacct\n";
    let membership = "12:cpu,cpuacct:/docker/abc\n9:memory:/docker/abc\n0::/\n";
    let mut read = limits(&[("/sys/fs/cgroup/memory/memory.limit_in_bytes", "268435456")]);
    assert_eq!(
        discover(membership.as_bytes(), mounts.as_bytes(), &mut read),
        Some(256 << 20)
    );
}

#[test]
fn a_v1_limit_is_the_smallest_along_the_ancestors_below_a_root_mount() {
    let mounts = "40 30 0:35 / /sys/fs/cgroup/memory rw master:17 - cgroup cgroup rw,memory\n";
    let membership = "9:memory:/batch/job-7\n";
    let mut read = limits(&[
        (
            "/sys/fs/cgroup/memory/memory.limit_in_bytes",
            "9223372036854771712",
        ),
        (
            "/sys/fs/cgroup/memory/batch/memory.limit_in_bytes",
            "536870912",
        ),
        (
            "/sys/fs/cgroup/memory/batch/job-7/memory.limit_in_bytes",
            "9223372036854771712",
        ),
    ]);
    assert_eq!(
        discover(membership.as_bytes(), mounts.as_bytes(), &mut read),
        Some(512 << 20)
    );
}

#[test]
fn an_unlimited_v1_sentinel_is_bounded_by_physical_memory() {
    let mounts = "40 30 0:35 / /sys/fs/cgroup/memory rw master:17 - cgroup cgroup rw,memory\n";
    let mut read = limits(&[(
        "/sys/fs/cgroup/memory/memory.limit_in_bytes",
        "9223372036854771712",
    )]);
    let limit =
        discover(b"9:memory:/\n", mounts.as_bytes(), &mut read).expect("the sentinel is a value");
    let physical = super::physical_memory().expect("this host reports its memory");
    assert!(limit > physical, "the v1 sentinel exceeds any host");
    assert_eq!(limit.min(physical), physical, "physical memory bounds it");
}

#[test]
fn a_hybrid_host_without_a_unified_memory_limit_uses_the_v1_controller() {
    let mounts = format!(
        "{}40 30 0:35 / /sys/fs/cgroup/memory rw master:17 - cgroup cgroup rw,memory\n",
        "31 25 0:27 / /sys/fs/cgroup/unified rw shared:5 - cgroup2 cgroup2 rw\n"
    );
    let membership = "9:memory:/svc\n0::/svc\n";
    let mut read = limits(&[(
        "/sys/fs/cgroup/memory/svc/memory.limit_in_bytes",
        "134217728",
    )]);
    assert_eq!(
        discover(membership.as_bytes(), mounts.as_bytes(), &mut read),
        Some(128 << 20)
    );
}

#[test]
fn a_bare_host_has_no_cgroup_limit() {
    let mut read = limits(&[]);
    assert_eq!(discover(b"", b"", &mut read), None);
}

#[test]
fn a_limit_file_that_cannot_be_read_fails_discovery_instead_of_reading_as_no_limit() {
    // The tight limit sits on a level whose file exists and cannot be read:
    // reporting the looser remaining levels would admit past the real limit.
    let mut read = limits(&[
        ("/sys/fs/cgroup/user.slice/memory.max", "EIO"),
        ("/sys/fs/cgroup/user.slice/app.scope/memory.max", "max"),
    ]);
    assert_eq!(
        cgroup_limit_from(
            b"0::/user.slice/app.scope\n",
            V2_MOUNTS.as_bytes(),
            &mut read
        ),
        Err(CeilingUnavailable)
    );
    // An unparsable value is unreadable too, not unlimited.
    let mut read = limits(&[("/sys/fs/cgroup/app/memory.max", "garbage")]);
    assert_eq!(
        cgroup_limit_from(b"0::/app\n", V2_MOUNTS.as_bytes(), &mut read),
        Err(CeilingUnavailable)
    );
    // A v1 memory membership whose controller is not mounted cannot be read.
    let mut read = limits(&[]);
    assert_eq!(
        cgroup_limit_from(b"9:memory:/svc\n", b"", &mut read),
        Err(CeilingUnavailable)
    );
}

#[test]
fn an_interrupted_kernel_file_read_is_retried() {
    // Every one of the first few calls of this thread's reads reports EINTR,
    // as a delivered signal would; the read still returns the whole file.
    super::interrupt_file_reads_for_test(5);
    let mut contents = [0_u8; 4096];
    let read = super::read_linux_file(b"/proc/self/status\0", &mut contents)
        .expect("an interrupted read is retried, not failed");
    assert!(
        contents[..read].starts_with(b"Name:"),
        "the whole file is read"
    );
}

#[test]
fn a_kernel_file_larger_than_its_buffer_is_unreadable_not_a_prefix() {
    // /proc/self/status is far longer than 64 bytes: its prefix must not pass
    // for the whole file.
    let mut small = [0_u8; 64];
    assert_eq!(
        super::read_linux_file(b"/proc/self/status\0", &mut small),
        Err(libc::EFBIG)
    );
    let mut whole = [0_u8; 16384];
    let read = super::read_linux_file(b"/proc/self/status\0", &mut whole).expect("fits");
    assert!(whole[..read].ends_with(b"\n"), "the whole file is read");
}

#[test]
fn a_mount_table_of_any_length_is_read_whole_line_by_line() {
    let expected = std::fs::read_to_string("/proc/self/mountinfo").unwrap();
    let mut lines = Vec::new();
    super::for_each_linux_file_line(b"/proc/self/mountinfo\0", &mut |line| {
        lines.push(String::from_utf8(line.to_vec()).unwrap());
        Ok(())
    })
    .expect("mountinfo is read whole");
    assert_eq!(
        lines,
        expected.lines().map(str::to_owned).collect::<Vec<_>>()
    );
}

#[test]
fn a_cgroup_mount_line_that_does_not_fit_fails_discovery_instead_of_being_dropped() {
    let mut lines = [0_u8; 8192];
    let mut len = 8192 - 40;
    let cgroup = b"40 30 0:35 / /sys/fs/cgroup/memory rw master:17 - cgroup cgroup rw,memory";
    assert_eq!(
        super::keep_cgroup_mount_line(cgroup, &mut lines, &mut len),
        Err(CeilingUnavailable)
    );
    // A line that is not a cgroup mount is not kept, and costs nothing.
    let other = b"22 1 8:1 / / rw,relatime shared:1 - ext4 /dev/sda1 rw";
    assert_eq!(
        super::keep_cgroup_mount_line(other, &mut lines, &mut len),
        Ok(())
    );
    assert_eq!(len, 8192 - 40);
}

#[test]
fn a_failed_mount_table_read_is_not_kept() {
    let cache = super::CgroupMountCache::new();
    assert!(cache.get_or_try(|_| Err(CeilingUnavailable)).is_err());
    let (lines, len) = cache
        .get_or_try(|lines| {
            let mut len = 0;
            super::keep_cgroup_mount_line(V2_MOUNTS.trim_end().as_bytes(), lines, &mut len)?;
            Ok(len)
        })
        .expect("the next discovery reads the table again");
    assert!(lines[..len].starts_with(b"30 25 0:26"));
}

#[test]
fn a_unified_membership_without_its_mount_is_unavailable_unless_v1_supplies_the_limit() {
    // The cgroup2 mount is not in the table: no conventional path is guessed.
    let mut read = limits(&[("/sys/fs/cgroup/svc/memory.max", "max")]);
    assert_eq!(
        cgroup_limit_from(b"0::/svc\n", b"", &mut read),
        Err(CeilingUnavailable)
    );
    // A pure v1 host lists the unified line too; its v1 memory controller
    // supplies the limit.
    let mounts = "40 30 0:35 / /sys/fs/cgroup/memory rw master:17 - cgroup cgroup rw,memory\n";
    let mut read = limits(&[(
        "/sys/fs/cgroup/memory/svc/memory.limit_in_bytes",
        "134217728",
    )]);
    assert_eq!(
        cgroup_limit_from(b"9:memory:/svc\n0::/svc\n", mounts.as_bytes(), &mut read),
        Ok(Some(128 << 20))
    );
}

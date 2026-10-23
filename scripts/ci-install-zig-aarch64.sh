#!/usr/bin/env bash
# Provide the aarch64-linux-gnu C compiler/linker for the hosted cross-compile job without apt:
# a checksum-verified, version-pinned zig release acts as cc/ar. Writes env to $GITHUB_ENV.
set -euo pipefail
zig_version=0.17.0
zig_sha256=1cbe9df9f27e6b78d14ccbca43b6703a404ef79ef1c463de901d7f088d4e2026
root=${1:?usage: ci-install-zig-aarch64.sh <empty-install-dir>}
mkdir -p "$root"
curl --fail --silent --show-error --location --retry 3 \
  --output "$root/zig.tar.xz" "https://ziglang.org/download/$zig_version/zig-x86_64-linux-$zig_version.tar.xz"
printf '%s  %s\n' "$zig_sha256" "$root/zig.tar.xz" | sha256sum --check --strict -
tar -xJf "$root/zig.tar.xz" -C "$root"
zig="$root/zig-x86_64-linux-$zig_version/zig"
"$zig" version | grep -Fx "$zig_version"
# cc-rs passes --target=<rust triple>, which zig cannot parse; zig is already given -target.
cat > "$root/aarch64-cc" <<WRAP
#!/bin/sh
for a; do shift; case "\$a" in --target=*) ;; *) set -- "\$@" "\$a" ;; esac; done
exec "$zig" cc -target aarch64-linux-gnu "\$@"
WRAP
printf '#!/bin/sh\nexec "%s" ar "$@"\n' "$zig" > "$root/aarch64-ar"
chmod +x "$root/aarch64-cc" "$root/aarch64-ar"
if [ -n "${GITHUB_ENV:-}" ]; then
  {
    echo "CARGO_TARGET_AARCH64_UNKNOWN_LINUX_GNU_LINKER=$root/aarch64-cc"
    echo "CC_aarch64_unknown_linux_gnu=$root/aarch64-cc"
    echo "AR_aarch64_unknown_linux_gnu=$root/aarch64-ar"
  } >> "$GITHUB_ENV"
fi

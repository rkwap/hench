#!/usr/bin/env bash
# Builds the README's Rust examples in a crate of their own whose dependencies are exactly the
# ones the README's Quick start lists (its first ```toml block), with this checkout's `hench`
# (and the `cli` feature plus `clap`, which the configuration section asks for) and
# `hench-web`.
#
# The README's examples also run as doc tests of `hench-server`, but those see every
# dependency of that crate (and the features the workspace enables), so they cannot tell
# whether a reader who copies the Quick start's list can build them.
#
# Usage: ci/check-readme-examples.sh [--offline]
# Honours CARGO_TARGET_DIR (sharing it with the workspace saves rebuilding dependencies).
set -euo pipefail

root=$(cd "$(dirname "$0")/.." && pwd)
readme="$root/README.md"
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT
mkdir -p "$work/src" "$work/examples"

# The first ```toml block of the README: the dependency list of the Quick start.
dependencies=$(awk '/^```toml$/ { inside = 1; next } inside && /^```$/ { exit } inside { print }' "$readme")
if ! grep -q '^hench = ' <<<"$dependencies" || ! grep -q '^hench-web = ' <<<"$dependencies"; then
  echo "the README's first toml block does not list hench and hench-web" >&2
  exit 1
fi
# This checkout's crates instead of the published ones.
dependencies=$(sed \
  -e "s|^hench = .*|hench = { path = \"$root/crates/hench\", features = [\"cli\"] }|" \
  -e "s|^hench-web = .*|hench-web = { path = \"$root/crates/hench-web\" }|" \
  <<<"$dependencies")

cat >"$work/Cargo.toml" <<TOML
[package]
name = "readme-examples"
version = "0.0.0"
edition = "2024"
publish = false

[workspace]

$dependencies
# The configuration section's addition.
clap = "4"
TOML
echo 'fn main() {}' >"$work/src/main.rs"
# The versions the workspace is tested with.
cp "$root/Cargo.lock" "$work/Cargo.lock"

# Every ```rust block (whatever its attributes) becomes an example.
awk -v dir="$work/examples" '
  /^```rust/ { count++; inside = 1; file = sprintf("%s/readme_%02d.rs", dir, count); next }
  inside && /^```$/ { inside = 0; next }
  inside { print > file }
' "$readme"
examples=$(find "$work/examples" -name '*.rs' | wc -l)
if [ "$examples" -eq 0 ]; then
  echo "no Rust example found in the README" >&2
  exit 1
fi

echo "building $examples README examples with the README's dependencies"
cargo check --manifest-path "$work/Cargo.toml" --examples --keep-going "$@"

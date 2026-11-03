#!/usr/bin/env bash
# Builds the Rust examples of the README and of the guide (docs/guide) the way a reader who
# copies them would: in a crate of their own whose dependencies are exactly the ones the
# document lists (its first ```toml block: the README's Quick start, the guide's Getting
# started), with this checkout's `hench` (and the `cli` feature plus `clap`, which the
# configuration page asks for) and `hench-web`.
#
# The examples also run as doc tests of `hench-server`, but those see every dependency of that
# crate (and the features the workspace enables), so they cannot tell whether a reader who
# copies a document's list can build them.
#
# Usage: ci/check-readme-examples.sh [--offline]
# Honours CARGO_TARGET_DIR (sharing it with the workspace saves rebuilding dependencies; the
# two example crates share one either way).
set -euo pipefail

root=$(cd "$(dirname "$0")/.." && pwd)
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT
export CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-$work/target}"

# check <name> <document listing the dependencies> <documents...>: builds every ```rust block
# of the documents (whatever its attributes) as an example of a crate named <name>.
check() {
  local name=$1 listing=$2
  shift 2
  local crate="$work/$name"
  mkdir -p "$crate/src" "$crate/examples"

  # The first ```toml block of the listing document.
  local dependencies
  dependencies=$(awk '/^```toml$/ { inside = 1; next } inside && /^```$/ { exit } inside { print }' "$listing")
  if ! grep -q '^hench = ' <<<"$dependencies"; then
    echo "the first toml block of $listing does not list hench" >&2
    exit 1
  fi
  # This checkout's crates instead of the published ones.
  dependencies=$(sed \
    -e "s|^hench = .*|hench = { path = \"$root/crates/hench\", features = [\"cli\"] }|" \
    -e "s|^hench-web = .*|hench-web = { path = \"$root/crates/hench-web\" }|" \
    <<<"$dependencies")

  cat >"$crate/Cargo.toml" <<TOML
[package]
name = "$name"
version = "0.0.0"
edition = "2024"
publish = false

[workspace]

$dependencies
# The configuration page's addition.
clap = "4"
TOML
  echo 'fn main() {}' >"$crate/src/main.rs"
  # The versions the workspace is tested with.
  cp "$root/Cargo.lock" "$crate/Cargo.lock"

  local document count=0
  for document in "$@"; do
    count=$(awk -v dir="$crate/examples" -v base="$(basename "$document" .md | tr '-' '_')" -v count="$count" '
      /^```rust/ { count++; inside = 1; file = sprintf("%s/%s_%02d.rs", dir, base, count); next }
      inside && /^```$/ { inside = 0; next }
      inside { print > file }
      END { print count }
    ' "$document")
  done
  if [ "$count" -eq 0 ]; then
    echo "no Rust example found in $*" >&2
    exit 1
  fi

  echo "building $count examples of $name with the dependencies $(basename "$listing") lists"
  cargo check --manifest-path "$crate/Cargo.toml" --examples --keep-going "${cargo_args[@]+"${cargo_args[@]}"}"
}

cargo_args=("$@")
check readme-examples "$root/README.md" "$root/README.md"
check guide-examples "$root/docs/guide/getting-started.md" "$root"/docs/guide/*.md

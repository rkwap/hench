#!/usr/bin/env bash
# Packages the workspace's crates as `cargo publish` does (and verifies each one builds from
# its package), then builds `hench` from its `.crate` file alone, as distributions and crater
# do: every test target and example must compile from the package with the default features,
# with all of them and without the default ones, and the unit tests must pass. Every target of
# the packaged `hench-web` and `hench-server` must compile too, and `hench-server`'s doc tests
# (the README's examples, which it includes from its package) must pass.
#
# In the workspace, hench's self dev-dependency turns on `test-util` and the repository's other
# files are at hand; the published crate has neither. So the tests that start a
# `redis-server` declare `required-features = ["test-util"]`, and the checks of the repository
# itself (`tests/spec_parity.rs`, `tests/readme.rs`) are left out of the package.
#
# Usage: ci/check-package.sh [--offline]
# Honours CARGO_TARGET_DIR (sharing it with the workspace saves rebuilding dependencies). The
# unit tests start `redis-server`.
set -euo pipefail

root=$(cd "$(dirname "$0")/.." && pwd)
target=${CARGO_TARGET_DIR:-$root/target}
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT

version=$(sed -n 's/^version = "\(.*\)"$/\1/p' "$root/Cargo.toml" | head -n 1)
if [ -z "$version" ]; then
  echo "no [workspace.package] version in $root/Cargo.toml" >&2
  exit 1
fi

echo "packaging the workspace (version $version)"
(cd "$root" && cargo package --workspace --allow-dirty "$@")

for crate in hench hench-macros hench-web hench-server; do
  tar -xzf "$target/package/$crate-$version.crate" -C "$work"
done
hench="$work/hench-$version"
# hench-macros of the same release, not yet published: from its package too.
patch="patch.crates-io.hench-macros.path=\"$work/hench-macros-$version\""
# The same for every crate of the release that hench-web and hench-server depend on.
patches=(--config "$patch"
  --config "patch.crates-io.hench.path=\"$hench\""
  --config "patch.crates-io.hench-web.path=\"$work/hench-web-$version\"")

listed=$(tar -tzf "$target/package/hench-$version.crate")
for left_out in tests/spec_parity.rs tests/readme.rs; do
  if grep -q "/$left_out\$" <<<"$listed"; then
    echo "$left_out is in the package: it reads files of the repository" >&2
    exit 1
  fi
done

cd "$hench"
echo "building hench's tests and examples from its package"
cargo check --config "$patch" --tests --examples --keep-going "$@"
cargo check --config "$patch" --all-targets --all-features --keep-going "$@"
cargo check --config "$patch" --lib --examples --no-default-features --keep-going "$@"
echo "running hench's unit tests from its package"
cargo test --config "$patch" --all-features --lib "$@"

for crate in hench-web hench-server; do
  echo "building $crate's targets from its package"
  (cd "$work/$crate-$version" && cargo check "${patches[@]}" --all-targets --keep-going "$@")
done
# The README's examples are doc tests of hench-server, included from the README its package
# carries (a path out of the package would only fail here).
echo "running hench-server's doc tests (the README's examples) from its package"
(cd "$work/hench-server-$version" && cargo test "${patches[@]}" --doc "$@")

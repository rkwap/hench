#!/usr/bin/env bash
# Prepares a fresh Ubuntu machine (x86_64 or arm64) for benchmarks/run.sh: build tools, Rust
# (rustup, stable), Redis (the official packages.redis.io repository, else Ubuntu's), toxiproxy,
# and Ruby with YJIT (built by ruby-build, which enables YJIT when rustc is present). The
# system Redis service is stopped: run.sh starts a private redis-server for every run. The CPU
# idle states that take more than 20 us to leave are disabled until the next boot
# (benchmarks/README.md, Method): on EC2 instances that expose C-states, such as c7i.4xlarge,
# C6 takes 190 us to leave, and every thread woken on a CPU sleeping in it waits that long.
#
# Usage: benchmarks/setup-ubuntu.sh   (as a user with sudo; then open a new shell, or
#        `source ~/.cargo/env` and put ~/.rubies/<version>/bin in PATH, as it prints)
set -euo pipefail

ruby_version=${RUBY_VERSION:-4.0.9}
toxiproxy_version=${TOXIPROXY_VERSION:-2.13.0}
arch=$(dpkg --print-architecture)

echo "== packages"
sudo apt-get update -q
sudo apt-get install -y -q build-essential pkg-config git curl ca-certificates gnupg lsb-release \
  autoconf libssl-dev libyaml-dev zlib1g-dev libffi-dev libgmp-dev libreadline-dev

echo "== rust"
if ! command -v cargo >/dev/null && [ ! -x "$HOME/.cargo/bin/cargo" ]; then
  curl -fsSL https://sh.rustup.rs | sh -s -- -y --profile minimal
fi
# shellcheck disable=SC1091
. "$HOME/.cargo/env"
rustc --version

echo "== redis"
if ! command -v redis-server >/dev/null; then
  curl -fsSL https://packages.redis.io/gpg | sudo gpg --dearmor --yes \
    -o /usr/share/keyrings/redis-archive-keyring.gpg
  echo "deb [signed-by=/usr/share/keyrings/redis-archive-keyring.gpg] https://packages.redis.io/deb $(lsb_release -cs) main" \
    | sudo tee /etc/apt/sources.list.d/redis.list >/dev/null
  # The repository may not know a brand-new Ubuntu release yet: Ubuntu's own package then.
  if ! sudo apt-get update -q || ! sudo apt-get install -y -q redis; then
    sudo rm -f /etc/apt/sources.list.d/redis.list
    sudo apt-get update -q
    sudo apt-get install -y -q redis-server
  fi
fi
sudo systemctl disable --now redis-server >/dev/null 2>&1 || true
redis-server --version

echo "== toxiproxy"
if ! command -v toxiproxy-server >/dev/null; then
  deb="toxiproxy_${toxiproxy_version}_linux_${arch}.deb"
  curl -fsSLo "/tmp/$deb" \
    "https://github.com/Shopify/toxiproxy/releases/download/v${toxiproxy_version}/$deb"
  sudo apt-get install -y -q "/tmp/$deb"
fi
toxiproxy-server --version

echo "== ruby $ruby_version (YJIT)"
rubies="$HOME/.rubies"
if [ ! -x "$rubies/$ruby_version/bin/ruby" ]; then
  if [ ! -d "$HOME/.ruby-build" ]; then
    git clone --depth 1 https://github.com/rbenv/ruby-build.git "$HOME/.ruby-build"
  fi
  MAKE_OPTS="-j$(nproc)" "$HOME/.ruby-build/bin/ruby-build" "$ruby_version" "$rubies/$ruby_version"
fi
RUBY_YJIT_ENABLE=1 "$rubies/$ruby_version/bin/ruby" -e \
  'puts RUBY_DESCRIPTION; abort "no YJIT" unless defined?(RubyVM::YJIT) && RubyVM::YJIT.enabled?'

echo "== cpu idle states"
for state in /sys/devices/system/cpu/cpu[0-9]*/cpuidle/state*; do
  [ -r "$state/latency" ] || continue
  if [ "$(cat "$state/latency")" -gt 20 ]; then echo 1 | sudo tee "$state/disable" >/dev/null; fi
done
for state in /sys/devices/system/cpu/cpu0/cpuidle/state*; do
  [ -r "$state/latency" ] || continue
  echo "$(cat "$state/name"): $(cat "$state/latency") us to leave$([ "$(cat "$state/disable")" = 1 ] \
    && echo ', disabled until the next boot')"
done

echo
echo "Ready. Run the benchmarks with:"
echo "  source ~/.cargo/env"
echo "  RUBY=$rubies/$ruby_version/bin/ruby benchmarks/run.sh"

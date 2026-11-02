# Hench — agent notes

Hench is a Rust (tokio + Redis) background job system modelled on Sidekiq 8.1.

- `docs/DESIGN.md` is the binding contract (data model, public API, semantics). Read it before changing code; update it when a decision changes.
- `docs/STATUS.md` tracks progress; update it in the same commit as the work.
- Reference sources (read-only, outside the repo): Sidekiq `/home/user/ref/sidekiq`, rusty-sidekiq `/home/user/ref/sidekiq-rs`, Faktory `/home/user/ref/faktory`. If missing, clone github.com/sidekiq/sidekiq, github.com/film42/sidekiq-rs, github.com/contribsys/faktory.
- Sidekiq is LGPL-3.0: learn behaviour from it, write original code. Never copy its source, comments, Lua, templates, CSS, JS or locale strings. Port test scenarios, not test code.
- Dependency APIs: grep the exact versions under `~/.cargo/registry/src/*/` instead of guessing.
- redis-rs 1.8 multiplexed connections time out responses after 500 ms by default: blocking commands (BRPOP/BLMOVE) must use dedicated connections with a longer response timeout.
- Integration tests use `hench::test_util::TestRedis` (feature `test-util`), which spawns a private `redis-server` per test; never point tests at a shared Redis.
- Before committing: `cargo fmt --all`, `cargo clippy --workspace --all-targets --all-features -- -D warnings`, `cargo test --workspace --all-features`.
- Library code: no `unwrap()`/`expect()` on fallible runtime paths, doc comments on public items, `tracing` for logs.

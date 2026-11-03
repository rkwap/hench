# Contributing to Hench

Thank you for helping. Bug reports, documentation fixes, tests and code are all welcome. This
page explains how to propose a change and what a pull request needs to be merged.

## Before you start

- **Bugs.** Open an issue with the versions of Hench, Rust and Redis (or Valkey), what you
  expected, what happened, and the smallest program or steps that show it.
- **Small changes** (a bug fix, a test, a documentation fix): open a pull request directly.
- **Larger changes** (a new feature, a change in behaviour, anything that touches how jobs are
  stored in Redis): open an issue first, so we can agree on the approach before you write the
  code. [`docs/DESIGN.md`](docs/DESIGN.md) is the contract the code follows (the Redis data
  model, the semantics, and every deviation from Sidekiq); a change to it is discussed first.
- **Security issues:** do not open a public issue; see [`SECURITY.md`](SECURITY.md).

## The clean-room rule

Hench follows Sidekiq's design and keeps its Redis data model, but it is an independent
implementation. Sidekiq is licensed under the LGPL-3.0, and Hench under the MIT license, so:

- Learn from Sidekiq's behaviour, documentation and tests, but **never copy its code**: no
  Ruby, Lua scripts, templates, CSS, JavaScript or locale strings, not even translated to Rust.
- **Port test scenarios, not test code**: describe the case in your own words and write the
  test from scratch.
- **Never use code from Sidekiq Pro or Sidekiq Enterprise.** They are commercial and closed;
  their public documentation is the only source to learn from.
- The same applies to every other project Hench learns from (BullMQ, Oban, Faktory, the Sidekiq
  add-ons): ideas and behaviour are welcome, copied code is not.

Code written with an AI assistant counts as yours: you are responsible for it following these
rules.

## Setting up

You need:

- **Rust**: the latest stable release. Hench supports Rust 1.88 and newer, so avoid language
  and library features newer than that.
- **`redis-server`** (Redis 6.2 or newer, or Valkey) on your `PATH`, or its path in
  `HENCH_REDIS_SERVER`. The tests start a private server for each test on a temporary socket
  and never touch a Redis you are running.
- **`openssl`**, for the TLS tests (`--features tls`).
- Optionally, **Node.js with the `playwright` package and its Chromium**, for the dashboard's
  browser tests; without them those tests are skipped with a note.

The macro diagnostics tests (`crates/hench/tests/macros.rs`) compare compiler error messages,
which change between Rust versions, so they only run on the version recorded in
`crates/hench/tests/ui/typeck/rustc-version.txt` and are skipped on others.

## Before you open a pull request

Run these from the repository root; each must pass:

```bash
cargo fmt --all
```

```bash
cargo clippy --workspace --all-targets --all-features -- -D warnings
```

```bash
cargo test --workspace --all-features
```

Then check that your change:

- **has tests**: a bug fix comes with a test that fails without it;
- **follows the code conventions**: no `unwrap()` or `expect()` on fallible runtime paths, a
  doc comment on every public item, and `tracing` for logs;
- **updates the documentation** it affects: the rustdoc, the [guide](docs/guide), and
  [`docs/DESIGN.md`](docs/DESIGN.md) when behaviour or the Redis data changes;
- **adds an entry to [`CHANGELOG.md`](CHANGELOG.md)** under `Unreleased` when users will notice
  it;
- **keeps Hench fast**: changes to fetching, processing or the Redis connections come with a
  before-and-after run of the benchmarks ([`benchmarks/README.md`](benchmarks/README.md) has
  the commands).

## Pull requests

- Keep each pull request to one topic, and say what it changes and why.
- Write the title as `area: what changed` (for example `redis: reconnect after a failover`).
  Pull requests are squash-merged, so the title becomes the commit message on `main`.
- GitHub Actions checks each pull request: formatting, clippy, the documentation, the minimum
  Rust version, and the whole test suite with its coverage. Run the commands above first all
  the same: they are quicker on your machine than a round trip through CI.

The maintainers update [`docs/STATUS.md`](docs/STATUS.md) and publish releases
([`docs/RELEASING.md`](docs/RELEASING.md)); you do not need to touch either.

## License

Hench is licensed under the [MIT license](LICENSE). Unless you say otherwise, any contribution
you submit for inclusion in Hench is licensed under the same license, without any additional
terms or conditions.

## Code of conduct

Everyone taking part in Hench's issues, pull requests and discussions is expected to follow the
[code of conduct](CODE_OF_CONDUCT.md).

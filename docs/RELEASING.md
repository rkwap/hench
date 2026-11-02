# Releasing hench

The four crates share one version (`[workspace.package] version`) and are published together.
What crates.io and docs.rs show of a version (its README, its metadata, its documentation) is
frozen once it is published, so every step before `cargo publish` matters.

## Before the first release (0.1.0)

These are settings of the GitHub repository, which no commit can make:

1. Make `github.com/rkwap/hench` public: crates.io, docs.rs, the README and the documentation
   link to it (the crates' `repository`, the crate table and the examples of the README, and
   `docs/DESIGN.md` and `docs/design/*.md` from the rustdoc).
2. Make the released code the default branch's: fast-forward `main` to the release commit
   (`git push origin <commit>:main`; `main` holds only the initial commit, an ancestor of the
   release branch) or make the release branch the default one, so that visitors of the
   repository find the code.

## Every release

1. Bump `[workspace.package] version` and the versions of the internal dependencies in
   `[workspace.dependencies]`: `hench-macros` is required with `=` (the expansion of
   `#[hench::job]` uses `hench::__private`, so a `hench` release works with its own
   `hench-macros` only), `hench` and `hench-web` with the version whose API `hench-web` and
   `hench-server` use.
2. Replace `v<previous>` by `v<new>` in the links to the repository from `README.md` and the
   rustdoc (`grep -rn -E '(blob|tree)/v[0-9]' README.md crates`): they point at the tagged
   sources, so that the README and the documentation of a version keep linking to the files of
   that version (crates.io resolves a README's relative links against each crate's directory,
   and docs.rs not at all, so the links are absolute).
3. Pass the gate: `cargo fmt --all --check`, `cargo clippy --workspace --all-targets
   --all-features -- -D warnings`, `cargo test --workspace --all-features`, `RUSTDOCFLAGS="-D
   warnings" cargo doc --workspace --no-deps --all-features`, `cargo +1.88 check --workspace
   --all-features --all-targets`, `cargo deny --all-features check` (cargo-deny takes
   `--all-features` before the subcommand), `ci/check-readme-examples.sh`,
   `ci/check-package.sh` (packages the crates as `cargo publish` does, builds every target of
   the packaged `hench`, `hench-web` and `hench-server` from their `.crate` files, as
   distributions and crater do, and runs the README's doc tests from the packaged
   `hench-server`), `ci/soak.sh --short` and, for a change to the fetch or the processor,
   `ci/throughput.sh --base v<previous>`. Update `CHANGELOG.md`: move `[Unreleased]` under the
   new version with its date, and say what the next one plans.
4. Tag the release commit `v<new>` and push the tag: the links of step 2 resolve from then on.
5. Publish in dependency order, each once the previous one is visible on crates.io:
   `hench-macros`, `hench`, `hench-web`, `hench-server` (or `cargo publish --workspace`, which
   orders them). Publish from a checkout with symbolic links (Linux or macOS):
   `crates/hench-server/README.md` links to the repository's README, which `cargo package`
   stores in the package as a file.

## Compatibility promises

- `hench`, `hench-macros` and `hench-web` follow semantic versioning: before 1.0, a minor
  version (0.x) may break the API, a patch version (0.x.y) does not. Types that are expected to
  grow (`cli::WorkerArgs`, the API's results, the metrics' results and enums, `Exhausted`) are
  `#[non_exhaustive]`; items marked `#[doc(hidden)]` (`__private`, `random_hex`) are not part
  of the API.
- `hench-server` is published for its binary, `hench`; its library exists for that binary and
  its tests, and its API is not covered by these promises (its documentation says so). The
  command line and the JSON API of `hench` are.
- The data hench keeps in Redis is described in `docs/DESIGN.md` (§2 and the feature
  sections). A batch records the format it was written in, and a version that finds a newer
  one refuses it (`BatchError::Unsupported`) rather than misread it.

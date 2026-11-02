//! Checks of the README's Rust examples that compiling them cannot make: the examples run
//! against Redis (they are `no_run` doc tests of `hench-server`), so mistakes that only show
//! when a worker starts would otherwise reach readers.

use std::collections::BTreeSet;

/// The README, read from the repository (two levels above this crate).
fn readme() -> String {
    let path = concat!(env!("CARGO_MANIFEST_DIR"), "/../../README.md");
    std::fs::read_to_string(path).unwrap_or_else(|error| panic!("{path}: {error}"))
}

/// The README's ```rust blocks (whatever their attributes), with the line each starts at.
fn rust_blocks(readme: &str) -> Vec<(usize, String)> {
    let mut blocks = Vec::new();
    let mut current: Option<(usize, String)> = None;
    for (index, line) in readme.lines().enumerate() {
        match &mut current {
            None if line.starts_with("```rust") => current = Some((index + 1, String::new())),
            None => {}
            Some(_) if line == "```" => blocks.extend(current.take()),
            Some((_, block)) => {
                block.push_str(line);
                block.push('\n');
            }
        }
    }
    blocks
}

/// The functions of `block` that `#[hench::job]` defines.
fn macro_jobs(block: &str) -> BTreeSet<String> {
    let mut jobs = BTreeSet::new();
    let mut in_attribute = false;
    for line in block.lines() {
        let line = line.trim_start();
        if line.starts_with("#[hench::job") {
            in_attribute = true;
        }
        if in_attribute && let Some(rest) = line.split("fn ").nth(1) {
            let name: String = rest
                .chars()
                .take_while(|c| c.is_alphanumeric() || *c == '_')
                .collect();
            if !name.is_empty() {
                jobs.insert(name);
                in_attribute = false;
            }
        }
    }
    jobs
}

/// `#[hench::job]` registers its job by itself, and `HenchBuilder::build` refuses a second
/// registration: an example that also calls `.register(job)` fails at start-up ("registered
/// twice"). Round 3 (docs-usability-1): four examples did.
#[test]
fn examples_do_not_register_macro_jobs_twice() {
    let readme = readme();
    let blocks = rust_blocks(&readme);
    assert!(blocks.len() > 10, "the README's examples were not found");
    let mut twice = Vec::new();
    for (line, block) in &blocks {
        for job in macro_jobs(block) {
            let registered = [format!(".register({job})"), format!(".register({job},")];
            if registered.iter().any(|call| block.contains(call.as_str())) {
                twice.push(format!("`{job}` in the example at line {line}"));
            }
        }
    }
    assert!(
        twice.is_empty(),
        "README examples register `#[hench::job]` jobs, which register themselves: {}",
        twice.join(", ")
    );
}

/// The JSON API answers under `<prefix>/api/v1`; the dashboard's own paths serve HTML. Round 3
/// (docs-usability-17): the feature sections cited `GET /locks` and the like.
#[test]
fn json_api_paths_carry_their_prefix() {
    // The prose with its line breaks and indentation folded into single spaces.
    let prose = readme().split_whitespace().collect::<Vec<_>>().join(" ");
    let mut bare = Vec::new();
    for (position, _) in prose.match_indices("JSON API (`") {
        let cited: String = prose[position..].chars().take(80).collect();
        if !cited.contains("/api/v1") {
            bare.push(cited);
        }
    }
    assert!(
        bare.is_empty(),
        "JSON API paths without `<prefix>/api/v1`: {bare:#?}"
    );
}

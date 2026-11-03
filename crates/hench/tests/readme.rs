//! Checks of the README's and the guide's Rust examples that compiling them cannot make: the
//! examples run against Redis (they are `no_run` doc tests of `hench-server`), so mistakes that
//! only show when a worker starts would otherwise reach readers. And checks of the links between
//! the documents, which moving a section breaks without a sound.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

/// The repository (two levels above this crate).
fn repository() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../..")
}

fn read(path: &Path) -> String {
    std::fs::read_to_string(path).unwrap_or_else(|error| panic!("{}: {error}", path.display()))
}

/// The pages of the guide (`docs/guide`), in name order.
fn guide_pages() -> Vec<PathBuf> {
    let directory = repository().join("docs/guide");
    let mut pages: Vec<PathBuf> = std::fs::read_dir(&directory)
        .unwrap_or_else(|error| panic!("{}: {error}", directory.display()))
        .map(|entry| entry.expect("a directory entry").path())
        .filter(|path| path.extension().is_some_and(|extension| extension == "md"))
        .collect();
    pages.sort();
    pages
}

/// `path` relative to the repository, for messages.
fn name(path: &Path) -> String {
    let root = repository();
    path.strip_prefix(&root)
        .unwrap_or(path)
        .display()
        .to_string()
}

/// The documents whose examples readers copy: the README, then the guide.
fn documents() -> Vec<(String, String)> {
    std::iter::once(repository().join("README.md"))
        .chain(guide_pages())
        .map(|path| (name(&path), read(&path)))
        .collect()
}

/// The ```rust blocks of `text` (whatever their attributes), with the line each starts at.
fn rust_blocks(text: &str) -> Vec<(usize, String)> {
    let mut blocks = Vec::new();
    let mut current: Option<(usize, String)> = None;
    for (index, line) in text.lines().enumerate() {
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
    let mut examples = 0;
    let mut twice = Vec::new();
    for (document, text) in documents() {
        for (line, block) in rust_blocks(&text) {
            examples += 1;
            for job in macro_jobs(&block) {
                let registered = [format!(".register({job})"), format!(".register({job},")];
                if registered.iter().any(|call| block.contains(call.as_str())) {
                    twice.push(format!("`{job}` in the example at {document}:{line}"));
                }
            }
        }
    }
    assert!(examples > 10, "the examples were not found");
    assert!(
        twice.is_empty(),
        "examples register `#[hench::job]` jobs, which register themselves: {}",
        twice.join(", ")
    );
}

/// The JSON API answers under `<prefix>/api/v1`; the dashboard's own paths serve HTML. Round 3
/// (docs-usability-17): the feature sections cited `GET /locks` and the like.
#[test]
fn json_api_paths_carry_their_prefix() {
    let mut bare = Vec::new();
    for (document, text) in documents() {
        // The prose with its line breaks and indentation folded into single spaces.
        let prose = text.split_whitespace().collect::<Vec<_>>().join(" ");
        for (position, _) in prose.match_indices("JSON API (`") {
            let cited: String = prose[position..].chars().take(80).collect();
            if !cited.contains("/api/v1") {
                bare.push(format!("{document}: {cited}"));
            }
        }
    }
    assert!(
        bare.is_empty(),
        "JSON API paths without `<prefix>/api/v1`: {bare:#?}"
    );
}

/// The guide's examples are doc tests of `hench-server`, one item per page of its `pages!`
/// list: a page missing from the list would have examples nothing compiles.
#[test]
fn guide_pages_are_doc_tests() {
    let lib = read(&repository().join("crates/hench-server/src/lib.rs"));
    let missing: Vec<String> = guide_pages()
        .iter()
        .filter(|page| !rust_blocks(&read(page)).is_empty())
        .filter_map(|page| page.file_name())
        .map(|file| file.to_string_lossy().into_owned())
        .filter(|file| !lib.contains(&format!("=> \"{file}\"")))
        .collect();
    assert!(
        missing.is_empty(),
        "guide pages missing from hench-server's doc tests: {missing:?}"
    );
}

/// The anchor GitHub gives a heading: lowercase, spaces to hyphens, other punctuation dropped.
fn anchor(heading: &str) -> String {
    heading
        .trim()
        .to_lowercase()
        .chars()
        .filter_map(|c| match c {
            ' ' => Some('-'),
            c if c.is_alphanumeric() || c == '-' || c == '_' => Some(c),
            _ => None,
        })
        .collect()
}

/// The anchors of `text`'s headings, and the targets of its links (inline and reference
/// definitions), outside code blocks.
fn anchors_and_links(text: &str) -> (BTreeSet<String>, Vec<String>) {
    let mut anchors = BTreeSet::new();
    let mut links = Vec::new();
    let mut in_code = false;
    for line in text.lines() {
        if line.starts_with("```") {
            in_code = !in_code;
            continue;
        }
        if in_code {
            continue;
        }
        if let Some(heading) = line.trim_start_matches('#').strip_prefix(' ')
            && line.starts_with('#')
        {
            anchors.insert(anchor(heading));
        }
        if line.starts_with('[')
            && let Some((_, target)) = line.split_once("]: ")
        {
            links.push(target.trim().to_owned());
        }
        for (position, _) in line.match_indices("](") {
            let rest = &line[position + 2..];
            if let Some(end) = rest.find(')') {
                links.push(rest[..end].to_owned());
            }
        }
    }
    (anchors, links)
}

/// The README is also the crates' page on crates.io, which resolves relative links against each
/// crate's directory: it links with absolute URLs, or to its own headings. The other documents'
/// relative links name files that exist, and headings those files have.
#[test]
fn links_resolve() {
    let root = repository();
    let mut broken = Vec::new();

    let readme = read(&root.join("README.md"));
    let (anchors, links) = anchors_and_links(&readme);
    for link in links {
        let ok = match link.strip_prefix('#') {
            Some(fragment) => anchors.contains(fragment),
            None => link.starts_with("https://") || link.starts_with("http://"),
        };
        if !ok {
            broken.push(format!("README.md: {link}"));
        }
    }

    let mut documents: Vec<PathBuf> = guide_pages();
    documents.extend(
        ["docs/ROADMAP.md", "benchmarks/README.md", "CHANGELOG.md"].map(|path| root.join(path)),
    );
    for document in documents.iter().filter(|path| path.exists()) {
        let (anchors, links) = anchors_and_links(&read(document));
        let directory = document.parent().unwrap_or(&root);
        for link in links {
            if link.contains("://") || link.starts_with("mailto:") {
                continue;
            }
            let (path, fragment) = link.split_once('#').unwrap_or((&link, ""));
            let ok = if path.is_empty() {
                anchors.contains(fragment)
            } else {
                let target = directory.join(path);
                target.exists()
                    && (fragment.is_empty()
                        || target.extension().is_none_or(|extension| extension != "md")
                        || anchors_and_links(&read(&target)).0.contains(fragment))
            };
            if !ok {
                broken.push(format!("{}: {link}", name(document)));
            }
        }
    }
    assert!(broken.is_empty(), "links that do not resolve: {broken:#?}");
}

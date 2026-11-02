//! Canonical JSON and SHA-256 digests: the one way hench turns a JSON value into a stable
//! identifier, shared by every feature that hashes something (`docs/design/pro-features.md`
//! §2.6):
//!
//! * unique-job digests (`hench::unique::digest`: the canonical JSON of
//!   `["hench-unique/1", class, queue or null, key]`),
//! * periodic definition and crontab digests,
//! * limiter keys longer than 128 bytes (`#` followed by the first 22 characters of the digest).
//!
//! The canonical form does not depend on how the value was built: object members are sorted by
//! the UTF-8 bytes of their keys at every level, whatever the iteration order of
//! `serde_json::Map` (another crate in the build can enable `serde_json`'s `preserve_order`
//! feature through feature unification, which would otherwise make two builds of one program
//! disagree). Everything else is written exactly as `serde_json` writes it: no whitespace,
//! numbers as their `serde_json::Number` text (so `1` and `1.0` differ), strings with
//! `serde_json`'s escaping, arrays in order.

use base64::Engine as _;
use serde_json::Value;
use sha2::{Digest as _, Sha256};

use crate::error::{Error, Result};

/// Nesting depth (arrays and objects) a canonical value may have; deeper values are refused.
pub(crate) const MAX_DEPTH: usize = 128;

/// Characters of a [`sha256_b64url`] digest.
#[cfg(test)]
pub(crate) const DIGEST_CHARS: usize = 43;

/// The canonical JSON text of `value` (see the [module documentation](self)): compact, with
/// object members sorted by key at every level. Fails with [`Error::InvalidJob`] for values
/// nested more than [`MAX_DEPTH`] levels deep.
pub(crate) fn canonical_json(value: &Value) -> Result<Vec<u8>> {
    let mut out = Vec::with_capacity(64);
    write_value(&mut out, value, 0)?;
    Ok(out)
}

/// The SHA-256 of `bytes` in base64url without padding: 43 characters of `[A-Za-z0-9_-]`, safe
/// in Redis keys, URLs and form fields.
pub(crate) fn sha256_b64url(bytes: &[u8]) -> String {
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(Sha256::digest(bytes))
}

/// [`sha256_b64url`] of the [`canonical_json`] of `value`.
pub(crate) fn digest(value: &Value) -> Result<String> {
    Ok(sha256_b64url(&canonical_json(value)?))
}

fn write_value(out: &mut Vec<u8>, value: &Value, depth: usize) -> Result<()> {
    match value {
        Value::Null => out.extend_from_slice(b"null"),
        Value::Bool(true) => out.extend_from_slice(b"true"),
        Value::Bool(false) => out.extend_from_slice(b"false"),
        Value::Number(number) => serde_json::to_writer(&mut *out, number)?,
        Value::String(text) => serde_json::to_writer(&mut *out, text)?,
        Value::Array(items) => {
            let depth = deeper(depth)?;
            out.push(b'[');
            for (index, item) in items.iter().enumerate() {
                if index > 0 {
                    out.push(b',');
                }
                write_value(out, item, depth)?;
            }
            out.push(b']');
        }
        Value::Object(members) => {
            let depth = deeper(depth)?;
            let mut sorted: Vec<(&String, &Value)> = members.iter().collect();
            // `str`'s ordering is the ordering of its UTF-8 bytes.
            sorted.sort_unstable_by(|(a, _), (b, _)| a.as_bytes().cmp(b.as_bytes()));
            out.push(b'{');
            for (index, (key, member)) in sorted.into_iter().enumerate() {
                if index > 0 {
                    out.push(b',');
                }
                serde_json::to_writer(&mut *out, key)?;
                out.push(b':');
                write_value(out, member, depth)?;
            }
            out.push(b'}');
        }
    }
    Ok(())
}

/// The depth inside one more array or object, refused beyond [`MAX_DEPTH`].
fn deeper(depth: usize) -> Result<usize> {
    let depth = depth + 1;
    if depth > MAX_DEPTH {
        return Err(Error::InvalidJob(format!(
            "a value to hash is nested more than {MAX_DEPTH} levels deep"
        )));
    }
    Ok(depth)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::{Map, json};

    fn text(value: &Value) -> String {
        String::from_utf8(canonical_json(value).unwrap()).unwrap()
    }

    #[test]
    fn values_are_written_compactly_with_sorted_members() {
        assert_eq!(text(&json!(null)), "null");
        assert_eq!(
            text(&json!([true, false, 1, 1.0, -2.5, "a"])),
            r#"[true,false,1,1.0,-2.5,"a"]"#
        );
        assert_eq!(
            text(&json!({"b": 1, "a": {"z": [3, {"y": 1, "x": 2}], "c": null}})),
            r#"{"a":{"c":null,"z":[3,{"x":2,"y":1}]},"b":1}"#
        );
        // serde_json's escaping, including control characters and non-ASCII text.
        assert_eq!(text(&json!("q\"\\\n\u{1}é")), r#""q\"\\\n\u0001é""#);
        // Keys sort by their UTF-8 bytes: uppercase before lowercase, "é" after "z".
        assert_eq!(
            text(&json!({"é": 1, "z": 2, "Z": 3, "": 4})),
            r#"{"":4,"Z":3,"z":2,"é":1}"#
        );
    }

    #[test]
    fn insertion_order_does_not_matter() {
        let mut forwards = Map::new();
        let mut backwards = Map::new();
        let keys = ["delta", "alpha", "charlie", "bravo"];
        for (index, key) in keys.iter().enumerate() {
            forwards.insert((*key).to_owned(), json!(index));
        }
        for (index, key) in keys.iter().enumerate().rev() {
            backwards.insert((*key).to_owned(), json!(index));
        }
        assert_eq!(
            canonical_json(&Value::Object(forwards)).unwrap(),
            canonical_json(&Value::Object(backwards)).unwrap()
        );
    }

    #[test]
    fn types_are_kept_apart() {
        let digests: std::collections::HashSet<String> = [
            json!(null),
            json!(1),
            json!(1.0),
            json!(true),
            json!("1"),
            json!([]),
            json!([null]),
        ]
        .iter()
        .map(|value| digest(value).unwrap())
        .collect();
        assert_eq!(digests.len(), 7);
    }

    #[test]
    fn depth_is_bounded() {
        let mut value = json!(0);
        for _ in 0..MAX_DEPTH {
            value = json!([value]);
        }
        assert!(canonical_json(&value).is_ok());
        let too_deep = json!({ "x": value });
        assert!(matches!(
            canonical_json(&too_deep),
            Err(Error::InvalidJob(_))
        ));
    }

    #[test]
    fn digests_are_43_url_safe_characters() {
        // Golden vectors (pinned: every stored digest depends on them).
        assert_eq!(
            sha256_b64url(b""),
            "47DEQpj8HBSa-_TImW-5JCeuQeRkm5NMpJWZG3hSuFU"
        );
        assert_eq!(
            sha256_b64url(b"hench"),
            "g4sctBBzMZw4hfCWOMjgE2bz7NxcihZRDSd03oSxdAg"
        );
        assert_eq!(
            digest(&json!({"b": [1, 2], "a": "x"})).unwrap(),
            sha256_b64url(br#"{"a":"x","b":[1,2]}"#)
        );
        let value = digest(&json!(["hench-unique/1", "Job", "default", [42]])).unwrap();
        assert_eq!(value.len(), DIGEST_CHARS);
        assert!(
            value
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
        );
    }
}

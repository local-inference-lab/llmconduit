//! Opt-in repairs for malformed upstream tool calls.
//!
//! Some models mix tool-call encodings: they open the arguments as JSON (what the
//! OpenAI-compatible tool contract asks for) and then switch into the markup their
//! own chat template trained them on. GLM does this with `<arg_key>`/`<arg_value>`,
//! and it happens mid-argument, so the result is still SYNTACTICALLY VALID JSON —
//! nothing downstream can reject it. The first argument simply swallows the rest:
//!
//! ```text
//! {"action": "edit<arg_key>appendContent</arg_key><arg_value>…the real value…",
//!  "scope": "local"}
//! ```
//!
//! The harness then sees `action: "edit<arg_key>…"`, rejects the call, and the
//! agent retries — intermittently, because the drift happens on long values.
//! Observed on GLM-5.2 and GLM-5.3-Flash through both llmconduit and LiteLLM, so
//! it is the model, not a gateway: the repair is a tolerant parse of a known
//! vendor quirk and is therefore OFF unless a model profile asks for it.

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

/// A named repair, enabled per model profile (`tool_call_repairs`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ToolCallRepair {
    /// Split GLM's native `<arg_key>K</arg_key><arg_value>V` markup back out of
    /// the JSON string value that swallowed it.
    GlmArgMarkup,
}

const ARG_KEY_OPEN: &str = "<arg_key>";
const ARG_KEY_CLOSE: &str = "</arg_key>";
const ARG_VALUE_OPEN: &str = "<arg_value>";
const ARG_VALUE_CLOSE: &str = "</arg_value>";

/// Whether `arguments` shows the GLM markup at all — the cheap gate that keeps a
/// healthy tool call on the untouched path.
pub fn needs_glm_arg_markup_repair(arguments: &str) -> bool {
    arguments.contains(ARG_KEY_OPEN)
}

/// Apply the enabled repairs to one tool call's raw `arguments` text. Returns
/// `None` when nothing applies, so the caller keeps the original bytes.
pub fn repair_arguments(arguments: &str, repairs: &[ToolCallRepair]) -> Option<String> {
    if !repairs.contains(&ToolCallRepair::GlmArgMarkup) || !needs_glm_arg_markup_repair(arguments) {
        return None;
    }
    repair_glm_arg_markup(arguments)
}

/// Recover the arguments object from GLM's hybrid output.
///
/// Two shapes are handled:
/// - JSON whose string values carry the markup (the common hybrid); each such
///   value is split into the part the model meant plus the arguments it encoded
///   in markup.
/// - Arguments that are ONLY markup (no JSON at all), which the same parse folds
///   into a plain object.
///
/// Returns `None` when the text cannot be read either way — the caller then keeps
/// the upstream bytes rather than inventing a call.
fn repair_glm_arg_markup(arguments: &str) -> Option<String> {
    if let Ok(Value::Object(object)) = serde_json::from_str::<Value>(arguments) {
        let mut repaired = Map::new();
        let mut changed = false;
        for (key, value) in object {
            let Value::String(text) = &value else {
                repaired.insert(key, value);
                continue;
            };
            let Some((head, recovered)) = split_markup(text) else {
                repaired.insert(key, value);
                continue;
            };
            changed = true;
            repaired.insert(key, Value::String(head));
            for (name, recovered_value) in recovered {
                repaired.insert(name, Value::String(recovered_value));
            }
        }
        if !changed {
            return None;
        }
        return serde_json::to_string(&Value::Object(repaired)).ok();
    }
    // Pure markup: no JSON wrapper at all.
    let (head, recovered) = split_markup(arguments)?;
    if !head.trim().is_empty() || recovered.is_empty() {
        return None;
    }
    let object: Map<String, Value> = recovered
        .into_iter()
        .map(|(name, value)| (name, Value::String(value)))
        .collect();
    serde_json::to_string(&Value::Object(object)).ok()
}

/// Split `text` into everything before the first `<arg_key>` and the (key, value)
/// pairs the markup encodes. `None` when there is no complete `<arg_key>…</arg_key>`
/// pair — a stray fragment is left alone rather than guessed at.
fn split_markup(text: &str) -> Option<(String, Vec<(String, String)>)> {
    let first = text.find(ARG_KEY_OPEN)?;
    let head = text[..first].to_string();
    let mut recovered = Vec::new();
    let mut rest = &text[first..];
    while let Some(open) = rest.find(ARG_KEY_OPEN) {
        let after_key_open = &rest[open + ARG_KEY_OPEN.len()..];
        let Some(key_end) = after_key_open.find(ARG_KEY_CLOSE) else {
            break;
        };
        let key = after_key_open[..key_end].trim().to_string();
        let after_key = &after_key_open[key_end + ARG_KEY_CLOSE.len()..];
        // The value opener is expected right after the key; tolerate whitespace.
        let after_value_open = match after_key.trim_start().strip_prefix(ARG_VALUE_OPEN) {
            Some(remainder) => remainder,
            // A key with no value block: nothing trustworthy to recover.
            None => break,
        };
        // The value runs to its own close tag, else to the next key, else to the end.
        let value_end = after_value_open
            .find(ARG_VALUE_CLOSE)
            .or_else(|| after_value_open.find(ARG_KEY_OPEN))
            .unwrap_or(after_value_open.len());
        let value = after_value_open[..value_end].to_string();
        if key.is_empty() {
            break;
        }
        recovered.push((key, value));
        rest = &after_value_open[value_end..];
        rest = rest.strip_prefix(ARG_VALUE_CLOSE).unwrap_or(rest);
    }
    if recovered.is_empty() {
        return None;
    }
    Some((head, recovered))
}

#[cfg(test)]
mod tests {
    use super::*;

    const GLM: &[ToolCallRepair] = &[ToolCallRepair::GlmArgMarkup];

    /// The exact shape captured off GLM-5.2 through the gateway: the second
    /// argument's key and value live inside the FIRST argument's string, and the
    /// whole thing still parses as JSON.
    #[test]
    fn splits_the_swallowed_argument_back_out() {
        let hybrid = r#"{"action": "edit<arg_key>appendContent</arg_key><arg_value>\n## NOTES\n- one\n- two", "scope": "local", "title": "auto-transaction-matrix"}"#;
        let repaired = repair_arguments(hybrid, GLM).expect("repaired");
        let value: Value = serde_json::from_str(&repaired).expect("valid JSON");
        assert_eq!(value["action"], "edit");
        assert_eq!(value["appendContent"], "\n## NOTES\n- one\n- two");
        assert_eq!(value["scope"], "local");
        assert_eq!(value["title"], "auto-transaction-matrix");
    }

    /// A closed `</arg_value>` and several recovered arguments in one value.
    #[test]
    fn recovers_every_markup_argument_in_one_value() {
        let hybrid = r#"{"op": "run<arg_key>path</arg_key><arg_value>/tmp/x</arg_value><arg_key>mode</arg_key><arg_value>fast</arg_value>"}"#;
        let repaired = repair_arguments(hybrid, GLM).expect("repaired");
        let value: Value = serde_json::from_str(&repaired).expect("valid JSON");
        assert_eq!(value["op"], "run");
        assert_eq!(value["path"], "/tmp/x");
        assert_eq!(value["mode"], "fast");
    }

    /// Arguments that are ONLY markup still yield an object.
    #[test]
    fn folds_pure_markup_into_an_object() {
        let markup = "<arg_key>query</arg_key><arg_value>ledger totals</arg_value>";
        let repaired = repair_arguments(markup, GLM).expect("repaired");
        assert_eq!(
            serde_json::from_str::<Value>(&repaired).unwrap()["query"],
            "ledger totals"
        );
    }

    /// A healthy tool call is never touched, and neither is anything when the
    /// repair is not enabled for the model.
    #[test]
    fn leaves_healthy_and_unconfigured_calls_alone() {
        let healthy = r#"{"action": "edit", "appendContent": "hello"}"#;
        assert_eq!(repair_arguments(healthy, GLM), None);
        let hybrid = r#"{"action": "edit<arg_key>k</arg_key><arg_value>v"}"#;
        assert_eq!(repair_arguments(hybrid, &[]), None, "off unless configured");
        assert!(
            repair_arguments(hybrid, GLM).is_some(),
            "on when configured"
        );
    }

    /// Fragments that do not form a complete key/value block are left as they
    /// are: the repair never invents an argument.
    #[test]
    fn refuses_to_guess_at_a_partial_block() {
        for partial in [
            r#"{"action": "edit<arg_key>truncated"}"#,
            r#"{"action": "edit<arg_key>k</arg_key>no value block"}"#,
            r#"{"action": "edit<arg_key></arg_key><arg_value>v"}"#,
        ] {
            assert_eq!(repair_arguments(partial, GLM), None, "{partial}");
        }
    }

    /// Non-string arguments (numbers, arrays, objects) survive a repair of their
    /// sibling untouched.
    #[test]
    fn preserves_non_string_siblings() {
        let hybrid = r#"{"action": "edit<arg_key>body</arg_key><arg_value>text", "count": 3, "flags": ["a"], "nested": {"k": 1}}"#;
        let repaired = repair_arguments(hybrid, GLM).expect("repaired");
        let value: Value = serde_json::from_str(&repaired).expect("valid JSON");
        assert_eq!(value["action"], "edit");
        assert_eq!(value["body"], "text");
        assert_eq!(value["count"], 3);
        assert_eq!(value["flags"], serde_json::json!(["a"]));
        assert_eq!(value["nested"], serde_json::json!({"k": 1}));
    }
}

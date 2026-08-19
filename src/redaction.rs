//! Image-URI redaction primitives shared across every logging/echoing surface.
//!
//! These are intentionally NOT vision-specific: the inbound request trace
//! (`http::redact_payload_secrets`), the upstream JSONL request log, the debug
//! monitor + `/debug/ws` broadcast, and the vision success/error text all route
//! through the SAME redactor so a `data:` image payload or a signed image URL
//! cannot leak to any sink (AGENTS.md redact rule). Keeping the URI semantics in
//! one module means there is exactly one definition of "what is a sensitive URI
//! run" to audit.
//!
//! The vision module re-exports [`redact_image_uris`], [`redact_image_uris_in_value`],
//! and [`redact_vision_text`] so existing `crate::vision::*` call sites keep
//! resolving; new code should prefer `crate::redaction::*`.

/// Cap on the redacted vision text snippet that becomes model-visible/logged.
const VISION_TEXT_REDACT_LIMIT: usize = 4096;

/// URI prefixes that could carry raw image data or a signed image URL, in BOTH
/// raw and JSON-escaped (`\/`) forms (G4 round-3 #4). Matched case-insensitively
/// (round-2 #2). `data:` base64 payloads have no slash to escape. Order matters
/// only for disambiguation; we pick the earliest match regardless.
const SENSITIVE_URI_PREFIXES: [&str; 5] = [
    "data:",
    "https://",
    "http://",
    "https:\\/\\/",
    "http:\\/\\/",
];

/// Whether `c` ends a sensitive URI run. A `data:` base64 URL contains a `,`
/// SEPARATING the media type from the payload, so `,` must not terminate it;
/// only whitespace/quote/bracket bounds a `data:` run. For an `http(s)` URL a
/// comma/paren also bounds it in prose/JSON. A backslash is NOT a delimiter so
/// JSON-escaped `\/` inside a URL is consumed as part of the run.
fn is_uri_run_delimiter(c: char, is_data: bool) -> bool {
    c.is_whitespace()
        || matches!(c, '"' | '\'' | ']' | '}' | '<' | '>')
        || (!is_data && matches!(c, ')' | ','))
}

/// THE single image-redaction primitive (G4 round-4 consolidation). Replaces
/// every `data:` and `http(s)` URI run — case-insensitive, raw AND JSON-escaped
/// (`\/`) form, including signed-URL query tokens — with `<redacted uri>`. This
/// is the one place the URI semantics live; ALL logging/echoing surfaces route
/// through it (inbound trace, upstream JSONL, debug monitor + `/debug/ws`, and
/// vision success/error text), so request image bytes / signed URLs cannot leak
/// to any sink (AGENTS.md redact rule). Does NOT truncate — callers that need a
/// length cap layer it on (see [`redact_vision_text`]).
pub fn redact_image_uris(text: &str) -> String {
    let lower = text.to_ascii_lowercase();
    let mut out = String::with_capacity(text.len());
    let mut cursor = 0usize;
    while cursor < text.len() {
        // Earliest sensitive URI start at/after `cursor` (case-insensitive via
        // the lowercased copy, which preserves byte offsets for ASCII prefixes).
        let next = SENSITIVE_URI_PREFIXES
            .iter()
            .filter_map(|prefix| {
                lower[cursor..]
                    .find(prefix)
                    .map(|rel| (cursor + rel, *prefix))
            })
            .min_by_key(|(pos, _)| *pos);
        let Some((start, prefix)) = next else {
            out.push_str(&text[cursor..]);
            break;
        };
        out.push_str(&text[cursor..start]);
        out.push_str("<redacted uri>");
        let is_data = prefix.starts_with("data:");
        let after = &text[start + prefix.len()..];
        let end = after
            .find(|c: char| is_uri_run_delimiter(c, is_data))
            .unwrap_or(after.len());
        cursor = start + prefix.len() + end;
    }
    out
}

/// Recursively redact image URIs in every string within a JSON value (G4
/// round-4 consolidation). Used by the request-logging surfaces (inbound trace
/// `redact_payload_secrets`, upstream JSONL) so a `data:`/signed `image_url`
/// anywhere in the body — string field, content-part, nested object/array — is
/// stripped before serialization, regardless of key name.
pub fn redact_image_uris_in_value(value: &mut serde_json::Value) {
    match value {
        serde_json::Value::String(text) => {
            let redacted = redact_image_uris(text);
            if redacted != *text {
                *text = redacted;
            }
        }
        serde_json::Value::Array(items) => {
            for item in items {
                redact_image_uris_in_value(item);
            }
        }
        serde_json::Value::Object(map) => {
            for (_, item) in map.iter_mut() {
                redact_image_uris_in_value(item);
            }
        }
        _ => {}
    }
}

/// Recursively redact SENSITIVE-KEY values in a JSON value — the shared walker
/// behind `http::redact_payload_secrets` (D1 R1 #10's "single sensitive-key
/// authority" extended to the walk itself, not just [`is_sensitive_payload_key`]),
/// so a NEW logged surface (e.g. the turn-capture `upstream_request` section,
/// `upstream.rs`) reuses the SAME redaction instead of re-implementing it
/// (AGENTS.md: don't bypass `redact_payload_secrets` when adding a new logged
/// surface). Any object key satisfying [`is_sensitive_payload_key`] has its value
/// replaced with the literal string `"[redacted]"`, regardless of nesting depth;
/// every other value is walked recursively (objects/arrays) or left as-is
/// (scalars).
pub(crate) fn redact_payload_secrets_in_value(value: &mut serde_json::Value) {
    match value {
        serde_json::Value::Object(map) => {
            for (key, value) in map {
                if is_sensitive_payload_key(key) {
                    *value = serde_json::Value::String("[redacted]".to_string());
                } else {
                    redact_payload_secrets_in_value(value);
                }
            }
        }
        serde_json::Value::Array(values) => {
            for value in values {
                redact_payload_secrets_in_value(value);
            }
        }
        _ => {}
    }
}

/// Vision text that becomes model-visible or logged: the analyzer's success
/// description and its error bodies/messages. Image URIs are redacted via
/// [`redact_image_uris`], then the result is UTF-8-safely capped so only a
/// bounded, image-free, token-free remainder survives.
pub fn redact_vision_text(text: &str) -> String {
    let trimmed = redact_image_uris(text);
    let trimmed = trimmed.trim();
    if trimmed.chars().count() > VISION_TEXT_REDACT_LIMIT {
        let end = trimmed
            .char_indices()
            .nth(VISION_TEXT_REDACT_LIMIT)
            .map(|(idx, _)| idx)
            .unwrap_or(trimmed.len());
        format!("{}…[truncated]", &trimmed[..end])
    } else {
        trimmed.to_string()
    }
}

// ===========================================================================
// Secret-key authority + the capped, redacting STREAMING capture primitive.
//
// These were consolidated here from `http.rs` / `dashboard_flow.rs` (D1 R1 #10)
// so there is ONE definition of "what key is sensitive" and ONE capped+redacting
// body serializer, reused by the inbound trace logger and the dashboard FlowStore
// capture seam. The capture primitive is O(CAP): it never materializes the whole
// body (no full `serde_json::Value`), redacts secrets inline (sensitive keys →
// `"[redacted]"`, image/data URIs stripped INCLUDING `\uXXXX`-escaped forms,
// over-long scalars + keys capped), and on malformed/too-deep/non-UTF8 input
// falls back to `redact_image_uris` over a CAP-bounded lossy prefix only.
// ===========================================================================

/// THE single authority for "is this object key / header name sensitive" (its
/// value must never be retained). Used by the inbound-trace logger
/// (`http::redact_payload_secrets`) and the dashboard capture seam alike.
pub(crate) fn is_sensitive_payload_key(key: &str) -> bool {
    let normalized = key.to_ascii_lowercase().replace(['-', '_'], "");
    matches!(
        normalized.as_str(),
        "apikey"
            | "xapikey"
            | "authorization"
            | "password"
            | "passwd"
            | "secret"
            | "clientsecret"
            | "accesstoken"
            | "refreshtoken"
            | "authtoken"
            | "bearertoken"
            // `openai-beta` carries feature-gating tokens; redact its value in
            // captured headers (D1 R1 #2). Also covers a JSON `openai_beta` field.
            | "openaibeta"
    )
}

/// Capped + redacting capture of a request/response body into an owned, redacted
/// `Vec<u8>` of at most `body_cap` bytes. Secrets are redacted INLINE; peak heap
/// use is O(`body_cap` + `scalar_cap`), never O(body) — a 10 MiB body is never
/// copied in full. Never retains a slice of `raw`. Callers wrap the result in an
/// `Arc<[u8]>` as needed (the dashboard FlowStore does).
pub(crate) fn capture_capped_redacted(raw: &[u8], body_cap: usize, scalar_cap: usize) -> Vec<u8> {
    // Common path: a single streaming pass over the bytes into a hard-capped writer
    // that pre-reserves only `min(raw.len(), body_cap)` and stops at the cap. No
    // `serde_json::Value` is ever built. A too-deep SUBTREE is replaced inline with
    // a placeholder and skipped (the parser still succeeds), so shallow `api_key`s
    // are redacted and the shallow preview survives WITHOUT reaching the fallback.
    let mut writer = CappedWriter::new(raw.len(), body_cap);
    if let Ok(text) = std::str::from_utf8(raw) {
        let mut parser = JsonRedactor::new(text, scalar_cap);
        parser.skip_ws();
        if parser.redact_value(&mut writer, 0, false) && parser.at_trailing_end() {
            return writer.into_vec();
        }
    }
    // Fallback (RARE — genuinely NON-JSON or NON-UTF8): the streaming redactor could
    // not structurally parse the body, so we CANNOT prove which spans are secrets
    // (an `api_key` / `\u`-escaped URI could sit anywhere). Retaining a lossy prefix
    // and only image-URI-redacting it would LEAK those (D1 R2 #1a). Instead store a
    // bounded FIXED marker that contains NONE of the original bytes — guaranteeing
    // no secret survives — while still recording that a body existed and its size.
    format!("[redacted: unparseable body {} bytes]", raw.len()).into_bytes()
}

/// Capped + redacting capture of a `Serialize` value WITHOUT a full O(body)
/// intermediate `Vec` (D2 R1 #1). The previous leaf path did `serde_json::to_vec`
/// (a full O(body) allocation for a huge prompt) and only THEN capped — defeating
/// the O(CAP) guarantee. Here the value is serialized via `serde_json::to_writer`
/// into a [`BoundedSerializeWriter`] hard-bounded at `2 × body_cap`: serialization
/// STOPS (errors) the instant it would exceed the bound, so peak heap stays
/// O(`body_cap`), never O(body). The bounded raw bytes are then run through the
/// EXISTING capped + redacting JSON pass ([`capture_capped_redacted`]):
/// - WITHIN the bound: a normal redacted, capped preview (secrets stripped). The
///   `2×` headroom means a body whose serialized form is up to `2 × body_cap` still
///   yields a full `body_cap` redacted preview even though redaction can EXPAND a
///   string (`data:` runs → `<redacted uri>`).
/// - OVER the bound: the writer truncates the bytes mid-JSON, so the strict
///   streaming parser bails and the body routes to the fixed
///   `[redacted: unparseable body …]` marker — NO raw/secret bytes survive, exactly
///   like the non-UTF8/malformed fallback. (A multi-MiB body is observability-only;
///   recording a bounded marker instead of its preview is the safe, O(CAP) choice.)
///
/// All secret-redaction guarantees of [`capture_capped_redacted`] hold unchanged
/// (sensitive keys → `[redacted]`, image/data URIs stripped incl. `\uXXXX` forms).
pub(crate) fn capture_capped_redacted_value<T: serde::Serialize>(
    value: &T,
    body_cap: usize,
    scalar_cap: usize,
) -> Vec<u8> {
    // Serialize directly into a writer bounded at 2×body_cap (never a full Vec). The
    // `2×` headroom lets a within-bound body survive redaction's possible expansion
    // and still produce a full body_cap preview; past the bound the writer truncates.
    let serialize_bound = body_cap.saturating_mul(2);
    let mut writer = BoundedSerializeWriter::new(serialize_bound);
    // `to_writer` returns Err the moment the writer reports it is full; in BOTH the
    // Ok and the bounded-overflow case the (≤ 2×body_cap) accumulated bytes are run
    // through the strict capped+redacting pass below. On overflow the bytes are a
    // mid-JSON truncation → the strict parser bails → fixed marker (no secret leak).
    let _ = serde_json::to_writer(&mut writer, value);
    capture_capped_redacted(writer.as_bytes(), body_cap, scalar_cap)
}

/// A `std::io::Write` sink that accumulates serialized bytes only until it reaches
/// `bound`; once full it returns a write error so `serde_json::to_writer` STOPS
/// (D2 R1 #1) — no full O(body) buffer is ever built. Pre-reserves only
/// `min(bound, 64 KiB)` so a tiny body does not eagerly allocate the whole bound.
struct BoundedSerializeWriter {
    buf: Vec<u8>,
    bound: usize,
    full: bool,
}

impl BoundedSerializeWriter {
    fn new(bound: usize) -> Self {
        Self {
            buf: Vec::with_capacity(bound.min(64 * 1024)),
            bound,
            full: false,
        }
    }

    fn as_bytes(&self) -> &[u8] {
        &self.buf
    }
}

impl std::io::Write for BoundedSerializeWriter {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        if self.full {
            return Err(std::io::Error::new(
                std::io::ErrorKind::WriteZero,
                "capture serialize bound exceeded",
            ));
        }
        let remaining = self.bound - self.buf.len();
        if bytes.len() <= remaining {
            self.buf.extend_from_slice(bytes);
            if self.buf.len() == self.bound {
                self.full = true;
            }
            Ok(bytes.len())
        } else {
            // Keep the bounded prefix (a mid-JSON truncation) and stop serialization.
            self.buf.extend_from_slice(&bytes[..remaining]);
            self.full = true;
            Err(std::io::Error::new(
                std::io::ErrorKind::WriteZero,
                "capture serialize bound exceeded",
            ))
        }
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// Redact captured headers. The NAME is capped to `scalar_cap` (D1 R2 #4 — a
/// pathologically long header name must not be retained unbounded). For a sensitive
/// name the VALUE becomes `"[redacted]"`. For every other header the value is run
/// through [`redact_image_uris`] FIRST, THEN capped to `scalar_cap` — redaction can
/// EXPAND a value (many `data:` runs → `<redacted uri>`), so capping post-redaction
/// is what actually bounds the retained bytes (D1 R2 #4).
pub(crate) fn redact_headers_capped(
    headers: &axum::http::HeaderMap,
    scalar_cap: usize,
) -> Vec<(String, String)> {
    headers
        .iter()
        .map(|(name, value)| {
            let name = cap_str_on_char_boundary(name.as_str(), scalar_cap).to_string();
            if is_sensitive_payload_key(&name) {
                return (name, "[redacted]".to_string());
            }
            let raw = value.to_str().unwrap_or("<non-utf8>");
            // Redact image/data URIs FIRST (it can grow the string), THEN cap.
            let redacted = redact_image_uris(raw);
            let capped = cap_str_on_char_boundary(&redacted, scalar_cap).to_string();
            (name, capped)
        })
        .collect()
}

/// Cap `text` to at most `cap` bytes on a UTF-8 char boundary (no allocation when
/// already within cap).
fn cap_str_on_char_boundary(text: &str, cap: usize) -> &str {
    if text.len() <= cap {
        return text;
    }
    let bytes = text.as_bytes();
    let mut end = cap;
    while end > 0 && (bytes[end] & 0xC0) == 0x80 {
        end -= 1;
    }
    &text[..end]
}

/// A `Vec<u8>` that accepts writes only until it reaches its cap; once full it
/// silently drops further writes and records `full = true`. Pre-reserves only
/// `min(hint, cap)` so peak allocation is bounded by the cap.
struct CappedWriter {
    buf: Vec<u8>,
    cap: usize,
    full: bool,
}

impl CappedWriter {
    fn new(size_hint: usize, cap: usize) -> Self {
        Self {
            buf: Vec::with_capacity(size_hint.min(cap)),
            cap,
            full: false,
        }
    }

    /// Append `bytes`, stopping at the cap. Returns `false` once the writer is full.
    fn write(&mut self, bytes: &[u8]) -> bool {
        if self.full {
            return false;
        }
        let remaining = self.cap - self.buf.len();
        if bytes.len() <= remaining {
            self.buf.extend_from_slice(bytes);
            if self.buf.len() == self.cap {
                self.full = true;
            }
            true
        } else {
            self.buf.extend_from_slice(&bytes[..remaining]);
            self.full = true;
            false
        }
    }

    fn write_byte(&mut self, byte: u8) -> bool {
        self.write(&[byte])
    }

    fn into_vec(self) -> Vec<u8> {
        self.buf
    }
}

/// Forward-pass recursive-descent JSON redactor over a `&str`. Walks the value at
/// the cursor writing a redacted copy into a [`CappedWriter`], never building a
/// `Value`. Returns `false` on malformed input or depth overflow so the caller can
/// fall back. `scalar_cap` bounds retained string VALUES and object KEYS.
struct JsonRedactor<'a> {
    bytes: &'a [u8],
    pos: usize,
    scalar_cap: usize,
}

/// Recursion-depth limit — bounds the call stack on adversarial nesting. At this
/// depth a subtree is replaced with a `"[truncated: too deep]"` placeholder and
/// skipped (siblings continue). Kept well below `serde_json`'s own default parse
/// recursion limit (128) so the redacted OUTPUT (≤ this many nested levels) always
/// re-parses cleanly downstream.
const MAX_JSON_DEPTH: usize = 64;

impl<'a> JsonRedactor<'a> {
    fn new(text: &'a str, scalar_cap: usize) -> Self {
        Self {
            bytes: text.as_bytes(),
            pos: 0,
            scalar_cap,
        }
    }

    fn skip_ws(&mut self) {
        while self.pos < self.bytes.len() {
            match self.bytes[self.pos] {
                b' ' | b'\t' | b'\n' | b'\r' => self.pos += 1,
                _ => break,
            }
        }
    }

    fn peek(&self) -> Option<u8> {
        self.bytes.get(self.pos).copied()
    }

    /// After parsing the top-level value, only whitespace may remain.
    fn at_trailing_end(&mut self) -> bool {
        self.skip_ws();
        self.pos >= self.bytes.len()
    }

    /// Redact one JSON value at the cursor into `out`. When `sensitive` is set the
    /// value belongs to a sensitive key, so it is replaced wholesale with
    /// `"[redacted]"` and parse-skipped. At the depth limit, the value is replaced
    /// with a `"[truncated: too deep]"` placeholder and PARSE-SKIPPED (D1 R2 #1b),
    /// then redaction CONTINUES on shallower siblings — so a deeply nested body no
    /// longer discards the whole (shallow) preview to the lossy fallback, and any
    /// shallow/top-level `api_key` still gets redacted. Returns `false` only on
    /// genuinely malformed input.
    fn redact_value(&mut self, out: &mut CappedWriter, depth: usize, sensitive: bool) -> bool {
        self.skip_ws();
        if self.peek().is_none() {
            return false;
        }
        if sensitive {
            out.write(b"\"[redacted]\"");
            return self.skip_value();
        }
        if depth > MAX_JSON_DEPTH {
            // Too deep to recurse safely: emit a placeholder and skip this subtree
            // iteratively (stack-safe), then let the caller continue with siblings.
            out.write(b"\"[truncated: too deep]\"");
            return self.skip_value();
        }
        match self.peek() {
            Some(b'{') => self.redact_object(out, depth),
            Some(b'[') => self.redact_array(out, depth),
            Some(b'"') => self.redact_string(out),
            _ => self.copy_scalar(out),
        }
    }

    fn redact_object(&mut self, out: &mut CappedWriter, depth: usize) -> bool {
        self.pos += 1; // consume '{'
        out.write_byte(b'{');
        self.skip_ws();
        if self.peek() == Some(b'}') {
            self.pos += 1;
            out.write_byte(b'}');
            return true;
        }
        loop {
            self.skip_ws();
            if self.peek() != Some(b'"') {
                return false;
            }
            // Emit the key CAPPED (D1 R1 #4a: a huge key must not allocate O(body))
            // and decode only a `scalar_cap`-bounded prefix for the sensitivity test.
            let key = match self.read_key_string(out) {
                Some(key) => key,
                None => return false,
            };
            self.skip_ws();
            if self.peek() != Some(b':') {
                return false;
            }
            self.pos += 1;
            out.write_byte(b':');
            let sensitive = is_sensitive_payload_key(&key);
            if !self.redact_value(out, depth + 1, sensitive) {
                return false;
            }
            self.skip_ws();
            match self.peek() {
                Some(b',') => {
                    self.pos += 1;
                    out.write_byte(b',');
                }
                Some(b'}') => {
                    self.pos += 1;
                    out.write_byte(b'}');
                    return true;
                }
                _ => return false,
            }
        }
    }

    fn redact_array(&mut self, out: &mut CappedWriter, depth: usize) -> bool {
        self.pos += 1; // consume '['
        out.write_byte(b'[');
        self.skip_ws();
        if self.peek() == Some(b']') {
            self.pos += 1;
            out.write_byte(b']');
            return true;
        }
        loop {
            if !self.redact_value(out, depth + 1, false) {
                return false;
            }
            self.skip_ws();
            match self.peek() {
                Some(b',') => {
                    self.pos += 1;
                    out.write_byte(b',');
                }
                Some(b']') => {
                    self.pos += 1;
                    out.write_byte(b']');
                    return true;
                }
                _ => return false,
            }
        }
    }

    /// Read a JSON string key. Emits a CAPPED, re-encoded key (so a multi-MiB key
    /// allocates only `scalar_cap`) and returns the decoded+capped key text for the
    /// sensitivity test. Cursor lands just past the closing quote.
    fn read_key_string(&mut self, out: &mut CappedWriter) -> Option<String> {
        let range = self.scan_string_raw()?;
        let raw = self.token_str(range);
        let inner = &raw[1..raw.len() - 1];
        // Decode only a bounded prefix of the key (keys are short in practice; this
        // bounds the worst case at `scalar_cap`).
        let capped_inner = cap_json_string_inner(inner, self.scalar_cap);
        let decoded = decode_json_string_inner(capped_inner);
        // Re-encode the (decoded, bounded) key as a valid JSON string for output.
        out.write(encode_json_string(&decoded).as_bytes());
        Some(decoded)
    }

    /// Redact a JSON string VALUE: decode a `scalar_cap`-bounded prefix (so
    /// `\uXXXX`-escaped `data:`/`https:` URIs are de-escaped — D1 R1 #3), run
    /// [`redact_image_uris`] over the DECODED text, then re-encode as a valid JSON
    /// string. The token is a zero-copy slice — a 10 MiB value allocates only the
    /// bounded prefix. Cursor lands just past the closing quote.
    fn redact_string(&mut self, out: &mut CappedWriter) -> bool {
        let Some(range) = self.scan_string_raw() else {
            return false;
        };
        let raw = self.token_str(range);
        let inner = &raw[1..raw.len() - 1];
        let capped_inner = cap_json_string_inner(inner, self.scalar_cap);
        let decoded = decode_json_string_inner(capped_inner);
        let redacted = redact_image_uris(&decoded);
        out.write(encode_json_string(&redacted).as_bytes());
        true
    }

    /// Copy a JSON scalar (number / `true` / `false` / `null`) through verbatim.
    /// Copy a JSON scalar at a VALUE position — but ONLY a STRICTLY valid one
    /// (`true`/`false`/`null` literals exactly, or a number per the JSON grammar).
    /// An arbitrary non-JSON token (e.g. a malformed fragment `api_key=SECRET`) is
    /// REJECTED by returning `false`, which fails the whole streaming parse so the
    /// caller falls back to the fixed redacted marker instead of persisting the raw
    /// token (D1 R3 #1 — `copy_scalar` previously copied any delimiter-bounded run
    /// verbatim, leaking secrets in malformed bodies). The cursor advances past the
    /// token only on success.
    fn copy_scalar(&mut self, out: &mut CappedWriter) -> bool {
        let start = self.pos;
        let end = match self.peek() {
            Some(b't') => self.match_literal(b"true"),
            Some(b'f') => self.match_literal(b"false"),
            Some(b'n') => self.match_literal(b"null"),
            Some(b'-') | Some(b'0'..=b'9') => self.match_number(),
            _ => None,
        };
        let Some(end) = end else {
            return false;
        };
        self.pos = end;
        out.write(&self.bytes[start..end]);
        true
    }

    /// If the bytes at the cursor are EXACTLY `literal` followed by a value
    /// terminator (delimiter / whitespace / EOF), return the end offset; else
    /// `None`. Rejects `truefoo`, `nullx`, etc.
    fn match_literal(&self, literal: &[u8]) -> Option<usize> {
        let end = self.pos + literal.len();
        if self.bytes.len() < end || &self.bytes[self.pos..end] != literal {
            return None;
        }
        // The next byte (if any) must end the value, not extend the token.
        match self.bytes.get(end) {
            None | Some(b',' | b'}' | b']' | b' ' | b'\t' | b'\n' | b'\r') => Some(end),
            _ => None,
        }
    }

    /// Validate a JSON number starting at the cursor and return its end offset, or
    /// `None` if it is not a well-formed JSON number (no leading zeros like `01`, a
    /// digit required after `.`/`e`, and a value terminator must follow — so
    /// `01.2.3`, `1.`, `1e`, `1x` are all rejected).
    fn match_number(&self) -> Option<usize> {
        let bytes = self.bytes;
        let mut i = self.pos;
        let len = bytes.len();
        // Optional minus.
        if bytes.get(i) == Some(&b'-') {
            i += 1;
        }
        // Integer part: `0` alone, or `[1-9][0-9]*` (no leading zeros).
        match bytes.get(i) {
            Some(b'0') => {
                i += 1;
            }
            Some(b'1'..=b'9') => {
                i += 1;
                while matches!(bytes.get(i), Some(b'0'..=b'9')) {
                    i += 1;
                }
            }
            _ => return None,
        }
        // Optional fraction: `.` then at least one digit.
        if bytes.get(i) == Some(&b'.') {
            i += 1;
            if !matches!(bytes.get(i), Some(b'0'..=b'9')) {
                return None;
            }
            while matches!(bytes.get(i), Some(b'0'..=b'9')) {
                i += 1;
            }
        }
        // Optional exponent: `e`/`E`, optional sign, at least one digit.
        if matches!(bytes.get(i), Some(b'e' | b'E')) {
            i += 1;
            if matches!(bytes.get(i), Some(b'+' | b'-')) {
                i += 1;
            }
            if !matches!(bytes.get(i), Some(b'0'..=b'9')) {
                return None;
            }
            while matches!(bytes.get(i), Some(b'0'..=b'9')) {
                i += 1;
            }
        }
        // A value terminator must follow (no trailing junk like `1.2.3`/`1x`).
        match bytes.get(i) {
            _ if i >= len => Some(i),
            Some(b',' | b'}' | b']' | b' ' | b'\t' | b'\n' | b'\r') => Some(i),
            _ => None,
        }
    }

    /// Parse-skip a JSON value WITHOUT emitting it (a sensitive value whose
    /// redaction marker was already written, or a too-deep subtree). ITERATIVE
    /// bracket-counting so an arbitrarily deep value is skipped with O(1) stack
    /// (D1 R2 #1b — the recursive version could overflow on adversarial nesting and
    /// could not be used to skip past the depth limit). Returns `false` only on
    /// genuinely malformed input.
    fn skip_value(&mut self) -> bool {
        self.skip_ws();
        // A scalar / string value: consume it directly, no nesting.
        match self.peek() {
            Some(b'"') => return self.scan_string_raw().is_some(),
            Some(b'{') | Some(b'[') => {}
            Some(_) => {
                let start = self.pos;
                while self.pos < self.bytes.len() {
                    match self.bytes[self.pos] {
                        b',' | b'}' | b']' | b' ' | b'\t' | b'\n' | b'\r' => break,
                        _ => self.pos += 1,
                    }
                }
                return self.pos != start;
            }
            None => return false,
        }
        // A container: walk forward counting brackets, treating string tokens
        // opaquely (so a `{`/`[`/`"` INSIDE a string never perturbs the depth).
        let mut depth: usize = 0;
        while self.pos < self.bytes.len() {
            match self.bytes[self.pos] {
                b'"' => {
                    if self.scan_string_raw().is_none() {
                        return false;
                    }
                }
                b'{' | b'[' => {
                    depth += 1;
                    self.pos += 1;
                }
                b'}' | b']' => {
                    depth -= 1;
                    self.pos += 1;
                    if depth == 0 {
                        return true;
                    }
                }
                _ => self.pos += 1,
            }
        }
        // Ran off the end without closing every container → malformed.
        false
    }

    /// Scan a JSON string token INCLUDING the surrounding quotes, enforcing the
    /// STRICT JSON string grammar (D1 R4 #1 — a lenient scanner deemed
    /// `"api_key=SECRET\q"` valid, re-encoded it, and persisted the secret instead
    /// of hitting the unparseable-body marker). Returns the `start..end` BYTE RANGE
    /// (zero-copy) or `None` on ANY violation:
    /// - after `\`, only `" \ / b f n r t u` are valid escapes;
    /// - `\u` must be followed by EXACTLY 4 hex digits;
    /// - a literal control byte (< 0x20) inside the string is rejected (JSON forbids
    ///   unescaped control chars).
    ///
    /// A `None` here propagates up as a parse failure → the whole body routes to the
    /// fixed redacted marker. Returning a RANGE (not an owned `String`) keeps capture
    /// O(CAP): a 10 MiB value is never copied in full. Bytes come from a `&str`, so
    /// multibyte UTF-8 (≥ 0x80) is valid content, never a control char.
    fn scan_string_raw(&mut self) -> Option<(usize, usize)> {
        if self.peek() != Some(b'"') {
            return None;
        }
        let start = self.pos;
        self.pos += 1; // opening quote
        while self.pos < self.bytes.len() {
            match self.bytes[self.pos] {
                b'\\' => {
                    // An escape sequence must be well-formed.
                    self.pos += 1;
                    match self.bytes.get(self.pos) {
                        Some(b'"' | b'\\' | b'/' | b'b' | b'f' | b'n' | b'r' | b't') => {
                            self.pos += 1;
                        }
                        Some(b'u') => {
                            self.pos += 1;
                            // Exactly 4 hex digits must follow.
                            for _ in 0..4 {
                                match self.bytes.get(self.pos) {
                                    Some(c) if c.is_ascii_hexdigit() => self.pos += 1,
                                    _ => return None,
                                }
                            }
                        }
                        // Unknown/absent escape → malformed.
                        _ => return None,
                    }
                }
                b'"' => {
                    self.pos += 1;
                    return Some((start, self.pos));
                }
                // JSON forbids literal control characters in a string.
                c if c < 0x20 => return None,
                _ => self.pos += 1,
            }
        }
        // Ran off the end without a closing quote → unterminated/malformed.
        None
    }

    /// The validated `&str` for a scanned string-token range (valid UTF-8: the
    /// bytes came from a `&str` and we split only on ASCII boundaries).
    fn token_str(&self, range: (usize, usize)) -> &'a str {
        std::str::from_utf8(&self.bytes[range.0..range.1]).unwrap_or("")
    }
}

/// Cap the INNER text (between the quotes) of a JSON string to `cap` bytes without
/// splitting a UTF-8 sequence or a trailing JSON escape (`\`): if the cut lands
/// right after a lone backslash, back off one byte so decoding stays well-formed.
fn cap_json_string_inner(inner: &str, cap: usize) -> &str {
    if inner.len() <= cap {
        return inner;
    }
    let bytes = inner.as_bytes();
    let mut end = cap;
    while end > 0 && (bytes[end] & 0xC0) == 0x80 {
        end -= 1;
    }
    // Drop a dangling odd backslash run so the trailing escape is not truncated.
    let mut backslashes = 0;
    let mut i = end;
    while i > 0 && bytes[i - 1] == b'\\' {
        backslashes += 1;
        i -= 1;
    }
    if backslashes % 2 == 1 {
        end -= 1;
    }
    &inner[..end]
}

/// Decode the INNER text of a JSON string (escapes → chars), enough for image-URI
/// redaction + key sensitivity. Handles the standard JSON escapes incl. `\uXXXX`
/// (with surrogate-pair joining) so an escaped `data:`/`https:` scheme de-escapes
/// to its raw form and is caught by [`redact_image_uris`] (D1 R1 #3). Unknown
/// escapes pass through.
fn decode_json_string_inner(inner: &str) -> String {
    let mut out = String::with_capacity(inner.len());
    let mut chars = inner.chars().peekable();
    while let Some(c) = chars.next() {
        if c != '\\' {
            out.push(c);
            continue;
        }
        match chars.next() {
            Some('"') => out.push('"'),
            Some('\\') => out.push('\\'),
            Some('/') => out.push('/'),
            Some('n') => out.push('\n'),
            Some('t') => out.push('\t'),
            Some('r') => out.push('\r'),
            Some('b') => out.push('\u{0008}'),
            Some('f') => out.push('\u{000C}'),
            Some('u') => {
                let Some(hi) = take_hex4(&mut chars) else {
                    continue;
                };
                // High surrogate: try to join a following `\uXXXX` low surrogate.
                if (0xD800..=0xDBFF).contains(&hi) {
                    if chars.peek() == Some(&'\\') {
                        let mut lookahead = chars.clone();
                        lookahead.next(); // backslash
                        if lookahead.peek() == Some(&'u') {
                            lookahead.next(); // 'u'
                            if let Some(lo) = take_hex4(&mut lookahead)
                                && (0xDC00..=0xDFFF).contains(&lo)
                            {
                                let c = 0x10000 + ((hi - 0xD800) << 10) + (lo - 0xDC00);
                                if let Some(decoded) = char::from_u32(c) {
                                    out.push(decoded);
                                }
                                chars = lookahead;
                                continue;
                            }
                        }
                    }
                    // Unpaired high surrogate: emit replacement char.
                    out.push('\u{FFFD}');
                } else if let Some(decoded) = char::from_u32(hi) {
                    out.push(decoded);
                }
            }
            Some(other) => out.push(other),
            None => {}
        }
    }
    out
}

/// Consume exactly 4 hex digits from `chars`, returning their value, or `None`.
fn take_hex4(chars: &mut impl Iterator<Item = char>) -> Option<u32> {
    let mut value = 0u32;
    for _ in 0..4 {
        let digit = chars.next()?.to_digit(16)?;
        value = (value << 4) | digit;
    }
    Some(value)
}

/// Encode `text` as a JSON string literal INCLUDING surrounding quotes. The input
/// is already bounded (≤ scalar_cap), so this small `serde_json` call is cheap and
/// produces correct escaping for control chars / quotes / backslashes.
fn encode_json_string(text: &str) -> String {
    serde_json::to_string(text).unwrap_or_else(|_| "\"\"".to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use pretty_assertions::assert_eq;

    #[test]
    fn is_sensitive_payload_key_covers_aliases_and_openai_beta() {
        for key in [
            "api_key",
            "API-KEY",
            "x-api-key",
            "Authorization",
            "openai-beta",
            "openai_beta",
            "client_secret",
            "refresh_token",
        ] {
            assert!(is_sensitive_payload_key(key), "{key} must be sensitive");
        }
        for key in ["model", "content", "messages", "x-request-id"] {
            assert!(
                !is_sensitive_payload_key(key),
                "{key} must NOT be sensitive"
            );
        }
    }

    #[test]
    fn capture_redacts_sensitive_keys_and_escaped_uris() {
        // Sensitive key value redacted; a `\uXXXX`-escaped image scheme de-escaped
        // then stripped; structure preserved.
        let esc_data: String = "data"
            .chars()
            .map(|c| format!("\\u{:04x}", c as u32))
            .collect();
        let body = format!(
            r#"{{"model":"m","api_key":"sk-LEAK","img":"{esc_data}:image/png;base64,ESCLEAK x"}}"#
        );
        let out = capture_capped_redacted(body.as_bytes(), 128 * 1024, 4 * 1024);
        let text = String::from_utf8_lossy(&out);
        assert!(!text.contains("sk-LEAK"), "api_key redacted");
        assert!(!text.contains("ESCLEAK"), "escaped data: uri redacted");
        assert!(text.contains("[redacted]"));
        assert!(text.contains("<redacted uri>"));
        // Output is still valid JSON.
        let _: serde_json::Value = serde_json::from_slice(&out).expect("valid json out");
    }

    #[test]
    fn capture_respects_configurable_caps() {
        // A tiny body_cap bounds the OUTPUT; a tiny scalar_cap bounds the retained
        // string. Both are honored (the primitive is parameterized for reuse).
        let body = format!("{{\"s\":\"{}\"}}", "z".repeat(10_000));
        let out = capture_capped_redacted(body.as_bytes(), 256, 64);
        assert!(out.len() <= 256, "body_cap honored: {} > 256", out.len());
    }

    #[test]
    fn capture_fallback_stores_fixed_marker_no_raw_bytes() {
        // D1 R2 #1a: a genuinely non-JSON / non-UTF8 body cannot be structurally
        // parsed, so we cannot prove which spans are secrets — retaining a lossy
        // prefix would LEAK an `api_key`/URI. The fallback stores a FIXED marker
        // containing NONE of the original bytes.
        let raw = b"not json api_key=SECRETKEY data:image/png;base64,RAWLEAK trailing";
        let out = capture_capped_redacted(raw, 128 * 1024, 4 * 1024);
        let text = String::from_utf8_lossy(&out);
        assert!(
            !text.contains("SECRETKEY"),
            "no api_key survives the fallback"
        );
        assert!(
            !text.contains("RAWLEAK"),
            "no raw bytes survive the fallback"
        );
        assert!(!text.contains("not json"), "no original text retained");
        assert!(
            text.starts_with("[redacted: unparseable body"),
            "fixed redacted marker stored: {text}"
        );

        // Non-UTF8 bytes also hit the fixed marker (no panic, no raw bytes).
        let non_utf8 = [0xFF, 0xFE, b'a', b'p', b'i', 0x80];
        let out2 = capture_capped_redacted(&non_utf8, 128 * 1024, 4 * 1024);
        assert!(
            String::from_utf8_lossy(&out2).starts_with("[redacted: unparseable body"),
            "non-utf8 body → fixed marker"
        );
    }

    #[test]
    fn capture_too_deep_subtree_truncates_but_keeps_shallow_redaction() {
        // D1 R2 #1b: a VALID but too-deep JSON body must NOT discard the whole body
        // to the fallback. The deep subtree becomes a placeholder; a TOP-LEVEL
        // `api_key` is still redacted and the shallow preview survives.
        let deep = "[".repeat(300) + &"]".repeat(300);
        let body = format!(r#"{{"api_key":"DEEPLEAK","nested":{deep},"model":"m"}}"#);
        let out = capture_capped_redacted(body.as_bytes(), 128 * 1024, 4 * 1024);
        let text = String::from_utf8_lossy(&out);
        assert!(
            !text.contains("DEEPLEAK"),
            "top-level api_key still redacted"
        );
        assert!(text.contains("[redacted]"), "api_key marker present");
        assert!(
            text.contains("[truncated: too deep]"),
            "deep subtree truncated"
        );
        assert!(text.contains("\"model\":\"m\""), "shallow siblings survive");
        // The result is still valid JSON.
        let _: serde_json::Value = serde_json::from_slice(&out).expect("valid json out");
    }

    #[test]
    fn invalid_value_token_falls_back_no_raw_bytes() {
        // D1 R3 #1: a non-JSON token at a VALUE position (e.g. `api_key=SECRET`)
        // must NOT be copied verbatim — the whole body fails the streaming parse and
        // hits the fixed marker, so the secret never persists.
        let body = br#"{"x": api_key=SECRETVAL}"#;
        let out = capture_capped_redacted(body, 128 * 1024, 4 * 1024);
        let text = String::from_utf8_lossy(&out);
        assert!(
            !text.contains("SECRETVAL"),
            "raw value token not persisted: {text}"
        );
        assert!(
            text.starts_with("[redacted: unparseable body"),
            "invalid value token → fixed marker: {text}"
        );
    }

    #[test]
    fn invalid_number_and_bad_string_escape_fall_back() {
        // Malformed numbers and a truncated body are rejected by the strict scalar
        // grammar / scanner → fixed marker (no raw bytes copied).
        for body in [
            br#"{"n": 01.2.3}"#.as_slice(),  // leading zero + double dot
            br#"{"n": 1.}"#.as_slice(),      // digit required after '.'
            br#"{"n": 1e}"#.as_slice(),      // digit required after exponent
            br#"{"n": 1x9}"#.as_slice(),     // trailing junk
            br#"{"n": truefoo}"#.as_slice(), // literal with trailing junk
        ] {
            let out = capture_capped_redacted(body, 128 * 1024, 4 * 1024);
            let text = String::from_utf8_lossy(&out);
            assert!(
                text.starts_with("[redacted: unparseable body"),
                "malformed scalar must fall back to the fixed marker: input={:?} out={text}",
                String::from_utf8_lossy(body)
            );
        }
    }

    #[test]
    fn valid_scalars_round_trip_through_strict_validation() {
        // The strict scalar validator must still accept every well-formed JSON
        // scalar (sign/frac/exp numbers, the three literals).
        let body = br#"{"a":-0.5e+10,"b":true,"c":false,"d":null,"e":0,"f":42,"g":1.0E-3}"#;
        let out = capture_capped_redacted(body, 128 * 1024, 4 * 1024);
        let value: serde_json::Value =
            serde_json::from_slice(&out).expect("valid scalars preserved as JSON");
        assert_eq!(value["b"], serde_json::json!(true));
        assert_eq!(value["d"], serde_json::Value::Null);
        assert_eq!(value["e"], serde_json::json!(0));
        assert_eq!(value["f"], serde_json::json!(42));
    }

    #[test]
    fn malformed_strings_fall_back_no_secret_leak() {
        // D1 R4 #1: a malformed JSON string must route to the fixed marker, never
        // persist the (raw or decoded) secret. Each form carries `SECRET`.
        let bad_escape = br#"{"x":"api_key=SECRET\q"}"#.to_vec(); // invalid escape \q
        let incomplete_u = br#"{"x":"api_key=SECRET\uAB"}"#.to_vec(); // \u + only 2 hex
        // A literal control byte (0x01) inside the string.
        let mut control_byte = br#"{"x":"api_key=SECRET"#.to_vec();
        control_byte.push(0x01);
        control_byte.extend_from_slice(b"\"}");

        for raw in [bad_escape, incomplete_u, control_byte] {
            let out = capture_capped_redacted(&raw, 128 * 1024, 4 * 1024);
            let text = String::from_utf8_lossy(&out);
            assert!(
                !text.contains("SECRET"),
                "malformed string must not persist the secret: input={:?} out={text}",
                String::from_utf8_lossy(&raw)
            );
            assert!(
                text.starts_with("[redacted: unparseable body"),
                "malformed string → fixed marker: input={:?} out={text}",
                String::from_utf8_lossy(&raw)
            );
        }
    }

    #[test]
    fn valid_string_escapes_round_trip() {
        // The strict string scanner must still accept every well-formed escape
        // (`\n`, `\"`, `\\`, `\/`, a BMP `\uXXXX`, and a surrogate PAIR) plus
        // multibyte UTF-8 content, and preserve the decoded text.
        // `🚀` is U+1F680 ROCKET; `ꯍ` is U+ABCD (ꯍ).
        let body = r#"{"a":"line\nbreak \"quote\" \\slash\/ ꯍ ꯍ 🚀 café"}"#;
        let out = capture_capped_redacted(body.as_bytes(), 128 * 1024, 4 * 1024);
        let value: serde_json::Value =
            serde_json::from_slice(&out).expect("valid escapes preserved as JSON");
        let s = value["a"].as_str().expect("string");
        assert!(s.contains("line\nbreak"), "\\n decoded");
        assert!(s.contains("\"quote\""), "\\\" decoded");
        assert!(s.contains("\\slash"), "\\\\ decoded");
        assert!(s.contains('ꯍ'), "multibyte content preserved");
        assert!(s.contains('\u{ABCD}'), "BMP \\u decoded");
        assert!(s.contains('\u{1F680}'), "surrogate-pair rocket decoded");
        assert!(s.contains("café"), "trailing multibyte preserved");
    }

    #[test]
    fn redact_headers_capped_redacts_sensitive_and_uri_values() {
        use axum::http::HeaderMap;
        use axum::http::HeaderName;
        use axum::http::HeaderValue;
        let mut headers = HeaderMap::new();
        headers.insert(
            HeaderName::from_static("authorization"),
            HeaderValue::from_static("Bearer HDRLEAK"),
        );
        headers.insert(
            HeaderName::from_static("openai-beta"),
            HeaderValue::from_static("tok=BETALEAK"),
        );
        headers.insert(
            HeaderName::from_static("x-cb"),
            HeaderValue::from_static("https://x/y?sig=URLLEAK"),
        );
        let out = redact_headers_capped(&headers, 4 * 1024);
        let dumped = format!("{out:?}");
        assert!(!dumped.contains("HDRLEAK"));
        assert!(!dumped.contains("BETALEAK"), "openai-beta value redacted");
        assert!(
            !dumped.contains("URLLEAK"),
            "uri-bearing header value redacted"
        );
    }

    #[test]
    fn redact_vision_text_strips_data_uris_and_caps_length() {
        let body = "error before data:image/png;base64,AAAABBBBCCCC after";
        let redacted = redact_vision_text(body);
        assert!(
            !redacted.contains("AAAABBBBCCCC"),
            "base64 payload stripped"
        );
        assert!(!redacted.contains("data:image"), "data uri stripped");
        assert!(redacted.contains("<redacted uri>"));
        assert!(redacted.contains("error before"));
        assert!(redacted.contains("after"));

        // UTF-8-safe length cap (limit + the truncation marker).
        let long = "x".repeat(VISION_TEXT_REDACT_LIMIT + 500);
        let capped = redact_vision_text(&long);
        assert!(capped.ends_with("…[truncated]"));
        assert!(
            capped.chars().count() <= VISION_TEXT_REDACT_LIMIT + "…[truncated]".chars().count()
        );
        // Truncation lands on a char boundary even with multi-byte content.
        let multibyte = "é".repeat(VISION_TEXT_REDACT_LIMIT + 10);
        let capped_mb = redact_vision_text(&multibyte);
        assert!(capped_mb.is_char_boundary(capped_mb.len()));

        // A JSON error message embedding a data URL inside quotes is bounded.
        let json_err = "{\"detail\":\"bad input data:image/jpeg;base64,ZZZZ\"}";
        let r = redact_vision_text(json_err);
        assert!(!r.contains("ZZZZ"));
    }

    #[test]
    fn redact_vision_text_is_case_insensitive_and_strips_http_image_urls() {
        // Round-2 #2: uppercase DATA: and http(s) image URLs with signed-URL
        // query tokens must all be redacted.
        let upper = "oops DATA:IMAGE/PNG;BASE64,SECRETPAYLOAD trailing";
        let r = redact_vision_text(upper);
        assert!(!r.contains("SECRETPAYLOAD"), "uppercase data: stripped");
        assert!(r.contains("<redacted uri>"));
        assert!(r.contains("trailing"));

        let signed =
            "fetch failed for https://cdn.example.com/img.png?sig=ABCSECRET123&exp=999 oh no";
        let r = redact_vision_text(signed);
        assert!(!r.contains("ABCSECRET123"), "signed-url token stripped");
        assert!(!r.contains("cdn.example.com"), "image host stripped");
        assert!(r.contains("fetch failed for"));
        assert!(r.contains("oh no"));

        let mixed_case_http = "HTTPS://Host/Path?token=ZZZ done";
        let r = redact_vision_text(mixed_case_http);
        assert!(!r.contains("ZZZ"), "uppercase https stripped");
        assert!(r.contains("done"));
    }

    #[test]
    fn redact_vision_text_strips_json_escaped_signed_urls() {
        // Round-3 #4: a raw non-2xx body often contains JSON-escaped slashes
        // (`https:\/\/...`); the escaped form must be redacted too.
        let escaped =
            r#"{"error":"could not load https:\/\/cdn.example.com\/i.png?sig=ESCAPEDTOKEN&x=1"}"#;
        let r = redact_vision_text(escaped);
        assert!(
            !r.contains("ESCAPEDTOKEN"),
            "escaped signed-url token stripped"
        );
        assert!(
            !r.contains("cdn.example.com"),
            "escaped image host stripped"
        );
        assert!(r.contains("<redacted uri>"));
        assert!(r.contains("could not load"));

        // Escaped http (no TLS) too, uppercase scheme.
        let escaped_http = r#"HTTP:\/\/host\/p?tok=ABC end"#;
        let r = redact_vision_text(escaped_http);
        assert!(!r.contains("ABC"), "escaped http token stripped");
        assert!(r.contains("end"));

        // A successful description echoing a data URL is redacted the same way.
        let success = "Here is your image data:image/png;base64,REALPAYLOAD and analysis.";
        let r = redact_vision_text(success);
        assert!(!r.contains("REALPAYLOAD"));
        assert!(r.contains("and analysis."));
    }

    #[test]
    fn redact_image_uris_in_value_strips_nested_image_fields() {
        // Round-4 #2/#3: the shared JSON redactor used by inbound trace + upstream
        // JSONL must strip data:/signed image URLs anywhere in the body —
        // including nested content-part arrays and object fields — by VALUE, not
        // by key name, leaving non-image content intact.
        let mut value = serde_json::json!({
            "model": "glm-5.1",
            "messages": [{
                "role": "user",
                "content": [
                    { "type": "text", "text": "keep this prose" },
                    { "type": "image_url", "image_url": { "url": "data:image/png;base64,NESTEDLEAK" } },
                    { "type": "image_url", "image_url": { "url": "https://cdn.x/i.png?sig=SIGLEAK" } }
                ]
            }],
            "note": "see https:\\/\\/cdn.x\\/e.png?tok=ESCLEAK now"
        });
        redact_image_uris_in_value(&mut value);
        let dumped = serde_json::to_string(&value).expect("serialize");
        assert!(
            !dumped.contains("NESTEDLEAK"),
            "nested data: payload stripped"
        );
        assert!(
            !dumped.contains("SIGLEAK"),
            "nested signed-url token stripped"
        );
        assert!(
            !dumped.contains("ESCLEAK"),
            "escaped signed-url token stripped"
        );
        assert!(!dumped.contains("cdn.x"), "image host stripped");
        assert!(
            dumped.contains("keep this prose"),
            "non-image text preserved"
        );
        assert!(dumped.contains("glm-5.1"), "model preserved");
        assert!(dumped.contains("<redacted uri>"));
    }

    #[test]
    fn redact_image_uris_handles_multibyte_around_uris() {
        // Round-5: the core redactor walks UNTRUSTED text; multibyte chars
        // adjacent to / before / inside a URI must not panic and must preserve
        // non-image content. `é`/`☕`/`café` straddle byte boundaries near the
        // `data:`/`http` scan points.
        let text = "café ☕ before data:image/png;base64,PAYLÖAD/é+= after — déjà vu";
        let r = redact_image_uris(text);
        assert!(
            !r.contains("PAYL"),
            "base64 payload (with multibyte) stripped"
        );
        assert!(r.contains("<redacted uri>"));
        assert!(r.contains("café ☕ before "));
        assert!(r.contains(" after — déjà vu"));

        // A multibyte run immediately preceding `https://` must not corrupt the
        // boundary handling.
        let text2 = "señor https://hôst.x/p?tok=ZZZé done ☕";
        let r2 = redact_image_uris(text2);
        assert!(!r2.contains("ZZZ"), "signed-url token stripped");
        assert!(r2.contains("señor "));
        assert!(r2.contains(" done ☕"));

        // Pure multibyte with no URI is returned intact (no panic, no change).
        let plain = "完全に日本語のテキスト ☕ café";
        assert_eq!(redact_image_uris(plain), plain);
    }
}

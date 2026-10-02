use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use hmac::{Hmac, Mac};
use sha2::Sha256;
use std::fmt;
use subtle::ConstantTimeEq;

pub const API_KEY_PREFIX: &str = "llmc_";
const DISPLAY_CHARS: usize = 12;

#[derive(Clone)]
pub struct AuthPepper(Vec<u8>);

impl AuthPepper {
    pub fn new(value: impl AsRef<[u8]>) -> Result<Self, KeyError> {
        let value = value.as_ref();
        if value.is_empty() {
            return Err(KeyError::MissingPepper);
        }
        Ok(Self(value.to_vec()))
    }

    pub fn digest(&self, raw_key: &str) -> [u8; 32] {
        let mut mac = Hmac::<Sha256>::new_from_slice(&self.0)
            .expect("HMAC-SHA256 accepts keys of every length");
        mac.update(raw_key.as_bytes());
        mac.finalize().into_bytes().into()
    }
}

impl fmt::Debug for AuthPepper {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("AuthPepper([REDACTED])")
    }
}

pub struct GeneratedApiKey {
    raw: String,
    pub prefix: String,
    pub digest: [u8; 32],
}

impl GeneratedApiKey {
    pub fn expose_once(self) -> String {
        self.raw
    }

    #[cfg(test)]
    pub(crate) fn raw(&self) -> &str {
        &self.raw
    }
}

impl fmt::Debug for GeneratedApiKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("GeneratedApiKey")
            .field("raw", &"[REDACTED]")
            .field("prefix", &self.prefix)
            .field("digest", &"[REDACTED]")
            .finish()
    }
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum KeyError {
    #[error("LLMCONDUIT_AUTH_PEPPER must not be empty")]
    MissingPepper,
    #[error("malformed API key")]
    Malformed,
}

pub fn generate_api_key(pepper: &AuthPepper) -> GeneratedApiKey {
    let secret = iroh::SecretKey::generate().to_bytes();
    let raw = format!("{API_KEY_PREFIX}{}", URL_SAFE_NO_PAD.encode(secret));
    let prefix = raw[..DISPLAY_CHARS.min(raw.len())].to_string();
    let digest = pepper.digest(&raw);
    GeneratedApiKey {
        raw,
        prefix,
        digest,
    }
}

pub fn key_prefix(raw: &str) -> Result<&str, KeyError> {
    let raw = raw.trim();
    if !raw.starts_with(API_KEY_PREFIX)
        || raw.len() < DISPLAY_CHARS
        || !raw[API_KEY_PREFIX.len()..]
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
    {
        return Err(KeyError::Malformed);
    }
    Ok(&raw[..DISPLAY_CHARS])
}

pub fn digest_matches(expected: &[u8], actual: &[u8; 32]) -> bool {
    let mut padded = [0u8; 32];
    let copy_len = expected.len().min(padded.len());
    padded[..copy_len].copy_from_slice(&expected[..copy_len]);
    let length_matches = (expected.len() as u64).ct_eq(&32u64);
    bool::from(padded.ct_eq(actual) & length_matches)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generated_key_is_high_entropy_and_debug_redacted() {
        let pepper = AuthPepper::new("test-pepper").unwrap();
        let generated = generate_api_key(&pepper);
        let raw = generated.raw().to_string();
        assert!(raw.starts_with(API_KEY_PREFIX));
        assert!(raw.len() >= 45);
        let debug = format!("{generated:?}");
        assert!(!debug.contains(&raw));
        assert!(!debug.contains(&hex::encode(generated.digest)));
        assert!(digest_matches(&generated.digest, &pepper.digest(&raw)));
    }

    #[test]
    fn malformed_keys_are_rejected() {
        for raw in ["", " ", "llmc_", "bearer llmc_abc", "llmc_not valid"] {
            assert_eq!(key_prefix(raw), Err(KeyError::Malformed));
        }
    }
}

use chrono::{DateTime, Datelike, Timelike, Utc};
use regex::Regex;
use std::collections::{HashMap, HashSet};
use std::fmt;
use std::sync::Arc;
use uuid::Uuid;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct AuthRequestId(String);

impl AuthRequestId {
    pub fn new() -> Self {
        Self(format!("authreq_{}", Uuid::new_v4().simple()))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl Default for AuthRequestId {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PolicyIdentity {
    pub request_id: AuthRequestId,
    pub key_id: String,
    pub key_prefix: String,
    pub principal_id: String,
    pub policy_epoch: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Endpoint {
    Responses,
    ChatCompletions,
    Messages,
    CountTokens,
    Completions,
    Models,
}

impl Endpoint {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Responses => "responses",
            Self::ChatCompletions => "chat",
            Self::Messages => "messages",
            Self::CountTokens => "count_tokens",
            Self::Completions => "completions",
            Self::Models => "models",
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "responses" => Some(Self::Responses),
            "chat" | "chat_completions" => Some(Self::ChatCompletions),
            "messages" => Some(Self::Messages),
            "count_tokens" => Some(Self::CountTokens),
            "completions" => Some(Self::Completions),
            "models" => Some(Self::Models),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PolicyEffect {
    Allow,
    Deny,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum PolicySubject {
    Key(String),
    Principal(String),
    Group(String),
    Role(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PolicyBinding {
    pub subject: PolicySubject,
}

#[derive(Clone)]
pub struct PolicyMatcher {
    pub endpoints: HashSet<Endpoint>,
    requested_models: Vec<NameMatcher>,
    served_models: Vec<NameMatcher>,
    providers: Vec<NameMatcher>,
    routes: Vec<NameMatcher>,
}

impl fmt::Debug for PolicyMatcher {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PolicyMatcher")
            .field("endpoints", &self.endpoints)
            .field("requested_models", &self.requested_models)
            .field("served_models", &self.served_models)
            .field("providers", &self.providers)
            .field("routes", &self.routes)
            .finish()
    }
}

impl PolicyMatcher {
    pub fn new(
        endpoints: impl IntoIterator<Item = Endpoint>,
        requested_models: impl IntoIterator<Item = String>,
        served_models: impl IntoIterator<Item = String>,
        providers: impl IntoIterator<Item = String>,
        routes: impl IntoIterator<Item = String>,
    ) -> Result<Self, AuthError> {
        Ok(Self {
            endpoints: endpoints.into_iter().collect(),
            requested_models: compile_names(requested_models)?,
            served_models: compile_names(served_models)?,
            providers: compile_names(providers)?,
            routes: compile_names(routes)?,
        })
    }

    pub fn all() -> Self {
        Self {
            endpoints: HashSet::new(),
            requested_models: Vec::new(),
            served_models: Vec::new(),
            providers: Vec::new(),
            routes: Vec::new(),
        }
    }

    fn matches_coarse(&self, endpoint: Endpoint, requested_model: Option<&str>) -> bool {
        matches_set(&self.endpoints, &endpoint)
            && matches_names(&self.requested_models, requested_model)
    }

    fn matches_endpoint(&self, endpoint: Endpoint) -> bool {
        matches_set(&self.endpoints, &endpoint)
    }

    fn is_endpoint_wide(&self) -> bool {
        self.requested_models.is_empty()
            && self.served_models.is_empty()
            && self.providers.is_empty()
            && self.routes.is_empty()
    }

    fn matches_candidate(
        &self,
        endpoint: Endpoint,
        requested_model: Option<&str>,
        provider: Option<&str>,
        route: Option<&str>,
        served_model: Option<&str>,
    ) -> bool {
        self.matches_coarse(endpoint, requested_model)
            && matches_names(&self.served_models, served_model)
            && matches_names(&self.providers, provider)
            && matches_names(&self.routes, route)
    }

    fn is_candidate_independent(&self) -> bool {
        self.served_models.is_empty() && self.providers.is_empty() && self.routes.is_empty()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ManagementPermission {
    KeysRead,
    KeysCreate,
    KeysRevoke,
    KeysRotate,
    PrincipalsRead,
    PrincipalsWrite,
    GroupsRead,
    GroupsWrite,
    RolesRead,
    RolesWrite,
    PoliciesRead,
    PoliciesWrite,
    UsageRead,
    AuditRead,
    PricingRead,
    PricingSync,
    PricingWrite,
    SessionsRead,
    SessionsTerminate,
    FleetModelsRead,
    FleetModelsLoad,
    FleetModelsUnload,
}

impl ManagementPermission {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::KeysRead => "auth.keys.read",
            Self::KeysCreate => "auth.keys.create",
            Self::KeysRevoke => "auth.keys.revoke",
            Self::KeysRotate => "auth.keys.rotate",
            Self::PrincipalsRead => "auth.principals.read",
            Self::PrincipalsWrite => "auth.principals.write",
            Self::GroupsRead => "auth.groups.read",
            Self::GroupsWrite => "auth.groups.write",
            Self::RolesRead => "auth.roles.read",
            Self::RolesWrite => "auth.roles.write",
            Self::PoliciesRead => "auth.policies.read",
            Self::PoliciesWrite => "auth.policies.write",
            Self::UsageRead => "auth.usage.read",
            Self::AuditRead => "auth.audit.read",
            Self::PricingRead => "auth.pricing.read",
            Self::PricingSync => "auth.pricing.sync",
            Self::PricingWrite => "auth.pricing.write",
            Self::SessionsRead => "auth.sessions.read",
            Self::SessionsTerminate => "auth.sessions.terminate",
            Self::FleetModelsRead => "fleet.models.read",
            Self::FleetModelsLoad => "fleet.models.load",
            Self::FleetModelsUnload => "fleet.models.unload",
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        ALL_MANAGEMENT_PERMISSIONS
            .into_iter()
            .find(|permission| permission.as_str() == value)
    }
}

pub const ALL_MANAGEMENT_PERMISSIONS: [ManagementPermission; 22] = [
    ManagementPermission::KeysRead,
    ManagementPermission::KeysCreate,
    ManagementPermission::KeysRevoke,
    ManagementPermission::KeysRotate,
    ManagementPermission::PrincipalsRead,
    ManagementPermission::PrincipalsWrite,
    ManagementPermission::GroupsRead,
    ManagementPermission::GroupsWrite,
    ManagementPermission::RolesRead,
    ManagementPermission::RolesWrite,
    ManagementPermission::PoliciesRead,
    ManagementPermission::PoliciesWrite,
    ManagementPermission::UsageRead,
    ManagementPermission::AuditRead,
    ManagementPermission::PricingRead,
    ManagementPermission::PricingSync,
    ManagementPermission::PricingWrite,
    ManagementPermission::SessionsRead,
    ManagementPermission::SessionsTerminate,
    ManagementPermission::FleetModelsRead,
    ManagementPermission::FleetModelsLoad,
    ManagementPermission::FleetModelsUnload,
];

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct LimitSet {
    pub max_concurrent_sessions: Option<u32>,
    pub max_daily_session_starts: Option<u32>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UtcWindow {
    /// Bit 0 is Monday and bit 6 is Sunday. Zero means every weekday.
    pub weekday_mask: u8,
    pub start_minute: u16,
    pub end_minute: u16,
    pub absolute_start_ms: Option<i64>,
    pub absolute_end_ms: Option<i64>,
}

impl UtcWindow {
    pub fn contains(&self, now: DateTime<Utc>) -> bool {
        let timestamp = now.timestamp_millis();
        if self
            .absolute_start_ms
            .is_some_and(|start| timestamp < start)
            || self.absolute_end_ms.is_some_and(|end| timestamp >= end)
        {
            return false;
        }
        let weekday = now.weekday().num_days_from_monday() as u8;
        if self.weekday_mask != 0 && self.weekday_mask & (1 << weekday) == 0 {
            return false;
        }
        let minute = (now.hour() * 60 + now.minute()) as u16;
        if self.start_minute == self.end_minute {
            true
        } else if self.start_minute < self.end_minute {
            minute >= self.start_minute && minute < self.end_minute
        } else {
            minute >= self.start_minute || minute < self.end_minute
        }
    }
}

#[derive(Debug, Clone)]
pub struct PolicyRule {
    pub id: String,
    pub effect: PolicyEffect,
    pub binding: PolicyBinding,
    pub matcher: PolicyMatcher,
    pub windows: Vec<UtcWindow>,
    pub limits: LimitSet,
    pub management_permissions: HashSet<ManagementPermission>,
}

impl PolicyRule {
    fn active(&self, now: DateTime<Utc>) -> bool {
        self.windows.is_empty() || self.windows.iter().any(|window| window.contains(now))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PolicyDecision {
    Allow,
    Deny,
}

#[derive(Debug, Clone)]
pub struct PolicySnapshot {
    epoch: u64,
    rules: Arc<[PolicyRule]>,
    principal_groups: Arc<HashMap<String, HashSet<String>>>,
    principal_roles: Arc<HashMap<String, HashSet<String>>>,
}

impl PolicySnapshot {
    pub fn new(
        epoch: u64,
        rules: Vec<PolicyRule>,
        principal_groups: HashMap<String, HashSet<String>>,
        principal_roles: HashMap<String, HashSet<String>>,
    ) -> Self {
        Self {
            epoch,
            rules: rules.into(),
            principal_groups: Arc::new(principal_groups),
            principal_roles: Arc::new(principal_roles),
        }
    }

    pub fn epoch(&self) -> u64 {
        self.epoch
    }

    pub fn authorize(
        self: &Arc<Self>,
        context: &PolicyIdentity,
        endpoint: Endpoint,
        requested_model: Option<&str>,
        now: DateTime<Utc>,
    ) -> Result<PolicyScope, AuthError> {
        if context.policy_epoch != self.epoch {
            return Err(AuthError::PolicyUnavailable);
        }
        let applicable = self.applicable_rules(context, now);
        let coarse_deny = applicable.iter().any(|rule| {
            rule.effect == PolicyEffect::Deny
                && rule.matcher.is_candidate_independent()
                && rule.matcher.matches_coarse(endpoint, requested_model)
        });
        let possible_allow = applicable.iter().any(|rule| {
            rule.effect == PolicyEffect::Allow
                && rule.matcher.matches_coarse(endpoint, requested_model)
        });
        if coarse_deny || !possible_allow {
            return Err(AuthError::Forbidden);
        }
        Ok(PolicyScope {
            context: context.clone(),
            snapshot: Arc::clone(self),
            endpoint,
            requested_model: requested_model.map(str::to_owned),
            evaluated_at: now,
        })
    }

    /// Coarse middleware gate used before a request body/model is available.
    /// Model/provider/route-specific denies are deferred to the richer checks;
    /// only an endpoint-wide deny can reject at this stage.
    pub fn permits_endpoint(
        &self,
        context: &PolicyIdentity,
        endpoint: Endpoint,
        now: DateTime<Utc>,
    ) -> bool {
        if context.policy_epoch != self.epoch {
            return false;
        }
        let rules = self.applicable_rules(context, now);
        !rules.iter().any(|rule| {
            rule.effect == PolicyEffect::Deny
                && rule.matcher.is_endpoint_wide()
                && rule.matcher.matches_endpoint(endpoint)
        }) && rules.iter().any(|rule| {
            rule.effect == PolicyEffect::Allow && rule.matcher.matches_endpoint(endpoint)
        })
    }

    pub fn management_permissions(
        &self,
        context: &PolicyIdentity,
        now: DateTime<Utc>,
    ) -> HashSet<ManagementPermission> {
        let rules = self.applicable_rules(context, now);
        let denied: HashSet<_> = rules
            .iter()
            .filter(|rule| rule.effect == PolicyEffect::Deny)
            .flat_map(|rule| rule.management_permissions.iter().copied())
            .collect();
        rules
            .iter()
            .filter(|rule| rule.effect == PolicyEffect::Allow)
            .flat_map(|rule| rule.management_permissions.iter().copied())
            .filter(|permission| !denied.contains(permission))
            .collect()
    }

    pub fn effective_limits(&self, context: &PolicyIdentity, now: DateTime<Utc>) -> LimitSet {
        let rules = self.applicable_rules(context, now);
        LimitSet {
            max_concurrent_sessions: minimum_limit(
                rules
                    .iter()
                    .filter_map(|rule| rule.limits.max_concurrent_sessions),
            ),
            max_daily_session_starts: minimum_limit(
                rules
                    .iter()
                    .filter_map(|rule| rule.limits.max_daily_session_starts),
            ),
        }
    }

    fn applicable_rules<'a>(
        &'a self,
        context: &PolicyIdentity,
        now: DateTime<Utc>,
    ) -> Vec<&'a PolicyRule> {
        let groups = self.principal_groups.get(&context.principal_id);
        let roles = self.principal_roles.get(&context.principal_id);
        self.rules
            .iter()
            .filter(|rule| rule.active(now))
            .filter(|rule| match &rule.binding.subject {
                PolicySubject::Key(id) => id == &context.key_id,
                PolicySubject::Principal(id) => id == &context.principal_id,
                PolicySubject::Group(id) => groups.is_some_and(|groups| groups.contains(id)),
                PolicySubject::Role(id) => roles.is_some_and(|roles| roles.contains(id)),
            })
            .collect()
    }
}

#[derive(Debug, Clone)]
pub struct PolicyScope {
    context: PolicyIdentity,
    snapshot: Arc<PolicySnapshot>,
    endpoint: Endpoint,
    requested_model: Option<String>,
    evaluated_at: DateTime<Utc>,
}

impl PolicyScope {
    pub fn context(&self) -> &PolicyIdentity {
        &self.context
    }

    pub fn endpoint(&self) -> Endpoint {
        self.endpoint
    }

    pub fn allows_candidate(
        &self,
        provider_id: Option<&str>,
        route_id: Option<&str>,
        served_model: Option<&str>,
    ) -> bool {
        let rules = self
            .snapshot
            .applicable_rules(&self.context, self.evaluated_at);
        let matches = |rule: &&PolicyRule| {
            rule.matcher.matches_candidate(
                self.endpoint,
                self.requested_model.as_deref(),
                provider_id,
                route_id,
                served_model,
            )
        };
        !rules
            .iter()
            .any(|rule| rule.effect == PolicyEffect::Deny && matches(rule))
            && rules
                .iter()
                .any(|rule| rule.effect == PolicyEffect::Allow && matches(rule))
    }

    pub fn allows_provider(&self, provider_id: &str) -> bool {
        self.allows_candidate(Some(provider_id), None, None)
    }

    pub fn allows_route(&self, route_id: &str) -> bool {
        self.allows_candidate(None, Some(route_id), None)
    }

    pub fn allows_backend_model(&self, served_model: &str) -> bool {
        self.allows_candidate(None, None, Some(served_model))
    }

    pub fn effective_limits(&self) -> LimitSet {
        let rules = self
            .snapshot
            .applicable_rules(&self.context, self.evaluated_at);
        LimitSet {
            max_concurrent_sessions: minimum_limit(
                rules
                    .iter()
                    .filter_map(|rule| rule.limits.max_concurrent_sessions),
            ),
            max_daily_session_starts: minimum_limit(
                rules
                    .iter()
                    .filter_map(|rule| rule.limits.max_daily_session_starts),
            ),
        }
    }
}

#[derive(Debug, thiserror::Error, Clone, PartialEq, Eq)]
pub enum AuthError {
    #[error("authentication required")]
    MissingCredential,
    #[error("invalid API key")]
    InvalidCredential,
    #[error("access denied")]
    Forbidden,
    #[error("session quota exceeded")]
    QuotaExceeded,
    #[error("authorization policy unavailable")]
    PolicyUnavailable,
    #[error("invalid policy: {0}")]
    InvalidPolicy(String),
}

impl AuthError {
    pub fn status_code(&self) -> u16 {
        match self {
            Self::MissingCredential | Self::InvalidCredential => 401,
            Self::Forbidden | Self::QuotaExceeded | Self::PolicyUnavailable => 403,
            Self::InvalidPolicy(_) => 500,
        }
    }
}

#[derive(Clone)]
enum NameMatcher {
    Exact(String),
    Glob { source: String, regex: Regex },
}

impl NameMatcher {
    fn matches(&self, value: &str) -> bool {
        match self {
            Self::Exact(exact) => exact.eq_ignore_ascii_case(value),
            Self::Glob { regex, .. } => regex.is_match(value),
        }
    }
}

impl fmt::Debug for NameMatcher {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Exact(value) => f.debug_tuple("Exact").field(value).finish(),
            Self::Glob { source, .. } => f.debug_tuple("Glob").field(source).finish(),
        }
    }
}

fn compile_names(values: impl IntoIterator<Item = String>) -> Result<Vec<NameMatcher>, AuthError> {
    values
        .into_iter()
        .map(|value| {
            let value = value.trim().to_string();
            if value.is_empty() {
                return Err(AuthError::InvalidPolicy("blank matcher".to_string()));
            }
            if value.contains(['*', '?', '[']) {
                let regex = Regex::new(&glob_regex(&value)).map_err(|err| {
                    AuthError::InvalidPolicy(format!("invalid matcher {value:?}: {err}"))
                })?;
                Ok(NameMatcher::Glob {
                    source: value,
                    regex,
                })
            } else {
                Ok(NameMatcher::Exact(value))
            }
        })
        .collect()
}

fn glob_regex(pattern: &str) -> String {
    let mut regex = String::from("(?i)^");
    for ch in pattern.chars() {
        match ch {
            '*' => regex.push_str(".*"),
            '?' => regex.push('.'),
            _ => regex.push_str(&regex::escape(&ch.to_string())),
        }
    }
    regex.push('$');
    regex
}

fn matches_names(matchers: &[NameMatcher], value: Option<&str>) -> bool {
    if matchers.is_empty() {
        return true;
    }
    value
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .is_some_and(|value| matchers.iter().any(|matcher| matcher.matches(value)))
}

fn matches_set<T: Eq + std::hash::Hash>(set: &HashSet<T>, value: &T) -> bool {
    set.is_empty() || set.contains(value)
}

fn minimum_limit(values: impl Iterator<Item = u32>) -> Option<u32> {
    values.min()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn context(epoch: u64) -> PolicyIdentity {
        PolicyIdentity {
            request_id: AuthRequestId::new(),
            key_id: "key_a".into(),
            key_prefix: "llmc_example".into(),
            principal_id: "usr_a".into(),
            policy_epoch: epoch,
        }
    }

    fn rule(
        id: &str,
        effect: PolicyEffect,
        subject: PolicySubject,
        matcher: PolicyMatcher,
    ) -> PolicyRule {
        PolicyRule {
            id: id.into(),
            effect,
            binding: PolicyBinding { subject },
            matcher,
            windows: Vec::new(),
            limits: LimitSet::default(),
            management_permissions: HashSet::new(),
        }
    }

    #[test]
    fn explicit_deny_overrides_additive_allow() {
        let allow = rule(
            "allow",
            PolicyEffect::Allow,
            PolicySubject::Principal("usr_a".into()),
            PolicyMatcher::all(),
        );
        let deny = rule(
            "deny",
            PolicyEffect::Deny,
            PolicySubject::Key("key_a".into()),
            PolicyMatcher::all(),
        );
        let snapshot = Arc::new(PolicySnapshot::new(
            7,
            vec![allow, deny],
            HashMap::new(),
            HashMap::new(),
        ));
        assert_eq!(
            snapshot
                .authorize(&context(7), Endpoint::Responses, Some("gpt"), Utc::now())
                .unwrap_err(),
            AuthError::Forbidden
        );
    }

    #[test]
    fn candidate_filter_is_fail_closed_and_supports_globs() {
        let matcher = PolicyMatcher::new(
            [Endpoint::Responses],
            ["gpt-*".into()],
            ["openai/*".into()],
            ["primary".into()],
            Vec::<String>::new(),
        )
        .unwrap();
        let snapshot = Arc::new(PolicySnapshot::new(
            1,
            vec![rule(
                "allow",
                PolicyEffect::Allow,
                PolicySubject::Principal("usr_a".into()),
                matcher,
            )],
            HashMap::new(),
            HashMap::new(),
        ));
        let scope = snapshot
            .authorize(&context(1), Endpoint::Responses, Some("GPT-4o"), Utc::now())
            .unwrap();
        assert!(scope.allows_candidate(Some("PRIMARY"), None, Some("openai/gpt-4o")));
        assert!(!scope.allows_candidate(Some("fallback"), None, Some("openai/gpt-4o")));
        assert!(!scope.allows_candidate(Some("primary"), None, Some("other/model")));
    }

    #[test]
    fn stale_snapshot_context_is_denied() {
        let snapshot = Arc::new(PolicySnapshot::new(
            2,
            Vec::new(),
            HashMap::new(),
            HashMap::new(),
        ));
        assert_eq!(
            snapshot
                .authorize(&context(1), Endpoint::Models, None, Utc::now())
                .unwrap_err(),
            AuthError::PolicyUnavailable
        );
    }

    #[test]
    fn management_permission_registry_is_complete_and_unique() {
        let unique = ALL_MANAGEMENT_PERMISSIONS
            .into_iter()
            .collect::<HashSet<_>>();
        assert_eq!(ALL_MANAGEMENT_PERMISSIONS.len(), 22);
        assert_eq!(unique.len(), ALL_MANAGEMENT_PERMISSIONS.len());
        assert!(ALL_MANAGEMENT_PERMISSIONS.into_iter().all(
            |permission| ManagementPermission::parse(permission.as_str()) == Some(permission)
        ));
    }

    #[test]
    fn requested_and_served_model_dimensions_are_independent() {
        let matcher = PolicyMatcher::new(
            [Endpoint::ChatCompletions],
            ["public-alias".into()],
            ["backend/model-v2".into()],
            ["provider-a".into()],
            Vec::<String>::new(),
        )
        .unwrap();
        let snapshot = Arc::new(PolicySnapshot::new(
            1,
            vec![rule(
                "allow-remap",
                PolicyEffect::Allow,
                PolicySubject::Principal("usr_a".into()),
                matcher,
            )],
            HashMap::new(),
            HashMap::new(),
        ));
        let scope = snapshot
            .authorize(
                &context(1),
                Endpoint::ChatCompletions,
                Some("public-alias"),
                Utc::now(),
            )
            .unwrap();
        assert!(scope.allows_candidate(Some("provider-a"), None, Some("backend/model-v2")));
        assert!(!scope.allows_candidate(Some("provider-a"), None, Some("public-alias")));
    }

    #[test]
    fn utc_windows_gate_rules_and_effective_limits_choose_the_strictest() {
        let now = Utc::now();
        let mut expired = rule(
            "expired",
            PolicyEffect::Allow,
            PolicySubject::Principal("usr_a".into()),
            PolicyMatcher::all(),
        );
        expired.windows.push(UtcWindow {
            weekday_mask: 0,
            start_minute: 0,
            end_minute: 0,
            absolute_start_ms: None,
            absolute_end_ms: Some(now.timestamp_millis() - 1),
        });
        let expired_snapshot = Arc::new(PolicySnapshot::new(
            1,
            vec![expired],
            HashMap::new(),
            HashMap::new(),
        ));
        assert_eq!(
            expired_snapshot
                .authorize(&context(1), Endpoint::Responses, Some("model"), now)
                .unwrap_err(),
            AuthError::Forbidden
        );

        let mut first = rule(
            "first",
            PolicyEffect::Allow,
            PolicySubject::Principal("usr_a".into()),
            PolicyMatcher::all(),
        );
        first.limits = LimitSet {
            max_concurrent_sessions: Some(5),
            max_daily_session_starts: Some(20),
        };
        let mut second = first.clone();
        second.id = "second".into();
        second.limits = LimitSet {
            max_concurrent_sessions: Some(2),
            max_daily_session_starts: Some(10),
        };
        let snapshot = Arc::new(PolicySnapshot::new(
            1,
            vec![first, second],
            HashMap::new(),
            HashMap::new(),
        ));
        assert_eq!(
            snapshot.effective_limits(&context(1), now),
            LimitSet {
                max_concurrent_sessions: Some(2),
                max_daily_session_starts: Some(10),
            }
        );
    }
}

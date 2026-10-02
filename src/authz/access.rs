use super::{AuthzService, ManagementPermission, keys::generate_api_key};
use crate::dashboard_access::{
    AccessApiKey, AccessBackend, AccessCounts, AccessError, AccessFuture, AccessOperation,
    AccessResult, AccessSummary, ActorSummary, CreatedAccessApiKey, ManagementActor,
    ManagementPermission as WirePermission,
};
use axum::http::StatusCode;
use chrono::{DateTime, Utc};

impl AccessBackend for AuthzService {
    fn dispatch<'a>(
        &'a self,
        actor: &'a ManagementActor,
        operation: AccessOperation,
    ) -> AccessFuture<'a> {
        let service = self.clone();
        let actor = actor.clone();
        match operation {
            AccessOperation::SyncPricing(request) => {
                Box::pin(async move { service.sync_openrouter_pricing(actor, request).await })
            }
            operation => Box::pin(async move {
                tokio::task::spawn_blocking(move || service.dispatch_access(&actor, operation))
                    .await
                    .map_err(|error| internal(format!("auth management worker failed: {error}")))?
            }),
        }
    }
}

impl AuthzService {
    fn dispatch_access(
        &self,
        actor: &ManagementActor,
        operation: AccessOperation,
    ) -> Result<AccessResult, AccessError> {
        let inner = match self.inner() {
            Ok(inner) => inner,
            Err(message) if message == "inference auth is disabled" => {
                return disabled_access_result(actor, operation);
            }
            Err(message) => return Err(internal(message)),
        };
        let mut store = inner
            .store
            .lock()
            .map_err(|_| internal("auth store lock poisoned".into()))?;
        let mutates = matches!(
            operation,
            AccessOperation::CreateUser(_)
                | AccessOperation::CreateGroup(_)
                | AccessOperation::CreateRole(_)
                | AccessOperation::CreatePolicy(_)
                | AccessOperation::CreateApiKey(_)
                | AccessOperation::RevokeApiKey(_)
                | AccessOperation::RotateApiKey(_)
                | AccessOperation::UpdateApiKeyPayloadCapture(_, _)
                | AccessOperation::RevokeSession(_)
                | AccessOperation::WritePricing(_)
        );
        let changes_pricing = matches!(operation, AccessOperation::WritePricing(_));
        let result = match operation {
            AccessOperation::CreateApiKey(body) => {
                let expires_at = body
                    .expires_at
                    .as_deref()
                    .map(parse_timestamp)
                    .transpose()?;
                let generated = generate_api_key(&inner.pepper);
                let raw = generated.expose_once();
                let digest = inner.pepper.digest(&raw);
                let created = store
                    .create_key_for_principal(
                        &body.principal_id,
                        &body.name,
                        &raw,
                        &digest,
                        expires_at,
                        body.capture_payloads,
                        actor,
                    )
                    .map_err(internal)?;
                Ok(AccessResult::CreatedApiKey(created_access(created, raw)))
            }
            AccessOperation::RotateApiKey(id) => {
                let generated = generate_api_key(&inner.pepper);
                let raw = generated.expose_once();
                let digest = inner.pepper.digest(&raw);
                let created = store
                    .rotate_key(&id, &raw, &digest, actor)
                    .map_err(internal)?;
                Ok(AccessResult::CreatedApiKey(created_access(created, raw)))
            }
            AccessOperation::UpdateApiKeyPayloadCapture(id, capture_payloads) => {
                if !store
                    .set_key_payload_capture(&id, capture_payloads, actor)
                    .map_err(internal)?
                {
                    return Err(AccessError::new(StatusCode::NOT_FOUND, "API key not found"));
                }
                store.list_access_keys().map(AccessResult::ApiKeys)
            }
            operation => store.dispatch_access(actor, operation),
        }?;
        if mutates {
            self.reload_locked(inner, &store).map_err(internal)?;
        }
        if changes_pricing {
            self.refresh_effective_prices_locked(inner, &store)
                .map_err(internal)?;
        }
        Ok(result)
    }
}

fn disabled_access_result(
    actor: &ManagementActor,
    operation: AccessOperation,
) -> Result<AccessResult, AccessError> {
    let empty = match operation {
        AccessOperation::Summary => AccessResult::Summary(AccessSummary {
            enabled: false,
            policy_epoch: 0,
            actor: ActorSummary {
                kind: match actor {
                    ManagementActor::Bootstrap => "bootstrap",
                    ManagementActor::Delegated { .. } => "delegated",
                }
                .into(),
                principal_id: match actor {
                    ManagementActor::Bootstrap => None,
                    ManagementActor::Delegated { principal_id, .. } => Some(principal_id.clone()),
                },
                display_name: "Authorization disabled".into(),
                permissions: Vec::new(),
            },
            counts: AccessCounts::default(),
        }),
        AccessOperation::ListUsers => AccessResult::Users(Vec::new()),
        AccessOperation::ListGroups => AccessResult::Groups(Vec::new()),
        AccessOperation::ListRoles => AccessResult::Roles(Vec::new()),
        AccessOperation::ListPolicies => AccessResult::Policies(Vec::new()),
        AccessOperation::ListApiKeys => AccessResult::ApiKeys(Vec::new()),
        AccessOperation::ListSessions => AccessResult::Sessions(Vec::new()),
        AccessOperation::Usage => AccessResult::Usage(Vec::new()),
        AccessOperation::Audit => AccessResult::Audit(Vec::new()),
        AccessOperation::Pricing => AccessResult::Pricing(Vec::new()),
        _ => {
            return Err(AccessError::new(
                StatusCode::CONFLICT,
                "enable inference authorization before changing access control",
            ));
        }
    };
    Ok(empty)
}

fn created_access(created: super::CreatedApiKey, raw_key: String) -> CreatedAccessApiKey {
    let key = created.summary;
    CreatedAccessApiKey {
        api_key: AccessApiKey {
            id: key.id,
            principal_id: key.principal_id,
            name: key.name,
            prefix: key.prefix,
            enabled: key.enabled,
            created_at: timestamp(key.created_at),
            expires_at: key.expires_at.map(timestamp),
            last_used_at: key.last_used_at.map(timestamp),
            capture_payloads: key.capture_payloads,
        },
        raw_key,
    }
}

fn parse_timestamp(value: &str) -> Result<i64, AccessError> {
    DateTime::parse_from_rfc3339(value)
        .map(|value| value.timestamp())
        .map_err(|_| {
            AccessError::new(
                StatusCode::UNPROCESSABLE_ENTITY,
                "expires_at must be an RFC 3339 timestamp",
            )
        })
}

fn timestamp(seconds: i64) -> String {
    DateTime::<Utc>::from_timestamp(seconds, 0)
        .map(|value| value.to_rfc3339())
        .unwrap_or_else(|| seconds.to_string())
}

fn internal(message: String) -> AccessError {
    tracing::error!(error = %message, "authorization management operation failed");
    AccessError::new(StatusCode::INTERNAL_SERVER_ERROR, "internal server error")
}

pub(crate) fn wire_permission(permission: ManagementPermission) -> Option<WirePermission> {
    Some(match permission {
        ManagementPermission::KeysRead => WirePermission::KeysRead,
        ManagementPermission::KeysCreate => WirePermission::KeysCreate,
        ManagementPermission::KeysRevoke => WirePermission::KeysRevoke,
        ManagementPermission::KeysRotate => WirePermission::KeysRotate,
        ManagementPermission::PrincipalsRead => WirePermission::PrincipalsRead,
        ManagementPermission::PrincipalsWrite => WirePermission::PrincipalsWrite,
        ManagementPermission::GroupsRead => WirePermission::GroupsRead,
        ManagementPermission::GroupsWrite => WirePermission::GroupsWrite,
        ManagementPermission::RolesRead => WirePermission::RolesRead,
        ManagementPermission::RolesWrite => WirePermission::RolesWrite,
        ManagementPermission::PoliciesRead => WirePermission::PoliciesRead,
        ManagementPermission::PoliciesWrite => WirePermission::PoliciesWrite,
        ManagementPermission::UsageRead => WirePermission::UsageRead,
        ManagementPermission::AuditRead => WirePermission::AuditRead,
        ManagementPermission::PricingRead => WirePermission::PricingRead,
        ManagementPermission::PricingSync => WirePermission::PricingSync,
        ManagementPermission::PricingWrite => WirePermission::PricingWrite,
        ManagementPermission::SessionsRead => WirePermission::SessionsRead,
        ManagementPermission::SessionsTerminate => WirePermission::SessionsTerminate,
        ManagementPermission::FleetModelsRead => WirePermission::FleetModelsRead,
        ManagementPermission::FleetModelsLoad => WirePermission::FleetModelsLoad,
        ManagementPermission::FleetModelsUnload => WirePermission::FleetModelsUnload,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{AuthConfig, AuthMode};
    use crate::dashboard_access::{
        AccessOperation, AccessPricingInput, AccessResult, AccessTimeWindow, CreateApiKeyRequest,
        CreateGroupRequest, CreatePolicyRequest, CreateRoleRequest, CreateUserRequest,
        WritePricingRequest,
    };
    use axum::http::{HeaderMap, HeaderValue};
    use axum::response::IntoResponse;

    #[test]
    fn disabled_auth_management_returns_an_explicit_empty_read_model() {
        let result = AuthzService::default()
            .dispatch_access(&ManagementActor::Bootstrap, AccessOperation::ListPolicies)
            .expect("disabled auth should remain inspectable");
        assert!(matches!(result, AccessResult::Policies(policies) if policies.is_empty()));

        let error = AuthzService::default()
            .dispatch_access(
                &ManagementActor::Bootstrap,
                AccessOperation::CreateUser(CreateUserRequest {
                    display_name: "blocked".into(),
                    kind: "user".into(),
                }),
            )
            .expect_err("disabled auth must reject mutations");
        assert_eq!(error.into_response().status(), StatusCode::CONFLICT);
    }

    #[tokio::test]
    async fn every_access_operation_is_backed_by_live_storage() {
        let path = std::env::temp_dir().join(format!(
            "llmconduit-access-{}.sqlite3",
            uuid::Uuid::new_v4()
        ));
        let service = AuthzService::open_enforced(
            &AuthConfig {
                mode: AuthMode::Enforce,
                store_path: path.clone(),
            },
            b"access-test-pepper".to_vec(),
            Some(&format!("llmc_{}", uuid::Uuid::new_v4().simple())),
        )
        .unwrap();
        let actor = ManagementActor::Bootstrap;

        assert!(matches!(
            service.dispatch_access(&actor, AccessOperation::Summary),
            Ok(AccessResult::Summary(_))
        ));
        let users = service
            .dispatch_access(
                &actor,
                AccessOperation::CreateUser(CreateUserRequest {
                    display_name: "agent".into(),
                    kind: "service_account".into(),
                }),
            )
            .unwrap();
        let principal_id = match users {
            AccessResult::Users(users) => {
                users
                    .into_iter()
                    .find(|user| user.display_name == "agent")
                    .unwrap()
                    .id
            }
            _ => panic!("unexpected user result"),
        };
        assert!(matches!(
            service.dispatch_access(
                &actor,
                AccessOperation::CreateGroup(CreateGroupRequest {
                    name: "operators".into(),
                    members: vec![principal_id.clone()],
                })
            ),
            Ok(AccessResult::Groups(_))
        ));
        let constrained_actor = ManagementActor::Delegated {
            session_id: "session_limited".into(),
            principal_id: principal_id.clone(),
            key_id: "key_limited".into(),
            permissions: vec![WirePermission::RolesWrite, WirePermission::PoliciesWrite].into(),
        };
        assert!(
            service
                .dispatch_access(
                    &constrained_actor,
                    AccessOperation::CreateRole(CreateRoleRequest {
                        name: "escalated".into(),
                        permissions: vec![WirePermission::KeysRead],
                    })
                )
                .is_err()
        );
        assert!(matches!(
            service.dispatch_access(
                &actor,
                AccessOperation::CreateRole(CreateRoleRequest {
                    name: "reader".into(),
                    permissions: vec![WirePermission::KeysRead],
                })
            ),
            Ok(AccessResult::Roles(_))
        ));
        assert!(
            service
                .dispatch_access(
                    &constrained_actor,
                    AccessOperation::CreatePolicy(Box::new(CreatePolicyRequest {
                        name: "escalated policy".into(),
                        effect: "allow".into(),
                        subjects: vec![format!("principal:{principal_id}")],
                        endpoints: vec!["chat".into()],
                        models: vec!["*".into()],
                        requested_models: Vec::new(),
                        served_models: Vec::new(),
                        providers: Vec::new(),
                        routes: Vec::new(),
                        time_windows: Vec::new(),
                        max_concurrent_sessions: None,
                        max_daily_session_starts: None,
                        management_permissions: Vec::new(),
                    }))
                )
                .is_err()
        );
        assert!(matches!(
            service.dispatch_access(
                &actor,
                AccessOperation::CreatePolicy(Box::new(CreatePolicyRequest {
                    name: "agent chat".into(),
                    effect: "allow".into(),
                    subjects: vec![format!("principal:{principal_id}")],
                    endpoints: vec!["chat".into()],
                    models: Vec::new(),
                    requested_models: vec!["public-*".into()],
                    served_models: vec!["backend-*".into()],
                    providers: vec!["provider-a".into()],
                    routes: Vec::new(),
                    time_windows: vec![AccessTimeWindow {
                        weekday_mask: 0,
                        start_minute: 0,
                        end_minute: 0,
                        absolute_start_ms: None,
                        absolute_end_ms: None,
                    }],
                    max_concurrent_sessions: Some(2),
                    max_daily_session_starts: Some(10),
                    management_permissions: Vec::new(),
                }))
            ),
            Ok(AccessResult::Policies(_))
        ));

        let created = service
            .dispatch_access(
                &actor,
                AccessOperation::CreateApiKey(CreateApiKeyRequest {
                    principal_id: principal_id.clone(),
                    name: "primary".into(),
                    expires_at: None,
                    capture_payloads: false,
                }),
            )
            .unwrap();
        let (key_id, raw) = match created {
            AccessResult::CreatedApiKey(created) => (created.api_key.id, created.raw_key),
            _ => panic!("unexpected create-key result"),
        };
        assert!(matches!(
            service.dispatch_access(&actor, AccessOperation::ListApiKeys),
            Ok(AccessResult::ApiKeys(_))
        ));

        let mut headers = HeaderMap::new();
        headers.insert("x-api-key", HeaderValue::from_str(&raw).unwrap());
        let context = service.authenticate(&headers).unwrap().unwrap();
        assert!(!context.capture_payloads());
        assert!(matches!(
            service.dispatch_access(
                &actor,
                AccessOperation::UpdateApiKeyPayloadCapture(key_id.clone(), true)
            ),
            Ok(AccessResult::ApiKeys(ref keys))
                if keys.iter().any(|key| key.id == key_id && key.capture_payloads)
        ));
        let context = service.authenticate(&headers).unwrap().unwrap();
        assert!(context.capture_payloads());
        assert!(context.allows_model("chat", "public-v1"));
        assert_eq!(context.effective_limits().max_concurrent_sessions, Some(2));
        assert!(matches!(
            service.dispatch_access(&actor, AccessOperation::ListPolicies),
            Ok(AccessResult::Policies(ref policies))
                if policies.iter().any(|policy| policy.time_windows.len() == 1
                    && policy.max_daily_session_starts == Some(10))
        ));
        let scope = context
            .authorization_scope(
                crate::upstream::InferenceEndpoint::ChatCompletions,
                "public-v1",
            )
            .unwrap();
        assert!(scope.allows_candidate(
            "provider-a",
            None,
            "backend-v2",
            crate::upstream::InferenceEndpoint::ChatCompletions
        ));
        assert!(!scope.allows_candidate(
            "provider-b",
            None,
            "backend-v2",
            crate::upstream::InferenceEndpoint::ChatCompletions
        ));
        let lease = service.acquire_session(&context).await.unwrap().unwrap();
        assert!(matches!(
            service.dispatch_access(&actor, AccessOperation::ListSessions),
            Ok(AccessResult::Sessions(ref sessions)) if sessions.iter().any(|session| session.id == lease.session_id())
        ));
        assert!(matches!(
            service.dispatch_access(
                &actor,
                AccessOperation::RevokeSession(lease.session_id().to_string())
            ),
            Ok(AccessResult::Sessions(_))
        ));
        drop(lease);

        let rotated = service
            .dispatch_access(&actor, AccessOperation::RotateApiKey(key_id))
            .unwrap();
        let rotated_id = match rotated {
            AccessResult::CreatedApiKey(created) => {
                assert!(created.api_key.capture_payloads);
                created.api_key.id
            }
            _ => panic!("unexpected rotate-key result"),
        };
        assert!(matches!(
            service.dispatch_access(&actor, AccessOperation::RevokeApiKey(rotated_id)),
            Ok(AccessResult::ApiKeys(_))
        ));
        assert!(matches!(
            service.dispatch_access(&actor, AccessOperation::Usage),
            Ok(AccessResult::Usage(_))
        ));
        assert!(matches!(
            service.dispatch_access(&actor, AccessOperation::Audit),
            Ok(AccessResult::Audit(ref events)) if !events.is_empty()
        ));
        let imported = crate::openrouter_pricing::OpenRouterPriceSnapshot {
            source_url:
                "https://openrouter.ai/api/v1/models/vendor/model-a/endpoints".to_string(),
            fetched_at_ms: 1_700_000_000_000,
            price: crate::openrouter_pricing::parse_endpoint_prices(
                br#"{"data":{"id":"model-a","endpoints":[{"tag":"provider-a","pricing":{"prompt":"0.000000003","completion":"0.000000004"}}]}}"#,
            )
            .unwrap(),
        };
        {
            let inner = service.inner().unwrap();
            let mut store = inner.store.lock().unwrap();
            store.persist_imported_price(&actor, &imported).unwrap();
            service
                .refresh_effective_prices_locked(inner, &store)
                .unwrap();
        }
        let effective = service.effective_price("model-a").unwrap();
        assert_eq!(effective.input_per_1k, 0.000003);
        assert_eq!(effective.output_per_1k, 0.000004);
        assert!(matches!(
            service.dispatch_access(
                &actor,
                AccessOperation::WritePricing(WritePricingRequest {
                    pricing: vec![AccessPricingInput {
                        model: "model-a".into(),
                        provider: "provider-a".into(),
                        input_per_1k: "0.001".into(),
                        output_per_1k: "0.002".into(),
                    }],
                })
            ),
            Ok(AccessResult::Pricing(ref prices)) if prices.len() == 2
        ));
        assert!(matches!(
            service.dispatch_access(&actor, AccessOperation::Pricing),
            Ok(AccessResult::Pricing(ref prices)) if prices.len() == 2
        ));
        let effective = service.effective_price("model-a").unwrap();
        assert_eq!(effective.input_per_1k, 0.001);
        assert_eq!(effective.output_per_1k, 0.002);

        drop(service);
        let _ = std::fs::remove_file(path);
    }
}

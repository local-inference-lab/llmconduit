use axum::body::{Body, to_bytes};
use axum::http::{HeaderMap, HeaderValue, Method, Request, StatusCode};
use llmconduit::authz::{AuthFailure, AuthzService};
use llmconduit::config::{AuthConfig, AuthMode};
use llmconduit::dashboard_access::{ManagementActor, ManagementPermission, routes};
use serde_json::Value;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use tower::ServiceExt;

static AUTH_ENV_LOCK: Mutex<()> = Mutex::new(());

fn enforced_service() -> (AuthzService, PathBuf) {
    let _guard = AUTH_ENV_LOCK.lock().expect("auth env lock");
    let path = std::env::temp_dir().join(format!(
        "llmconduit-management-lifecycle-{}.sqlite3",
        uuid::Uuid::new_v4()
    ));
    let bootstrap = format!("llmc_{}", uuid::Uuid::new_v4().simple());
    let old_pepper = std::env::var_os("LLMCONDUIT_AUTH_PEPPER");
    let old_bootstrap = std::env::var_os("LLMCONDUIT_AUTH_BOOTSTRAP_KEY");
    // SAFETY: this integration-test binary serializes its environment mutation and restores
    // both variables before releasing the lock. Production code never mutates these values.
    unsafe {
        std::env::set_var("LLMCONDUIT_AUTH_PEPPER", "management-test-pepper-never-log");
        std::env::set_var("LLMCONDUIT_AUTH_BOOTSTRAP_KEY", &bootstrap);
    }
    let service = AuthzService::from_config(&AuthConfig {
        mode: AuthMode::Enforce,
        store_path: path.clone(),
    })
    .expect("enforced auth service");
    // SAFETY: paired with the serialized mutation above.
    unsafe {
        match old_pepper {
            Some(value) => std::env::set_var("LLMCONDUIT_AUTH_PEPPER", value),
            None => std::env::remove_var("LLMCONDUIT_AUTH_PEPPER"),
        }
        match old_bootstrap {
            Some(value) => std::env::set_var("LLMCONDUIT_AUTH_BOOTSTRAP_KEY", value),
            None => std::env::remove_var("LLMCONDUIT_AUTH_BOOTSTRAP_KEY"),
        }
    }
    (service, path)
}

fn delegated(permission: ManagementPermission) -> ManagementActor {
    ManagementActor::Delegated {
        session_id: "session_test".into(),
        principal_id: "principal_admin".into(),
        key_id: "key_admin".into(),
        permissions: Arc::from([permission]),
    }
}

async fn json(response: axum::response::Response) -> Value {
    let bytes = to_bytes(response.into_body(), 64 * 1024)
        .await
        .expect("bounded response body");
    serde_json::from_slice(&bytes).expect("JSON response")
}

fn remove_store(path: &Path) {
    let _ = std::fs::remove_file(path);
    let _ = std::fs::remove_file(path.with_extension("sqlite3-wal"));
    let _ = std::fs::remove_file(path.with_extension("sqlite3-shm"));
}

fn assert_store_files_do_not_contain(path: &Path, secret: &str) {
    for candidate in [
        path.to_path_buf(),
        path.with_extension("sqlite3-wal"),
        path.with_extension("sqlite3-shm"),
    ] {
        let Ok(bytes) = std::fs::read(&candidate) else {
            continue;
        };
        assert!(
            !bytes
                .windows(secret.len())
                .any(|window| window == secret.as_bytes()),
            "raw key leaked into {}",
            candidate.display()
        );
    }
}

#[tokio::test]
async fn management_key_list_and_revoke_are_permissioned_and_never_recover_raw_secret() {
    let (service, path) = enforced_service();
    let service = Arc::new(service);
    let users = routes::<()>(service.access_backend())
        .layer(axum::extract::Extension(ManagementActor::Bootstrap))
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/dashboard/api/auth/users")
                .header("content-type", "application/json")
                .body(Body::from(
                    r#"{"display_name":"management lifecycle principal","kind":"service_account"}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(users.status(), StatusCode::OK);
    let users = json(users).await;
    let principal_id = users["users"]
        .as_array()
        .unwrap()
        .iter()
        .find(|user| user["display_name"] == "management lifecycle principal")
        .expect("created principal is returned")["id"]
        .as_str()
        .unwrap()
        .to_owned();
    let create_body = serde_json::json!({
        "principal_id": principal_id,
        "name": "management lifecycle key",
        "expires_at": null
    })
    .to_string();

    let denied_create = routes::<()>(service.access_backend())
        .layer(axum::extract::Extension(delegated(
            ManagementPermission::KeysRead,
        )))
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/dashboard/api/auth/api-keys")
                .header("content-type", "application/json")
                .body(Body::from(create_body.clone()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(denied_create.status(), StatusCode::FORBIDDEN);

    let created = routes::<()>(service.access_backend())
        .layer(axum::extract::Extension(delegated(
            ManagementPermission::KeysCreate,
        )))
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/dashboard/api/auth/api-keys")
                .header("content-type", "application/json")
                .body(Body::from(create_body))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(created.status(), StatusCode::OK);
    let created = json(created).await;
    let created_key_id = created["id"].as_str().unwrap().to_owned();
    let created_raw = created["raw_key"].as_str().unwrap().to_owned();
    assert!(created_raw.starts_with("llmc_"));
    assert!(created.get("api_key").is_none(), "created key is flattened");

    let list_response = routes::<()>(service.access_backend())
        .layer(axum::extract::Extension(delegated(
            ManagementPermission::KeysRead,
        )))
        .oneshot(
            Request::builder()
                .uri("/dashboard/api/auth/api-keys")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(list_response.status(), StatusCode::OK);
    let listed = json(list_response).await;
    let listed_wire = listed.to_string();
    assert!(
        listed["api_keys"]
            .as_array()
            .unwrap()
            .iter()
            .any(|key| key["id"] == created_key_id)
    );
    assert!(!listed_wire.contains(&created_raw));
    assert!(!listed_wire.contains("raw_key"));

    let mut headers = HeaderMap::new();
    headers.insert(
        "x-api-key",
        HeaderValue::from_str(&created_raw).expect("generated key is a valid header"),
    );
    assert!(service.authenticate(&headers).unwrap().is_some());

    let rotated = routes::<()>(service.access_backend())
        .layer(axum::extract::Extension(delegated(
            ManagementPermission::KeysRotate,
        )))
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri(format!(
                    "/dashboard/api/auth/api-keys/{created_key_id}/rotate"
                ))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(rotated.status(), StatusCode::OK);
    let rotated = json(rotated).await;
    let key_id = rotated["id"].as_str().unwrap().to_owned();
    let raw = rotated["raw_key"].as_str().unwrap().to_owned();
    assert_ne!(key_id, created_key_id);
    assert_ne!(raw, created_raw);
    assert!(matches!(
        service.authenticate(&headers),
        Err(AuthFailure::Invalid)
    ));
    headers.insert(
        "x-api-key",
        HeaderValue::from_str(&raw).expect("rotated key is a valid header"),
    );
    assert!(service.authenticate(&headers).unwrap().is_some());

    let denied = routes::<()>(service.access_backend())
        .layer(axum::extract::Extension(delegated(
            ManagementPermission::KeysRead,
        )))
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri(format!("/dashboard/api/auth/api-keys/{key_id}/revoke"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(denied.status(), StatusCode::FORBIDDEN);

    let revoked = routes::<()>(service.access_backend())
        .layer(axum::extract::Extension(delegated(
            ManagementPermission::KeysRevoke,
        )))
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri(format!("/dashboard/api/auth/api-keys/{key_id}/revoke"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(revoked.status(), StatusCode::OK);
    let revoked_body = json(revoked).await;
    let revoked_key = revoked_body["api_keys"]
        .as_array()
        .unwrap()
        .iter()
        .find(|key| key["id"] == key_id)
        .expect("revoked key remains listable");
    assert_eq!(revoked_key["enabled"], false);
    assert!(!revoked_body.to_string().contains(&raw));
    assert!(matches!(
        service.authenticate(&headers),
        Err(AuthFailure::Invalid)
    ));

    let denied_audit = routes::<()>(service.access_backend())
        .layer(axum::extract::Extension(delegated(
            ManagementPermission::KeysRead,
        )))
        .oneshot(
            Request::builder()
                .uri("/dashboard/api/auth/audit")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(denied_audit.status(), StatusCode::FORBIDDEN);

    let audit = routes::<()>(service.access_backend())
        .layer(axum::extract::Extension(delegated(
            ManagementPermission::AuditRead,
        )))
        .oneshot(
            Request::builder()
                .uri("/dashboard/api/auth/audit")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(audit.status(), StatusCode::OK);
    let audit = json(audit).await;
    let audit_wire = audit.to_string();
    assert!(!audit_wire.contains(&created_raw));
    assert!(!audit_wire.contains(&raw));

    drop(service);
    assert_store_files_do_not_contain(&path, &created_raw);
    assert_store_files_do_not_contain(&path, &raw);
    remove_store(&path);
}

#[tokio::test]
async fn delegated_role_writer_cannot_grant_permissions_it_does_not_hold() {
    let (service, path) = enforced_service();
    let service = Arc::new(service);

    let denied = routes::<()>(service.access_backend())
        .layer(axum::extract::Extension(delegated(
            ManagementPermission::RolesWrite,
        )))
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/dashboard/api/auth/roles")
                .header("content-type", "application/json")
                .body(Body::from(
                    r#"{"name":"escalated role","permissions":["auth.keys.revoke"]}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(denied.status(), StatusCode::FORBIDDEN);
    assert!(
        json(denied).await["error"]
            .as_str()
            .unwrap()
            .contains("does not possess")
    );

    let allowed = routes::<()>(service.access_backend())
        .layer(axum::extract::Extension(delegated(
            ManagementPermission::RolesWrite,
        )))
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/dashboard/api/auth/roles")
                .header("content-type", "application/json")
                .body(Body::from(
                    r#"{"name":"bounded role","permissions":["auth.roles.write"]}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(allowed.status(), StatusCode::OK);
    let allowed = json(allowed).await;
    let roles = allowed["roles"].as_array().unwrap();
    assert!(roles.iter().any(|role| role["name"] == "bounded role"));
    assert!(!roles.iter().any(|role| role["name"] == "escalated role"));

    let audit = routes::<()>(service.access_backend())
        .layer(axum::extract::Extension(ManagementActor::Bootstrap))
        .oneshot(
            Request::builder()
                .uri("/dashboard/api/auth/audit")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(audit.status(), StatusCode::OK);
    let events = json(audit).await;
    assert!(events["events"].as_array().unwrap().iter().any(|event| {
        event["action"] == "role.create"
            && event["outcome"] == "denied"
            && event["metadata"]["reason"] == "requested permissions exceed actor"
    }));

    drop(service);
    remove_store(&path);
}

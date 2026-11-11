use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::time::Duration;

use futures::StreamExt;
use serde_json::json;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

struct Server {
    child: Child,
    dir: PathBuf,
    config: PathBuf,
    url: String,
    client: reqwest::Client,
}

#[derive(Default)]
struct Launch<'a> {
    args: &'a [&'a str],
    env: &'a [(&'a str, &'a str)],
    toml: bool,
    default_command: bool,
}

impl Server {
    async fn start(contents: &str) -> Self {
        Self::start_with(contents, Launch::default()).await
    }

    async fn start_with(contents: &str, launch: Launch<'_>) -> Self {
        let dir = std::env::temp_dir().join(format!("llmconduit-reload-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir(&dir).unwrap();
        let config = if launch.default_command {
            std::fs::create_dir(dir.join("llmconduit")).unwrap();
            dir.join("llmconduit/config.yaml")
        } else {
            dir.join(if launch.toml {
                "config.toml"
            } else {
                "config.yaml"
            })
        };
        let port = std::net::TcpListener::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap()
            .port();
        let contents = if launch.toml {
            format!("bind_addr = \"127.0.0.1:{port}\"\n{contents}")
        } else {
            format!("bind_addr: 127.0.0.1:{port}\n{contents}")
        };
        std::fs::write(&config, contents).unwrap();
        let log = std::fs::File::create(dir.join("server.log")).unwrap();
        let mut command = Command::new(env!("CARGO_BIN_EXE_llmconduit"));
        for (name, _) in std::env::vars_os() {
            if name.to_string_lossy().starts_with("LLMCONDUIT_")
                || name == "OPENAI_API_KEY"
                || name == "BRAVE_SEARCH_API_KEY"
            {
                command.env_remove(name);
            }
        }
        if launch.default_command {
            command.env("XDG_CONFIG_HOME", &dir);
        } else {
            command.args(["start", "--config"]).arg(&config);
        }
        let child = command
            .args(launch.args)
            .envs(launch.env.iter().copied())
            .env("RUST_LOG", "info,llmconduit::http=warn")
            .stdout(log.try_clone().unwrap())
            .stderr(Stdio::from(log))
            .spawn()
            .unwrap();
        let server = Self {
            child,
            dir,
            config,
            url: format!("http://127.0.0.1:{port}"),
            client: reqwest::Client::new(),
        };
        server
            .wait_for("startup", || async {
                reqwest::get(format!("{}/health", server.url))
                    .await
                    .is_ok_and(|response| response.status().is_success())
            })
            .await;
        server
    }

    async fn wait_for<F, Fut>(&self, description: &str, mut check: F)
    where
        F: FnMut() -> Fut,
        Fut: std::future::Future<Output = bool>,
    {
        let result = tokio::time::timeout(Duration::from_secs(8), async {
            loop {
                if check().await {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        })
        .await;
        assert!(
            result.is_ok(),
            "timed out waiting for {description}: {}",
            self.log()
        );
    }

    fn log(&self) -> String {
        std::fs::read_to_string(self.dir.join("server.log")).unwrap()
    }

    async fn model(&self) -> serde_json::Value {
        self.client
            .get(format!("{}/v1/models", self.url))
            .send()
            .await
            .unwrap()
            .json::<serde_json::Value>()
            .await
            .unwrap()["data"][0]["id"]
            .clone()
    }

    async fn post(&self, endpoint: &str, body: serde_json::Value) -> reqwest::Response {
        self.client
            .post(format!("{}{endpoint}", self.url))
            .json(&body)
            .send()
            .await
            .unwrap()
    }

    fn atomic_replace(&self, contents: &str) {
        let temporary = self.config.with_extension("tmp");
        std::fs::write(&temporary, contents).unwrap();
        std::fs::rename(temporary, &self.config).unwrap();
    }

    async fn websocket(&self, endpoint: &str, cookie: &str) -> tokio::net::TcpStream {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let host = self.url.trim_start_matches("http://");
        let mut socket = tokio::net::TcpStream::connect(host).await.unwrap();
        socket.write_all(format!(
            "GET {endpoint} HTTP/1.1\r\nHost: {host}\r\nConnection: Upgrade\r\nUpgrade: websocket\r\nSec-WebSocket-Version: 13\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nOrigin: {}\r\nCookie: {cookie}\r\n\r\n", self.url,
        ).as_bytes()).await.unwrap();
        let mut headers = Vec::new();
        tokio::time::timeout(Duration::from_secs(5), async {
            while !headers.ends_with(b"\r\n\r\n") {
                headers.push(socket.read_u8().await.unwrap());
            }
        })
        .await
        .expect("WebSocket upgrade");
        assert!(
            headers.starts_with(b"HTTP/1.1 101"),
            "{}",
            String::from_utf8_lossy(&headers)
        );
        socket
    }
}

async fn websocket_json(socket: &mut tokio::net::TcpStream) -> serde_json::Value {
    use tokio::io::AsyncReadExt;
    tokio::time::timeout(Duration::from_secs(5), async {
        assert_eq!(socket.read_u8().await.unwrap(), 0x81);
        let length = socket.read_u8().await.unwrap();
        assert_eq!(length & 0x80, 0);
        let length = match length {
            126 => u64::from(socket.read_u16().await.unwrap()),
            127 => socket.read_u64().await.unwrap(),
            length => u64::from(length),
        };
        assert!(length < 1024 * 1024);
        let mut payload = vec![0; length as usize];
        socket.read_exact(&mut payload).await.unwrap();
        serde_json::from_slice(&payload).unwrap()
    })
    .await
    .expect("WebSocket frame")
}

impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

async fn upstream(model: &str) -> MockServer {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/v1/models"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "object": "list", "data": [{"id": model, "object": "model"}]
        })))
        .mount(&server)
        .await;
    let chunk = json!({
        "id": "chat-reload", "model": model,
        "choices": [{"index": 0, "delta": {"content": model}, "finish_reason": "stop"}],
        "usage": {"prompt_tokens": 5, "completion_tokens": 1, "total_tokens": 6}
    });
    Mock::given(method("POST"))
        .and(path("/v1/chat/completions"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("content-type", "text/event-stream")
                .set_body_string(format!("data: {chunk}\n\ndata: [DONE]\n\n")),
        )
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/v1/completions"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"model": model})))
        .mount(&server)
        .await;
    server
}

#[tokio::test]
async fn cli_hot_reload_replaces_upstream_without_restart() {
    let first = upstream("model-a").await;
    let second = upstream("model-b").await;
    let server = Server::start(&format!("upstream_base_url: {}/v1\n", first.uri())).await;
    assert_eq!(server.model().await, "model-a");
    let contents = std::fs::read_to_string(&server.config).unwrap();
    std::fs::write(
        &server.config,
        contents.replace(&first.uri(), &second.uri()),
    )
    .unwrap();
    server
        .wait_for("model catalog reload", || async {
            server.model().await == "model-b"
        })
        .await;
}

#[tokio::test]
async fn cli_hot_reload_rejects_bad_files_and_recovers_after_atomic_replace() {
    let first = upstream("model-a").await;
    let second = upstream("model-b").await;
    let server = Server::start(&format!("upstream_base_url: {}/v1\n", first.uri())).await;
    let original = std::fs::read_to_string(&server.config).unwrap();
    let changed_bind = original.replace(server.url.trim_start_matches("http://"), "127.0.0.1:1");
    for bad in [
        "upstream_base_url: [",
        "upstream_base_url: invalid-url",
        "brave_max_results: private-secret-reload-sentinel",
        "",
        &changed_bind,
        &format!("{original}debug_log_max_age_hours: 1\n"),
    ] {
        let rejected = server.log().matches("config reload rejected").count();
        std::fs::write(&server.config, bad).unwrap();
        server
            .wait_for("invalid config rejection", || async {
                server.log().matches("config reload rejected").count() > rejected
            })
            .await;
        assert_eq!(server.model().await, "model-a");
    }
    assert!(server.log().contains("bind_addr changes require a restart"));
    assert!(!server.log().contains("private-secret-reload-sentinel"));
    std::fs::remove_file(&server.config).unwrap();
    server
        .wait_for("missing config rejection", || async {
            server.log().contains("No such file or directory")
        })
        .await;
    assert_eq!(server.model().await, "model-a");
    server.atomic_replace(&original.replace(&first.uri(), &second.uri()));
    server
        .wait_for("recovery", || async { server.model().await == "model-b" })
        .await;
    let legacy = server
        .post(
            "/v1/completions",
            json!({"model":"model-b", "prompt":"hello"}),
        )
        .await;
    assert_eq!(
        legacy.json::<serde_json::Value>().await.unwrap()["model"],
        "model-b"
    );
}

#[tokio::test]
async fn cli_hot_reload_updates_profiles_and_both_body_limits() {
    let upstream = upstream("model-a").await;
    let server = Server::start(&format!(
        "upstream_base_url: {}/v1\nmax_request_body_bytes: 4096\nsystem_prompt_prefix: prefix-a\nmodel_profiles:\n  model-a:\n    temperature: 0.2\n",
        upstream.uri(),
    )).await;
    let large = json!({"model":"model-a", "input":"x".repeat(2048), "store": false});
    assert!(
        server
            .post("/v1/responses", large.clone())
            .await
            .status()
            .is_success()
    );
    let original = std::fs::read_to_string(&server.config).unwrap();
    std::fs::write(
        &server.config,
        original
            .replace("4096", "1024")
            .replace("prefix-a", "prefix-b")
            .replace("0.2", "0.8"),
    )
    .unwrap();
    server
        .wait_for("profile reload", || async {
            server.log().contains("configuration reloaded")
        })
        .await;
    for (endpoint, body) in [
        (
            "/v1/responses",
            json!({"model":"model-a", "input":"hello", "store": false}),
        ),
        (
            "/v1/chat/completions",
            json!({"model":"model-a", "messages":[{"role":"user", "content":"hello"}]}),
        ),
        (
            "/v1/messages",
            json!({"model":"model-a", "max_tokens":32, "messages":[{"role":"user", "content":"hello"}]}),
        ),
    ] {
        let response = server.post(endpoint, body).await;
        assert!(
            response.status().is_success(),
            "{}",
            response.text().await.unwrap()
        );
        let requests = upstream.received_requests().await.unwrap();
        let body: serde_json::Value = serde_json::from_slice(
            &requests
                .iter()
                .rev()
                .find(|r| r.method == "POST")
                .unwrap()
                .body,
        )
        .unwrap();
        assert_eq!(body["temperature"], 0.8);
        assert!(body["messages"].to_string().contains("prefix-b"));
    }
    let response = server
        .post(
            "/v1/responses",
            json!({"model":"model-a", "input":"explicit", "temperature":0.3, "store": false}),
        )
        .await;
    assert!(response.status().is_success());
    let requests = upstream.received_requests().await.unwrap();
    let body: serde_json::Value = serde_json::from_slice(
        &requests
            .iter()
            .rev()
            .find(|r| r.method == "POST")
            .unwrap()
            .body,
    )
    .unwrap();
    assert_eq!(body["temperature"], 0.3);
    assert_eq!(server.post("/v1/responses", large).await.status(), 413);
    let compressed = zstd::stream::encode_all(
        serde_json::to_vec(&json!({"input":"x".repeat(2048)}))
            .unwrap()
            .as_slice(),
        1,
    )
    .unwrap();
    assert_eq!(
        server
            .client
            .post(format!("{}/v1/responses", server.url))
            .header("content-type", "application/json")
            .header("content-encoding", "zstd")
            .body(compressed)
            .send()
            .await
            .unwrap()
            .status(),
        413
    );
    let mut expanded = std::fs::read_to_string(&server.config)
        .unwrap()
        .replace("1024", "8192");
    expanded.push_str("strict_routing: true\n");
    std::fs::write(&server.config, expanded).unwrap();
    server
        .wait_for("larger body limit", || async {
            server.log().matches("configuration reloaded").count() == 2
        })
        .await;
    assert!(
        server
            .post(
                "/v1/responses",
                json!({"model":"model-a", "input":"x".repeat(5000), "store":false})
            )
            .await
            .status()
            .is_success()
    );
    assert_eq!(
        server
            .post("/v1/responses", json!({"model":"unknown", "input":"hello"}))
            .await
            .status(),
        400
    );
}

#[tokio::test]
async fn cli_hot_reload_supports_default_invocation_and_toml() {
    for toml in [false, true] {
        let first = upstream("model-a").await;
        let second = upstream("model-b").await;
        let contents = if toml {
            format!("upstream_base_url = \"{}/v1\"\n", first.uri())
        } else {
            format!("upstream_base_url: {}/v1\n", first.uri())
        };
        let server = Server::start_with(
            &contents,
            Launch {
                toml,
                default_command: !toml,
                ..Launch::default()
            },
        )
        .await;
        assert_eq!(server.model().await, "model-a");
        let original = std::fs::read_to_string(&server.config).unwrap();
        if toml {
            std::fs::write(
                &server.config,
                "upstream_api_key = private-secret-reload-sentinel",
            )
            .unwrap();
            server
                .wait_for("invalid TOML rejection", || async {
                    server.log().contains("invalid TOML config at line")
                })
                .await;
            assert!(!server.log().contains("private-secret-reload-sentinel"));
            assert_eq!(server.model().await, "model-a");
        }
        server.atomic_replace(&original.replace(&first.uri(), &second.uri()));
        server
            .wait_for("catalog reload", || async {
                server.model().await == "model-b"
            })
            .await;
    }
}

#[tokio::test]
async fn cli_hot_reload_preserves_env_and_cli_route_precedence() {
    let file_upstream = upstream("file-model").await;
    let env_upstream = upstream("env-model").await;
    let cli_upstream = upstream("cli-model").await;
    let env_url = format!("{}/v1", env_upstream.uri());
    let route_spec = format!("routed={}/v1,cli-model", cli_upstream.uri());
    let env_server = Server::start_with(
        &format!(
            "upstream_base_url: {}/v1\nsystem_prompt_prefix: file-prefix\n",
            file_upstream.uri(),
        ),
        Launch {
            env: &[
                ("LLMCONDUIT_UPSTREAM_BASE_URL", &env_url),
                ("LLMCONDUIT_SYSTEM_PROMPT_PREFIX", "env-prefix"),
            ],
            ..Launch::default()
        },
    )
    .await;
    let route_server = Server::start_with(&format!(
        "upstream_base_url: {}/v1\nsystem_prompt_prefix: file-prefix\nmodel_routes:\n  routed:\n    base_url: {}/v1\n    upstream_model: file-model\n",
        file_upstream.uri(), file_upstream.uri(),
    ), Launch {
        args: &["--model-route", &route_spec],
        env: &[("LLMCONDUIT_UPSTREAM_BASE_URL", &env_url), ("LLMCONDUIT_SYSTEM_PROMPT_PREFIX", "env-prefix")],
        ..Launch::default()
    }).await;
    for (server, model) in [(&env_server, "env-model"), (&route_server, "routed")] {
        for reload in [false, true] {
            if reload {
                let contents = std::fs::read_to_string(&server.config).unwrap();
                server.atomic_replace(&contents.replace("file-prefix", "edited-file-prefix"));
                server
                    .wait_for("override reload", || async {
                        server.log().contains("configuration reloaded")
                    })
                    .await;
            }
            let response = server
                .post(
                    "/v1/responses",
                    json!({"model":model, "input":"hello", "store":false}),
                )
                .await;
            assert!(
                response.status().is_success(),
                "{}",
                response.text().await.unwrap()
            );
        }
    }
    for (provider, expected_model) in [(&env_upstream, "env-model"), (&cli_upstream, "cli-model")] {
        let requests = provider.received_requests().await.unwrap();
        let posts = requests
            .iter()
            .filter(|r| r.method == "POST")
            .collect::<Vec<_>>();
        assert_eq!(posts.len(), 2);
        for request in posts {
            let body: serde_json::Value = serde_json::from_slice(&request.body).unwrap();
            assert_eq!(body["model"], expected_model);
            assert!(body["messages"].to_string().contains("env-prefix"));
            assert!(!body["messages"].to_string().contains("file-prefix"));
        }
    }
    assert!(file_upstream.received_requests().await.unwrap().is_empty());
}

#[tokio::test]
async fn cli_hot_reload_keeps_streams_and_dashboard_sessions_and_history() {
    use axum::{
        Json, Router,
        body::Body,
        response::Response,
        routing::{get, post},
    };
    use std::sync::Arc;
    use tokio::sync::Notify;

    let finish = Arc::new(Notify::new());
    let release = finish.clone();
    let upstream_app = Router::new()
        .route("/v1/models", get(|| async { Json(json!({"data":[{"id":"model-a"}]})) }))
        .route("/v1/chat/completions", post(move || {
            let release = release.clone();
            async move {
                let stream = async_stream::stream! {
                    yield Ok::<_, std::convert::Infallible>(format!("data: {}\n\n", json!({
                        "id":"chat-a", "choices":[{"index":0,"delta":{"content":"from-a-"},"finish_reason":null}]
                    })));
                    release.notified().await;
                    yield Ok(format!("data: {}\n\ndata: [DONE]\n\n", json!({
                        "id":"chat-a", "choices":[{"index":0,"delta":{"content":"finished"},"finish_reason":"stop"}],
                        "usage":{"prompt_tokens":5,"completion_tokens":2,"total_tokens":7}
                    })));
                };
                Response::builder().header("content-type","text/event-stream").body(Body::from_stream(stream)).unwrap()
            }
        }));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let first_url = format!("http://{}", listener.local_addr().unwrap());
    let _upstream_task = tokio_util::task::AbortOnDropHandle::new(tokio::spawn(async move {
        axum::serve(listener, upstream_app).await.unwrap();
    }));
    let second = upstream("model-b").await;
    let server = Server::start_with(
        &format!("upstream_base_url: {first_url}/v1\n"),
        Launch {
            args: &["--with-debug-ui"],
            env: &[("LLMCONDUIT_DASHBOARD_TOKEN", "reload-test-token")],
            ..Launch::default()
        },
    )
    .await;
    let login = server
        .post("/dashboard/login", json!({"token":"reload-test-token"}))
        .await;
    assert!(login.status().is_success());
    let cookie = login.headers()["set-cookie"]
        .to_str()
        .unwrap()
        .split(';')
        .next()
        .unwrap()
        .to_string();
    let flows = || async {
        let response = server
            .client
            .get(format!("{}/dashboard/api/flows", server.url))
            .header("cookie", &cookie)
            .send()
            .await
            .unwrap();
        assert!(
            response.status().is_success(),
            "dashboard cookie must survive reload"
        );
        response.json::<serde_json::Value>().await.unwrap()
    };
    let response = server
        .post(
            "/v1/responses",
            json!({"model":"model-a","input":"hello","stream":true,"store":false}),
        )
        .await;
    assert!(response.status().is_success());
    let mut stream = response.bytes_stream();
    let mut body = String::new();
    tokio::time::timeout(Duration::from_secs(5), async {
        while !body.contains("from-a-") {
            body.push_str(std::str::from_utf8(&stream.next().await.unwrap().unwrap()).unwrap());
        }
    })
    .await
    .expect("first upstream delta");
    let before = flows().await;
    assert_eq!(before["flows"].as_array().unwrap().len(), 1, "{before}");
    let active_id = before["flows"][0]["api_call_id"].clone();
    assert!(active_id.is_string());
    let original = std::fs::read_to_string(&server.config).unwrap();
    server.atomic_replace(&original.replace(&first_url, &second.uri()));
    server
        .wait_for("reload during streaming", || async {
            server.model().await == "model-b"
        })
        .await;
    let during = flows().await;
    assert_eq!(during["flows"][0]["api_call_id"], active_id);
    let response = server
        .post(
            "/v1/responses",
            json!({"model":"model-b", "input":"hello","store":false}),
        )
        .await;
    assert!(response.status().is_success());
    assert!(response.text().await.unwrap().contains("model-b"));
    finish.notify_one();
    tokio::time::timeout(Duration::from_secs(5), async {
        while let Some(chunk) = stream.next().await {
            body.push_str(std::str::from_utf8(&chunk.unwrap()).unwrap());
        }
    })
    .await
    .expect("original stream completes");
    assert!(body.contains("response.completed"), "{body}");
    assert!(body.contains("from-a-finished"), "{body}");
    assert!(!body.contains("model-b"));
    let after = flows().await;
    let rows = after["flows"].as_array().unwrap();
    assert_eq!(rows.len(), 2);
    assert!(
        rows.iter().all(|flow| flow["status"] == "completed"),
        "{after}"
    );
}

#[tokio::test]
async fn cli_hot_reload_updates_prices_on_an_open_dashboard_websocket() {
    let upstream = upstream("model-a").await;
    let server = Server::start_with(&format!(
        "upstream_base_url: {}/v1\nprice_table:\n  model-a:\n    input_per_1k: 1.0\n    output_per_1k: 1.0\n",
        upstream.uri(),
    ), Launch { args: &["--with-debug-ui"], ..Launch::default() }).await;
    let response = server
        .post(
            "/v1/responses",
            json!({"model":"model-a", "input":"hello", "store":false}),
        )
        .await;
    assert!(response.status().is_success());
    let mut socket = server.websocket("/dashboard/ws", "").await;
    let initial = websocket_json(&mut socket).await;
    assert_eq!(initial["type"], "snapshot");
    let cost = initial["metrics"]["cost_per_min"].as_f64().unwrap();
    assert!(cost > 0.0, "{initial}");
    let original = std::fs::read_to_string(&server.config).unwrap();
    server.atomic_replace(&original.replace("1.0", "2.0"));
    tokio::time::timeout(Duration::from_secs(8), async {
        loop {
            let frame = websocket_json(&mut socket).await;
            if let Some(batch) = frame["batch"].as_array()
                && batch.iter().any(|payload| {
                    payload["cost_per_min"]
                        .as_f64()
                        .is_some_and(|updated| (updated - 2.0 * cost).abs() < 1e-9)
                })
            {
                break;
            }
        }
    })
    .await
    .expect("live prices update without another inference request");
}

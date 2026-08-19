use crate::config::Config;
use crate::error::AppError;
use crate::error::AppResult;
use async_trait::async_trait;
use serde::Deserialize;
use url::Url;

/// A single web result, structured for the Anthropic `web_search_tool_result`
/// block (so clients render source citations). The model-facing prose still
/// comes from [`SearchOutcome::formatted`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SearchSource {
    pub title: String,
    pub url: String,
}

/// Result of a web search: the flattened text injected into the model's
/// context, plus the structured sources surfaced to the client.
#[derive(Debug, Clone, Default)]
pub struct SearchOutcome {
    pub formatted: String,
    pub sources: Vec<SearchSource>,
}

#[async_trait]
pub trait SearchClient: Send + Sync {
    async fn search(&self, query: &str) -> AppResult<SearchOutcome>;
}

#[derive(Debug, Clone)]
pub struct BraveSearchClient {
    client: reqwest::Client,
    base_url: Url,
    api_key: Option<String>,
    max_results: usize,
}

impl BraveSearchClient {
    pub fn new(client: reqwest::Client, config: Config) -> Self {
        Self {
            client,
            base_url: config.brave_base_url,
            api_key: config.brave_api_key,
            max_results: config.brave_max_results,
        }
    }

    fn endpoint_url(&self, path: &str) -> AppResult<Url> {
        let mut url = self.base_url.clone();
        if !url.path().ends_with('/') {
            let new_path = format!("{}/", url.path());
            url.set_path(&new_path);
        }
        url.join(path)
            .map_err(|err| AppError::internal(format!("invalid Brave URL: {err}")))
    }
}

#[async_trait]
impl SearchClient for BraveSearchClient {
    async fn search(&self, query: &str) -> AppResult<SearchOutcome> {
        let api_key = self.api_key.as_deref().ok_or_else(|| {
            AppError::internal("web_search is configured but BRAVE_SEARCH_API_KEY is missing")
        })?;
        let url = self.endpoint_url("web/search")?;
        let response = self
            .client
            .get(url)
            .header("X-Subscription-Token", api_key)
            .query(&[
                ("q", query),
                ("count", &self.max_results.to_string()),
                ("text_decorations", "false"),
                ("spellcheck", "false"),
            ])
            .send()
            .await
            .map_err(|err| AppError::upstream(format!("Brave search request failed: {err}")))?;
        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            return Err(AppError::upstream(format!(
                "Brave search failed with {status}: {body}"
            )));
        }
        let payload: BraveSearchResponse = response
            .json()
            .await
            .map_err(|err| AppError::upstream(format!("invalid Brave search JSON: {err}")))?;
        Ok(SearchOutcome {
            formatted: format_search_results(&payload),
            sources: collect_sources(&payload),
        })
    }
}

fn collect_sources(payload: &BraveSearchResponse) -> Vec<SearchSource> {
    payload
        .web
        .as_ref()
        .map(|web| web.results.as_slice())
        .unwrap_or(&[])
        .iter()
        .filter(|result| !result.url.is_empty())
        .map(|result| SearchSource {
            title: result.title.clone(),
            url: result.url.clone(),
        })
        .collect()
}

#[derive(Debug, Deserialize)]
struct BraveSearchResponse {
    #[serde(default)]
    web: Option<BraveWebResults>,
}

#[derive(Debug, Deserialize)]
struct BraveWebResults {
    #[serde(default)]
    results: Vec<BraveWebResult>,
}

#[derive(Debug, Deserialize)]
struct BraveWebResult {
    #[serde(default)]
    title: String,
    #[serde(default)]
    url: String,
    #[serde(default)]
    description: String,
}

fn format_search_results(payload: &BraveSearchResponse) -> String {
    let mut lines = Vec::new();
    for (index, result) in payload
        .web
        .as_ref()
        .map(|web| web.results.as_slice())
        .unwrap_or(&[])
        .iter()
        .enumerate()
    {
        lines.push(format!("{}. {}", index + 1, result.title));
        if !result.url.is_empty() {
            lines.push(format!("URL: {}", result.url));
        }
        if !result.description.is_empty() {
            lines.push(format!("Snippet: {}", result.description));
        }
        lines.push(String::new());
    }
    if lines.is_empty() {
        "No Brave search results found.".to_string()
    } else {
        lines.join("\n").trim().to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::BraveSearchClient;
    use crate::config::Config;

    use super::BraveSearchResponse;
    use super::BraveWebResult;
    use super::BraveWebResults;
    use super::SearchSource;
    use super::collect_sources;
    use super::format_search_results;

    #[test]
    fn format_search_results_empty() {
        let response = BraveSearchResponse { web: None };
        assert_eq!(
            format_search_results(&response),
            "No Brave search results found."
        );
    }

    #[test]
    fn format_search_results_missing_fields() {
        let response = BraveSearchResponse {
            web: Some(BraveWebResults {
                results: vec![BraveWebResult {
                    title: String::new(),
                    url: String::new(),
                    description: String::new(),
                }],
            }),
        };
        let result = format_search_results(&response);
        assert!(result.contains("1."));
    }

    #[test]
    fn collect_sources_extracts_structured_url_title_skipping_empty_urls() {
        // The structured sources feed the Anthropic `web_search_tool_result`
        // block; results without a URL can't be a citation and are dropped.
        // The model-facing formatted text must stay byte-identical.
        let response = BraveSearchResponse {
            web: Some(BraveWebResults {
                results: vec![
                    BraveWebResult {
                        title: "Alpha".to_string(),
                        url: "https://a.test".to_string(),
                        description: "da".to_string(),
                    },
                    BraveWebResult {
                        title: "No URL".to_string(),
                        url: String::new(),
                        description: "d".to_string(),
                    },
                    BraveWebResult {
                        title: "Beta".to_string(),
                        url: "https://b.test".to_string(),
                        description: "db".to_string(),
                    },
                ],
            }),
        };
        let sources = collect_sources(&response);
        assert_eq!(
            sources,
            vec![
                SearchSource {
                    title: "Alpha".to_string(),
                    url: "https://a.test".to_string(),
                },
                SearchSource {
                    title: "Beta".to_string(),
                    url: "https://b.test".to_string(),
                },
            ]
        );
        assert!(format_search_results(&response).contains("URL: https://a.test"));
    }

    #[test]
    fn endpoint_url_preserves_v1_without_trailing_slash() {
        let client = BraveSearchClient::new(
            reqwest::Client::new(),
            Config {
                bind_addr: "127.0.0.1:0".parse().expect("socket addr"),
                system_prompt_prefix: None,
                upstream_request_log_path: None,
                turn_capture_dir: None,
                upstream_chat_kwargs: serde_json::Map::new(),
                upstreams: Vec::new(),
                upstream_failure_cooldown_secs: 30,
                model_profiles: Vec::new(),
                template_family: None,
                brave_base_url: url::Url::parse("https://api.search.brave.com/res/v1")
                    .expect("url"),
                brave_api_key: Some("secret".to_string()),
                brave_max_results: 5,
                request_timeout: std::time::Duration::from_secs(30),
                connect_timeout_secs: 10,
                max_web_search_rounds: 5,
                flatten_content: true,
                max_replay_entries: 1000,
                debug_log_max_age_hours: None,
                min_completion_tokens: 4096,
                max_sse_frame_bytes: 8 * 1024 * 1024,
                max_request_body_bytes: 10 * 1024 * 1024,
                image_cache_max_size: 100,
                image_cache_ttl_secs: 300,
                price_table: std::collections::HashMap::new(),
            },
        );

        assert_eq!(
            client
                .endpoint_url("web/search")
                .expect("endpoint")
                .as_str(),
            "https://api.search.brave.com/res/v1/web/search"
        );
    }

    #[test]
    fn endpoint_url_preserves_v1_with_trailing_slash() {
        let client = BraveSearchClient::new(
            reqwest::Client::new(),
            Config {
                bind_addr: "127.0.0.1:0".parse().expect("socket addr"),
                system_prompt_prefix: None,
                upstream_request_log_path: None,
                turn_capture_dir: None,
                upstream_chat_kwargs: serde_json::Map::new(),
                upstreams: Vec::new(),
                upstream_failure_cooldown_secs: 30,
                model_profiles: Vec::new(),
                template_family: None,
                brave_base_url: url::Url::parse("https://api.search.brave.com/res/v1/")
                    .expect("url"),
                brave_api_key: Some("secret".to_string()),
                brave_max_results: 5,
                request_timeout: std::time::Duration::from_secs(30),
                connect_timeout_secs: 10,
                max_web_search_rounds: 5,
                flatten_content: true,
                max_replay_entries: 1000,
                debug_log_max_age_hours: None,
                min_completion_tokens: 4096,
                max_sse_frame_bytes: 8 * 1024 * 1024,
                max_request_body_bytes: 10 * 1024 * 1024,
                image_cache_max_size: 100,
                image_cache_ttl_secs: 300,
                price_table: std::collections::HashMap::new(),
            },
        );

        assert_eq!(
            client
                .endpoint_url("web/search")
                .expect("endpoint")
                .as_str(),
            "https://api.search.brave.com/res/v1/web/search"
        );
    }
}

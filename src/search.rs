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

// ──────────────────────────────────────────────────────────────────────────
// Kagi search backend: Kagi Search API
// ──────────────────────────────────────────────────────────────────────────

/// Kagi Search API backend.  Uses `https://kagi.com/api/v1/search` with a
/// Bearer API key.  Returns clean search results with title, URL, and snippet.
#[derive(Debug, Clone)]
pub struct KagiSearchClient {
    client: reqwest::Client,
    base_url: Url,
    api_key: Option<String>,
    max_results: usize,
}

impl KagiSearchClient {
    pub fn new(client: reqwest::Client, config: Config) -> Self {
        Self {
            client,
            base_url: config.kagi_base_url,
            api_key: config.kagi_api_key,
            max_results: config.kagi_max_results,
        }
    }
}

#[async_trait]
impl SearchClient for KagiSearchClient {
    async fn search(&self, query: &str) -> AppResult<SearchOutcome> {
        let api_key = self.api_key.as_deref().ok_or_else(|| {
            AppError::internal("web_search is configured but KAGI_API_KEY is missing")
        })?;
        let url = self
            .base_url
            .join("search")
            .map_err(|err| AppError::internal(format!("invalid Kagi URL: {err}")))?;
        let body = serde_json::json!({
            "query": query,
            "limit": self.max_results,
        });
        let response = self
            .client
            .post(url)
            .header("Authorization", format!("Bearer {api_key}"))
            .header("Content-Type", "application/json")
            .json(&body)
            .send()
            .await
            .map_err(|err| AppError::upstream(format!("Kagi search request failed: {err}")))?;
        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            return Err(AppError::upstream(format!(
                "Kagi search failed with {status}: {body}"
            )));
        }
        let payload: KagiResponse = response
            .json()
            .await
            .map_err(|err| AppError::upstream(format!("invalid Kagi search JSON: {err}")))?;

        let results = payload.data.unwrap_or_default().search;
        if results.is_empty() {
            return Ok(SearchOutcome {
                formatted: "No search results found.".to_string(),
                sources: Vec::new(),
            });
        }

        let mut lines = Vec::new();
        let mut sources = Vec::new();
        for (index, result) in results.iter().enumerate() {
            if result.url.is_empty() {
                continue;
            }
            lines.push(format!("{}. {}", index + 1, result.title));
            lines.push(format!("URL: {}", result.url));
            if !result.snippet.is_empty() {
                lines.push(format!("Snippet: {}", result.snippet));
            }
            lines.push(String::new());
            sources.push(SearchSource {
                title: result.title.clone(),
                url: result.url.clone(),
            });
        }

        let formatted = if lines.is_empty() {
            "No search results found.".to_string()
        } else {
            lines.join("\n").trim().to_string()
        };

        Ok(SearchOutcome { formatted, sources })
    }
}

// ── Kagi response types ───────────────────────────────────────────────────

#[derive(Debug, Deserialize)]
struct KagiResponse {
    #[serde(default)]
    data: Option<KagiData>,
}

#[derive(Debug, Deserialize, Default)]
struct KagiData {
    #[serde(default)]
    search: Vec<KagiResult>,
}

#[derive(Debug, Deserialize)]
struct KagiResult {
    #[serde(default)]
    title: String,
    #[serde(default)]
    url: String,
    #[serde(default)]
    snippet: String,
}

// ──────────────────────────────────────────────────────────────────────────
// Crawl4AI search backend: SearXNG (search) + crawl4ai (content extraction)
// ──────────────────────────────────────────────────────────────────────────

/// A free, self-hosted alternative to the Brave Search backend.  SearXNG
/// produces the URL list; crawl4ai extracts clean Markdown from the top
/// results so the model gets full-page context instead of short snippets.
#[derive(Debug, Clone)]
pub struct Crawl4AISearchClient {
    client: reqwest::Client,
    searxng_base_url: Url,
    crawl4ai_base_url: Url,
    crawl4ai_api_token: Option<String>,
    max_crawl_urls: usize,
    content_max_chars: usize,
}

impl Crawl4AISearchClient {
    pub fn new(client: reqwest::Client, config: Config) -> Self {
        Self {
            client,
            searxng_base_url: config.searxng_base_url,
            crawl4ai_base_url: config.crawl4ai_base_url,
            crawl4ai_api_token: config.crawl4ai_api_token,
            max_crawl_urls: config.crawl4ai_max_crawl_urls,
            content_max_chars: config.crawl4ai_content_max_chars,
        }
    }
}

#[async_trait]
impl SearchClient for Crawl4AISearchClient {
    async fn search(&self, query: &str) -> AppResult<SearchOutcome> {
        // Step 1 — SearXNG search to get candidate URLs + snippets.
        let searxng_url = self
            .searxng_base_url
            .join("search")
            .map_err(|err| AppError::internal(format!("invalid SearXNG URL: {err}")))?;
        let searxng_resp = self
            .client
            .get(searxng_url)
            .query(&[("q", query), ("format", "json")])
            .send()
            .await
            .map_err(|err| AppError::upstream(format!("SearXNG request failed: {err}")))?;
        if !searxng_resp.status().is_success() {
            let status = searxng_resp.status();
            let body = searxng_resp.text().await.unwrap_or_default();
            return Err(AppError::upstream(format!(
                "SearXNG failed with {status}: {body}"
            )));
        }
        let searxng_payload: SearXNGResponse = searxng_resp
            .json()
            .await
            .map_err(|err| AppError::upstream(format!("invalid SearXNG JSON: {err}")))?;

        let all_results = &searxng_payload.results;
        if all_results.is_empty() {
            return Ok(SearchOutcome {
                formatted: "No search results found.".to_string(),
                sources: Vec::new(),
            });
        }

        // Take top N results for crawling; keep all for source citations.
        let crawl_count = self.max_crawl_urls.min(all_results.len());
        let urls_to_crawl: Vec<&str> = all_results
            .iter()
            .take(crawl_count)
            .map(|r| r.url.as_str())
            .filter(|u| !u.is_empty())
            .collect();

        // Step 2 — crawl4ai content extraction for the top URLs.
        let crawled = if urls_to_crawl.is_empty() {
            Vec::new()
        } else {
            self.crawl_urls(&urls_to_crawl).await.unwrap_or_else(|err| {
                tracing::warn!("crawl4ai extraction failed, using snippets only: {err}");
                Vec::new()
            })
        };

        // Step 3 — combine snippets + crawled markdown into the formatted output.
        let mut lines = Vec::new();
        let mut sources = Vec::new();

        for (index, result) in all_results.iter().enumerate() {
            if result.url.is_empty() {
                continue;
            }
            lines.push(format!("{}. {}", index + 1, result.title));
            lines.push(format!("URL: {}", result.url));
            if !result.content.is_empty() {
                lines.push(format!("Snippet: {}", result.content));
            }
            // Attach crawled markdown for results that were crawled.
            if let Some(md) = crawled.iter().find(|c| c.url == result.url) {
                if !md.markdown.is_empty() {
                    let truncated = truncate_str(&md.markdown, self.content_max_chars);
                    lines.push(format!("Content:\n{}", truncated));
                }
            }
            lines.push(String::new());
            sources.push(SearchSource {
                title: result.title.clone(),
                url: result.url.clone(),
            });
        }

        let formatted = if lines.is_empty() {
            "No search results found.".to_string()
        } else {
            lines.join("\n").trim().to_string()
        };

        Ok(SearchOutcome { formatted, sources })
    }
}

impl Crawl4AISearchClient {
    /// POST the URLs to crawl4ai `/crawl` and return extracted Markdown per URL.
    async fn crawl_urls(&self, urls: &[&str]) -> AppResult<Vec<CrawledPage>> {
        let endpoint = self
            .crawl4ai_base_url
            .join("crawl")
            .map_err(|err| AppError::internal(format!("invalid crawl4ai URL: {err}")))?;

        let body = serde_json::json!({
            "urls": urls,
            "browser_config": {
                "type": "BrowserConfig",
                "params": {"headless": true}
            },
            "crawler_config": {
                "type": "CrawlerRunConfig",
                "params": {
                    "stream": false,
                    "cache_mode": "enabled",
                    "page_timeout": 15000,
                    "word_count_threshold": 50
                }
            }
        });

        let mut req = self.client.post(endpoint).json(&body);
        if let Some(token) = &self.crawl4ai_api_token {
            req = req.header("Authorization", format!("Bearer {token}"));
        }

        let resp = req
            .send()
            .await
            .map_err(|err| AppError::upstream(format!("crawl4ai request failed: {err}")))?;
        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            return Err(AppError::upstream(format!(
                "crawl4ai failed with {status}: {body}"
            )));
        }

        let payload: Crawl4AIResponse = resp
            .json()
            .await
            .map_err(|err| AppError::upstream(format!("invalid crawl4ai JSON: {err}")))?;

        let pages = payload
            .results
            .into_iter()
            .filter(|r| r.success)
            .map(|r| CrawledPage {
                url: r.url,
                markdown: r.markdown.raw_markdown,
            })
            .collect();

        Ok(pages)
    }
}

/// Truncate a string to at most `max` chars, appending an ellipsis if cut.
fn truncate_str(s: &str, max: usize) -> String {
    if s.len() <= max {
        return s.to_string();
    }
    let boundary = s.char_indices().take(max).last().map(|(i, _)| i).unwrap_or(max);
    format!("{}…", &s[..boundary])
}

#[derive(Debug)]
struct CrawledPage {
    url: String,
    markdown: String,
}

// ── SearXNG response types ────────────────────────────────────────────────

#[derive(Debug, Deserialize)]
struct SearXNGResponse {
    #[serde(default)]
    results: Vec<SearXNGResult>,
}

#[derive(Debug, Deserialize)]
struct SearXNGResult {
    #[serde(default)]
    title: String,
    #[serde(default)]
    url: String,
    #[serde(default)]
    content: String,
}

// ── crawl4ai response types ───────────────────────────────────────────────

#[derive(Debug, Deserialize)]
struct Crawl4AIResponse {
    #[serde(default)]
    results: Vec<Crawl4AIResult>,
}

#[derive(Debug, Deserialize)]
struct Crawl4AIResult {
    #[serde(default)]
    url: String,
    #[serde(default)]
    success: bool,
    #[serde(default)]
    markdown: Crawl4AIMarkdown,
}

#[derive(Debug, Deserialize, Default)]
struct Crawl4AIMarkdown {
    #[serde(default)]
    raw_markdown: String,
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
                upstream_base_url: url::Url::parse("http://127.0.0.1:8000/v1/").expect("url"),
                upstream_api_key: None,
                upstream_model: None,
                default_reasoning_effort: "max".to_string(),
                system_prompt_prefix: None,
                upstream_request_log_path: None,
                upstream_chat_kwargs: serde_json::Map::new(),
                upstreams: Vec::new(),
                fallback_upstreams: Vec::new(),
                upstream_failure_cooldown_secs: 30,
                model_profiles: std::collections::BTreeMap::new(),
                brave_base_url: url::Url::parse("https://api.search.brave.com/res/v1")
                    .expect("url"),
                brave_api_key: Some("secret".to_string()),
                brave_max_results: 5,
                kagi_base_url: url::Url::parse("https://kagi.com/api/v1").expect("url"),
                kagi_api_key: None,
                kagi_max_results: 5,
                search_backend: "brave".to_string(),
                searxng_base_url: url::Url::parse("http://localhost:4040").expect("url"),
                crawl4ai_base_url: url::Url::parse("http://localhost:11235").expect("url"),
                crawl4ai_api_token: None,
                crawl4ai_max_crawl_urls: 3,
                crawl4ai_content_max_chars: 8000,
                request_timeout: std::time::Duration::from_secs(30),
                connect_timeout_secs: 10,
                max_web_search_rounds: 5,
                flatten_content: true,
                max_replay_entries: 1000,
                image_agent_enabled: false,
                image_agent_always_active: false,
                vision_url: None,
                vision_model: None,
                image_cache_max_size: 100,
                image_cache_ttl_secs: 300,
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
                upstream_base_url: url::Url::parse("http://127.0.0.1:8000/v1/").expect("url"),
                upstream_api_key: None,
                upstream_model: None,
                default_reasoning_effort: "max".to_string(),
                system_prompt_prefix: None,
                upstream_request_log_path: None,
                upstream_chat_kwargs: serde_json::Map::new(),
                upstreams: Vec::new(),
                fallback_upstreams: Vec::new(),
                upstream_failure_cooldown_secs: 30,
                model_profiles: std::collections::BTreeMap::new(),
                brave_base_url: url::Url::parse("https://api.search.brave.com/res/v1/")
                    .expect("url"),
                brave_api_key: Some("secret".to_string()),
                brave_max_results: 5,
                kagi_base_url: url::Url::parse("https://kagi.com/api/v1").expect("url"),
                kagi_api_key: None,
                kagi_max_results: 5,
                search_backend: "brave".to_string(),
                searxng_base_url: url::Url::parse("http://localhost:4040").expect("url"),
                crawl4ai_base_url: url::Url::parse("http://localhost:11235").expect("url"),
                crawl4ai_api_token: None,
                crawl4ai_max_crawl_urls: 3,
                crawl4ai_content_max_chars: 8000,
                request_timeout: std::time::Duration::from_secs(30),
                connect_timeout_secs: 10,
                max_web_search_rounds: 5,
                flatten_content: true,
                max_replay_entries: 1000,
                image_agent_enabled: false,
                image_agent_always_active: false,
                vision_url: None,
                vision_model: None,
                image_cache_max_size: 100,
                image_cache_ttl_secs: 300,
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

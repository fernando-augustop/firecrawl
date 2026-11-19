//! Web search through an operator-configured SearXNG instance.

use std::{collections::HashSet, time::Duration};

use futures::stream::{self, StreamExt};
use serde::Deserialize;
use serde_json::{json, Value};
use url::Url;

use crate::scrape::{Engine, ScrapeOptions};

#[derive(Clone)]
pub(crate) struct Search {
  endpoint: Url,
  http: reqwest::Client,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct SearchRequest {
  query: String,
  limit: Option<usize>,
  tbs: Option<String>,
  #[serde(default)]
  include_domains: Vec<String>,
  #[serde(default)]
  exclude_domains: Vec<String>,
  #[serde(default)]
  categories: Vec<Value>,
  scrape_options: Option<ScrapeOptions>,
}

#[derive(Debug)]
pub(crate) enum SearchError {
  Invalid(String),
  Backend,
}

#[derive(Deserialize)]
struct Response {
  results: Vec<ResultItem>,
  #[serde(default)]
  unresponsive_engines: Vec<Value>,
}

#[derive(Deserialize)]
struct ResultItem {
  url: String,
  #[serde(default)]
  title: String,
  #[serde(default)]
  content: String,
}

fn domain(value: &str) -> Result<String, SearchError> {
  let value = value.trim().trim_start_matches('.').trim_end_matches('.');
  let parsed = Url::parse(&format!("https://{value}"))
    .map_err(|_| SearchError::Invalid("Invalid search domain.".into()))?;
  let name = parsed
    .domain()
    .ok_or_else(|| SearchError::Invalid("Invalid search domain.".into()))?;
  if value.contains(|c| matches!(c, '/' | ':' | '?' | '#' | '@' | '*'))
    || name.contains(|c: char| !(c.is_ascii_alphanumeric() || matches!(c, '.' | '-')))
  {
    return Err(SearchError::Invalid("Invalid search domain.".into()));
  }
  Ok(name.to_ascii_lowercase())
}

fn matches_domain(host: &str, domain: &str) -> bool {
  host == domain
    || host
      .strip_suffix(domain)
      .is_some_and(|prefix| prefix.ends_with('.'))
}

fn category_queries(categories: &[Value]) -> Result<Vec<&'static str>, SearchError> {
  categories.iter().map(|value| {
    match value.as_str().or_else(|| value.get("type").and_then(Value::as_str)) {
      Some("github") => Ok("site:github.com"),
      Some("research") => Ok("(site:arxiv.org OR site:pubmed.ncbi.nlm.nih.gov OR site:biorxiv.org OR site:papers.ssrn.com)"),
      Some("pdf") => Ok("filetype:pdf"),
      _ => Err(SearchError::Invalid("Unsupported search category.".into())),
    }
  }).collect()
}

impl Search {
  pub(crate) fn new(endpoint: Url) -> Result<Self, String> {
    if !matches!(endpoint.scheme(), "http" | "https")
      || !endpoint.has_host()
      || !endpoint.username().is_empty()
      || endpoint.password().is_some()
      || endpoint.query().is_some()
      || endpoint.fragment().is_some()
    {
      return Err(
        "The SearXNG endpoint must be an HTTP(S) URL without credentials, query or fragment."
          .into(),
      );
    }
    let http = reqwest::Client::builder()
      .timeout(Duration::from_secs(20))
      .redirect(reqwest::redirect::Policy::none())
      .build()
      .map_err(|_| "Could not build the SearXNG client.")?;
    Ok(Self { endpoint, http })
  }

  pub(crate) async fn run(
    &self,
    engine: &Engine,
    request: SearchRequest,
  ) -> Result<Value, SearchError> {
    let query = request.query.trim();
    if query.is_empty() || query.len() > 2000 {
      return Err(SearchError::Invalid(
        "Search query must contain 1 to 2000 bytes.".into(),
      ));
    }
    let limit = request.limit.unwrap_or(5);
    if !(1..=100).contains(&limit) {
      return Err(SearchError::Invalid(
        "Search limit must be between 1 and 100.".into(),
      ));
    }
    let include = request
      .include_domains
      .iter()
      .map(|v| domain(v))
      .collect::<Result<Vec<_>, _>>()?;
    let exclude = request
      .exclude_domains
      .iter()
      .map(|v| domain(v))
      .collect::<Result<Vec<_>, _>>()?;
    let mut terms = vec![query.to_string()];
    if !include.is_empty() {
      let sites: Vec<_> = include
        .iter()
        .map(|domain| format!("site:{domain}"))
        .collect();
      terms.push(format!("({})", sites.join(" OR ")));
    }
    terms.extend(exclude.iter().map(|domain| format!("-site:{domain}")));
    let categories = category_queries(&request.categories)?;
    if !categories.is_empty() {
      terms.push(format!("({})", categories.join(" OR ")));
    }
    let range = match request.tbs.as_deref() {
      None | Some("") => None,
      Some("qdr:d") | Some("qdr:h") => Some("day"),
      Some("qdr:w") => Some("week"),
      Some("qdr:m") => Some("month"),
      Some("qdr:y") => Some("year"),
      Some(_) => {
        return Err(SearchError::Invalid(
          "Unsupported search time range.".into(),
        ))
      }
    };
    let query = terms.join(" ");
    let mut seen = HashSet::new();
    let mut items = Vec::new();
    for page in 1..=5 {
      let mut params = vec![
        ("q", query.clone()),
        ("format", "json".into()),
        ("pageno", page.to_string()),
      ];
      if let Some(range) = range {
        params.push(("time_range", range.into()));
      }
      let mut endpoint = self.endpoint.clone();
      endpoint.query_pairs_mut().extend_pairs(params);
      let response = self
        .http
        .get(endpoint)
        .send()
        .await
        .and_then(reqwest::Response::error_for_status)
        .map_err(|_| SearchError::Backend)?;
      let result: Response = response.json().await.map_err(|_| SearchError::Backend)?;
      if result.results.is_empty() {
        if items.is_empty() && !result.unresponsive_engines.is_empty() {
          return Err(SearchError::Backend);
        }
        break;
      }
      for item in result.results {
        let Ok(url) = Url::parse(&item.url) else {
          continue;
        };
        if crate::guard::check_url(&url, engine.policy()).is_err() {
          continue;
        }
        let Some(host) = url.host_str() else { continue };
        let host = host.trim_end_matches('.');
        if (!include.is_empty() && !include.iter().any(|domain| matches_domain(host, domain)))
          || exclude.iter().any(|domain| matches_domain(host, domain))
          || !seen.insert(url)
        {
          continue;
        }
        items.push(item);
        if items.len() == limit {
          break;
        }
      }
      if items.len() == limit {
        break;
      }
    }
    let items = stream::iter(items.into_iter().map(|item| {
      let options = request.scrape_options.as_ref();
      async move {
        let mut result = json!({"url": item.url, "title": item.title, "description": item.content});
        if let Some(options) = options {
          match engine.scrape(&item.url, options).await {
            Ok(Value::Object(page)) => {
              for (key, value) in page {
                result[key] = value;
              }
            }
            Ok(_) | Err(_) => result["warning"] = json!("Could not scrape the search result."),
          };
        }
        result
      }
    }))
    .buffered(3)
    .collect::<Vec<_>>()
    .await;
    Ok(json!({"success": true, "data": {"web": items}}))
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use axum::{routing::get, Json, Router};

  #[tokio::test]
  async fn filters_local_results_without_cloud() {
    let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0))
      .await
      .unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
      axum::serve(
        listener,
        Router::new().route(
          "/search",
          get(|| async {
            Json(json!({"results": [
              {"url": "https://docs.example.com/article", "title": "Keep", "content": "Snippet"},
              {"url": "https://docs.example.com/article", "title": "Duplicate"},
              {"url": "https://notexample.com/", "title": "Wrong domain"},
              {"url": "https://private.example.com/", "title": "Excluded"}
            ]}))
          }),
        ),
      )
      .await
      .unwrap();
    });
    let search = Search::new(Url::parse(&format!("http://{addr}/search")).unwrap()).unwrap();
    let engine = Engine::new(crate::scrape::EngineConfig {
      policy: crate::guard::NetworkPolicy::PublicOnly,
      user_agent: None,
      browser: crate::scrape::BrowserMode::Never,
    })
    .unwrap();
    let request = serde_json::from_value(json!({
      "query": "documents", "limit": 1, "includeDomains": ["example.com"], "excludeDomains": ["private.example.com"]
    })).unwrap();
    let value = search.run(&engine, request).await.unwrap();
    assert_eq!(
      value["data"]["web"],
      json!([{
        "url": "https://docs.example.com/article", "title": "Keep", "description": "Snippet"
      }])
    );
    assert!(matches!(
      domain("example.com/other"),
      Err(SearchError::Invalid(_))
    ));
    assert!(!matches_domain("notexample.com", "example.com"));
  }
}

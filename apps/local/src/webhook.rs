//! Deliver events for local crawl and batch jobs using the stored results.
use std::{collections::HashMap, sync::Arc, time::Duration};

use reqwest::header::{HeaderMap, HeaderName, HeaderValue};
use serde::Deserialize;
use serde_json::{json, Value};
use tokio::sync::{Semaphore, SemaphorePermit};
use url::Url;

use crate::{
  fetch::Fetcher,
  jobs::{Jobs, Kind},
  scrape::ScrapeError,
};

static DELIVERIES: Semaphore = Semaphore::const_new(20);

#[derive(Clone, Copy, Deserialize, PartialEq)]
#[serde(rename_all = "lowercase")]
enum Event {
  Started,
  Page,
  Completed,
  Failed,
}

impl Event {
  fn name(self) -> &'static str {
    match self {
      Self::Started => "started",
      Self::Page => "page",
      Self::Completed => "completed",
      Self::Failed => "failed",
    }
  }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Request {
  url: String,
  #[serde(default)]
  headers: HashMap<String, String>,
  #[serde(default)]
  metadata: HashMap<String, String>,
  events: Option<Vec<Event>>,
}

pub(crate) struct Delivery {
  url: Url,
  headers: HeaderMap,
  metadata: HashMap<String, String>,
  events: Option<Vec<Event>>,
  fetcher: Fetcher,
  _slot: SemaphorePermit<'static>,
}

impl Delivery {
  pub(crate) fn from_payload(
    value: Option<&Value>,
    fetcher: &Fetcher,
  ) -> Result<Option<Self>, ScrapeError> {
    let Some(value) = value else { return Ok(None) };
    let invalid = || {
      ScrapeError::new(
        400,
        "BAD_REQUEST",
        "Invalid webhook URL, headers or events.",
      )
    };
    let object = value
      .as_str()
      .map(|url| json!({ "url": url }))
      .unwrap_or_else(|| value.clone());
    let request: Request = serde_json::from_value(object).map_err(|_| invalid())?;
    let url = Url::parse(&request.url).map_err(|_| invalid())?;
    if !url.username().is_empty()
      || url.password().is_some()
      || crate::guard::check_url(&url, fetcher.policy()).is_err()
    {
      return Err(invalid());
    }
    let mut headers = HeaderMap::new();
    for (key, value) in request.headers {
      let name = HeaderName::from_bytes(key.as_bytes()).map_err(|_| invalid())?;
      let value = HeaderValue::from_str(&value).map_err(|_| invalid())?;
      if matches!(
        name.as_str(),
        "host" | "connection" | "content-length" | "transfer-encoding" | "x-firecrawl-signature"
      ) {
        return Err(invalid());
      }
      headers.insert(name, value);
    }
    let slot = DELIVERIES.try_acquire().map_err(|_| {
      ScrapeError::new(
        429,
        "WEBHOOK_CAPACITY",
        "Too many jobs have active webhooks.",
      )
    })?;
    Ok(Some(Self {
      url,
      headers,
      metadata: request.metadata,
      events: request.events,
      fetcher: fetcher.clone(),
      _slot: slot,
    }))
  }

  fn wants(&self, event: Event) -> bool {
    self
      .events
      .as_ref()
      .is_none_or(|events| events.contains(&event))
  }

  pub(crate) fn watch(self, jobs: Arc<Jobs>, kind: Kind, id: String) {
    tokio::spawn(async move {
      let mut started = false;
      let mut offset = 0;
      while let Ok(status) = jobs.status(kind, &id, offset) {
        if !status.queued {
          if !started {
            self.send(kind, &id, Event::Started, Vec::new()).await;
            started = true;
          }
          if self.wants(Event::Page) {
            if let Some(pages) = status.value.get("data").and_then(Value::as_array) {
              for page in pages {
                self.send(kind, &id, Event::Page, vec![page.clone()]).await;
                offset += 1;
              }
            }
            if status.next.is_some() {
              continue;
            }
          }
        }
        match status.value.get("status").and_then(Value::as_str) {
          Some("completed") => {
            self.send(kind, &id, Event::Completed, Vec::new()).await;
            break;
          }
          Some("failed" | "cancelled") => {
            self.send(kind, &id, Event::Failed, Vec::new()).await;
            break;
          }
          _ => tokio::time::sleep(Duration::from_millis(500)).await,
        }
      }
    });
  }

  async fn send(&self, kind: Kind, id: &str, event: Event, data: Vec<Value>) {
    if !self.wants(event) {
      return;
    }
    let prefix = match kind {
      Kind::Crawl => "crawl",
      Kind::Batch => "batch_scrape",
    };
    let mut payload = json!({
      "success": event != Event::Failed,
      "type": format!("{prefix}.{}", event.name()),
      "id": id,
      "webhookId": uuid::Uuid::new_v4().to_string(),
      "data": data,
      "metadata": self.metadata,
    });
    if event == Event::Failed {
      payload["error"] = json!("The job ended before it completed.");
    }
    for attempt in 0..3 {
      if attempt > 0 {
        tokio::time::sleep(Duration::from_millis(250 * attempt)).await;
      }
      if self
        .fetcher
        .send_webhook(&self.url, &self.headers, &payload)
        .await
      {
        return;
      }
    }
    eprintln!(
      "Webhook delivery failed for job {id}, event {}.",
      event.name()
    );
  }
}

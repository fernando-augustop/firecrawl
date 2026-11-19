use std::{
  collections::VecDeque,
  sync::Mutex,
  time::{Duration, Instant},
};

use serde_json::Value;
use sha2::{Digest, Sha256};

use crate::scrape::ScrapeOptions;

const CAPACITY: usize = 32 * 1024 * 1024;
const MAX_ENTRIES: usize = 256;
pub(crate) const DEFAULT_MAX_AGE: u64 = 2 * 24 * 60 * 60 * 1000;
type Key = [u8; 32];

#[derive(Default)]
pub(crate) struct Cache(Mutex<VecDeque<Entry>>);

struct Entry {
  key: Key,
  value: Value,
  size: usize,
  added: Instant,
}

impl Cache {
  pub(crate) fn key(url: &str, options: &ScrapeOptions) -> Option<Key> {
    if !options.headers.is_empty()
      || options
        .formats
        .iter()
        .any(|format| crate::extract::kind(format).is_some())
    {
      return None;
    }
    let mut options = options.clone();
    options.max_age = None;
    let mut hasher = Sha256::new();
    hasher.update(url.as_bytes());
    hasher.update([0]);
    hasher.update(serde_json::to_vec(&options).ok()?);
    Some(hasher.finalize().into())
  }

  pub(crate) fn get(&self, key: Key, max_age: u64) -> Option<Value> {
    if max_age == 0 {
      return None;
    }
    let entries = self.0.lock().ok()?;
    entries
      .iter()
      .find(|entry| entry.key == key && entry.added.elapsed() <= Duration::from_millis(max_age))
      .map(|entry| entry.value.clone())
  }

  pub(crate) fn insert(&self, key: Key, value: &Value) {
    if value.get("warning").is_some()
      || value.get("blocked").is_some()
      || !value
        .pointer("/metadata/statusCode")
        .and_then(Value::as_u64)
        .is_some_and(|status| (200..300).contains(&status))
    {
      return;
    }
    let Ok(size) = serde_json::to_vec(value).map(|value| value.len()) else {
      return;
    };
    if size > CAPACITY / 4 {
      return;
    }
    let Ok(mut entries) = self.0.lock() else {
      return;
    };
    entries.retain(|entry| {
      entry.key != key && entry.added.elapsed() < Duration::from_secs(7 * 24 * 60 * 60)
    });
    let mut used: usize = entries.iter().map(|entry| entry.size).sum();
    while used + size > CAPACITY || entries.len() >= MAX_ENTRIES {
      if let Some(entry) = entries.pop_front() {
        used -= entry.size;
      }
    }
    entries.push_back(Entry {
      key,
      value: value.clone(),
      size,
      added: Instant::now(),
    });
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use serde_json::json;

  #[test]
  fn bypasses_cache_for_fresh_requests_and_credentials() {
    let cache = Cache::default();
    let mut opts = ScrapeOptions::default();
    let url = "https://example.com";
    let key = Cache::key(url, &opts).unwrap();
    cache.insert(
      key,
      &json!({"markdown": "cached", "metadata": {"statusCode": 200}}),
    );
    assert_eq!(
      cache.get(key, DEFAULT_MAX_AGE).unwrap()["markdown"],
      "cached"
    );
    opts.max_age = Some(0);
    assert_eq!(Cache::key(url, &opts), Some(key));
    assert!(cache.get(key, 0).is_none());
    opts.headers.insert("Cookie".into(), "test-only".into());
    assert!(Cache::key(url, &opts).is_none());
  }
}

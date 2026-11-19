use std::{
  collections::HashMap,
  future::Future,
  panic::AssertUnwindSafe,
  path::Path,
  sync::{Arc, Mutex, MutexGuard},
};

use futures::FutureExt;
use rusqlite::{params, Connection, OptionalExtension};
use serde_json::{json, Value};
use tokio::{sync::Semaphore, task::AbortHandle};

use crate::scrape::ScrapeError;

const TTL_SECONDS: i64 = 60 * 60;
const MAX_STORED: i64 = 512;
const MAX_PENDING: i64 = 20;
const MAX_RESULT_BYTES: usize = 8 * 1024 * 1024;
const MAX_JOB_BYTES: usize = 48 * 1024 * 1024;
pub(crate) const MAX_PAGES: usize = 1000;

#[derive(Clone, Copy)]
pub(crate) enum Kind {
  Crawl,
  Batch,
}

impl Kind {
  fn name(self) -> &'static str {
    match self {
      Self::Crawl => "crawl",
      Self::Batch => "batch",
    }
  }
}

struct Runtime {
  db: Connection,
  tasks: HashMap<String, AbortHandle>,
}

pub(crate) struct Jobs {
  runtime: Mutex<Runtime>,
  slots: Semaphore,
  max_concurrency: usize,
}

pub(crate) struct Job {
  id: String,
  jobs: Arc<Jobs>,
}

pub(crate) struct Summary {
  pub active: i64,
  pub waiting: i64,
  pub max: usize,
  pub most_recent_success: Option<String>,
}

pub(crate) struct JobStatus {
  pub value: Value,
  pub next: Option<usize>,
  pub queued: bool,
}

#[derive(Default)]
pub(crate) struct Outcome {
  pub errors: Vec<Value>,
  pub robots_blocked: Vec<String>,
}

#[derive(Debug)]
pub(crate) enum Error {
  Storage,
  NotFound,
  Full,
}

impl From<rusqlite::Error> for Error {
  fn from(_: rusqlite::Error) -> Self {
    Self::Storage
  }
}

fn now() -> i64 {
  chrono::Utc::now().timestamp()
}

fn timestamp(seconds: i64) -> Option<String> {
  chrono::DateTime::from_timestamp(seconds, 0).map(|date| date.to_rfc3339())
}

impl Jobs {
  pub(crate) fn open(path: Option<&Path>, max_concurrency: usize) -> Result<Arc<Self>, Error> {
    let db = match path {
      Some(path) => Connection::open(path)?,
      None => Connection::open_in_memory()?,
    };
    db.execute_batch(
      "
      PRAGMA foreign_keys = ON;
      PRAGMA journal_mode = WAL;
      PRAGMA synchronous = NORMAL;
      CREATE TABLE IF NOT EXISTS jobs (
        id TEXT PRIMARY KEY, kind TEXT NOT NULL, status TEXT NOT NULL, created INTEGER NOT NULL,
        finished INTEGER, total INTEGER NOT NULL, bytes INTEGER NOT NULL DEFAULT 0,
        errors TEXT NOT NULL DEFAULT '[]', robots_blocked TEXT NOT NULL DEFAULT '[]'
      );
      CREATE TABLE IF NOT EXISTS job_pages (
        job_id TEXT NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
        idx INTEGER PRIMARY KEY, payload TEXT NOT NULL
      );
      CREATE INDEX IF NOT EXISTS job_pages_by_job ON job_pages(job_id, idx);
      DELETE FROM jobs WHERE created <= unixepoch() - 3600;
    ",
    )?;
    db.execute(
      "UPDATE jobs SET status = 'failed', finished = ?1, errors = ?2 WHERE status IN ('queued', 'scraping')",
      params![now(), r#"[{"code":"SERVER_RESTARTED","error":"The server restarted before this job finished."}]"#],
    )?;
    Ok(Arc::new(Self {
      runtime: Mutex::new(Runtime {
        db,
        tasks: HashMap::new(),
      }),
      slots: Semaphore::new(max_concurrency.max(1)),
      max_concurrency: max_concurrency.max(1),
    }))
  }

  fn locked(&self) -> Result<MutexGuard<'_, Runtime>, Error> {
    self.runtime.lock().map_err(|_| Error::Storage)
  }

  pub(crate) fn purge(&self) -> Result<(), Error> {
    let mut runtime = self.locked()?;
    let cutoff = now() - TTL_SECONDS;
    let expired = {
      let mut query = runtime
        .db
        .prepare("SELECT id FROM jobs WHERE created <= ?1")?;
      let rows = query.query_map([cutoff], |row| row.get::<_, String>(0))?;
      rows.collect::<Result<Vec<_>, _>>()?
    };
    runtime
      .db
      .execute("DELETE FROM jobs WHERE created <= ?1", [cutoff])?;
    for id in expired {
      if let Some(task) = runtime.tasks.remove(&id) {
        task.abort();
      }
    }
    Ok(())
  }

  pub(crate) fn start<F, Task>(
    self: &Arc<Self>,
    kind: Kind,
    total: usize,
    run: F,
  ) -> Result<String, Error>
  where
    F: FnOnce(Job) -> Task + Send + 'static,
    Task: Future<Output = Result<Outcome, ScrapeError>> + Send,
  {
    self.purge()?;
    let mut runtime = self.locked()?;
    let (stored, pending): (i64, i64) = runtime.db.query_row(
      "SELECT count(*), count(CASE WHEN status IN ('queued', 'scraping') THEN 1 END) FROM jobs",
      [],
      |row| Ok((row.get(0)?, row.get(1)?)),
    )?;
    if stored >= MAX_STORED || pending >= MAX_PENDING {
      return Err(Error::Full);
    }
    let id = uuid::Uuid::new_v4().to_string();
    runtime.db.execute(
      "INSERT INTO jobs(id, kind, status, created, total) VALUES(?1, ?2, 'queued', ?3, ?4)",
      params![id, kind.name(), now(), total as i64],
    )?;
    let jobs = Arc::clone(self);
    let task_id = id.clone();
    let task = tokio::spawn(async move {
      let Ok(_slot) = jobs.slots.acquire().await else {
        return;
      };
      if jobs.begin(&task_id).ok() != Some(true) {
        return;
      }
      let job = Job {
        id: task_id.clone(),
        jobs: jobs.clone(),
      };
      let result = AssertUnwindSafe(async move { run(job).await })
        .catch_unwind()
        .await
        .unwrap_or_else(|_| {
          Err(ScrapeError::new(
            500,
            "JOB_FAILED",
            "The background job failed.",
          ))
        });
      let _ = jobs.finish(&task_id, result);
    });
    runtime.tasks.insert(id.clone(), task.abort_handle());
    Ok(id)
  }

  fn begin(&self, id: &str) -> Result<bool, Error> {
    Ok(
      self.locked()?.db.execute(
        "UPDATE jobs SET status = 'scraping' WHERE id = ?1 AND status = 'queued'",
        [id],
      )? == 1,
    )
  }

  fn finish(&self, id: &str, result: Result<Outcome, ScrapeError>) -> Result<(), Error> {
    let (status, outcome) = match result {
      Ok(outcome) => ("completed", outcome),
      Err(err) => (
        "failed",
        Outcome {
          errors: vec![json!({"code": err.code, "error": err.message})],
          ..Default::default()
        },
      ),
    };
    let mut runtime = self.locked()?;
    let result = runtime.db.execute(
      "UPDATE jobs SET status = ?2, finished = ?3, errors = ?4, robots_blocked = ?5 WHERE id = ?1 AND status = 'scraping'",
      params![id, status, now(), json!(outcome.errors).to_string(), json!(outcome.robots_blocked).to_string()],
    );
    runtime.tasks.remove(id);
    result?;
    Ok(())
  }

  pub(crate) fn status(&self, kind: Kind, id: &str, offset: usize) -> Result<JobStatus, Error> {
    let runtime = self.locked()?;
    let job = runtime
      .db
      .query_row(
        "SELECT status, created, total, (SELECT count(*) FROM job_pages WHERE job_id = jobs.id)
        FROM jobs WHERE id = ?1 AND kind = ?2 AND created > ?3",
        params![id, kind.name(), now() - TTL_SECONDS],
        |row| {
          Ok((
            row.get::<_, String>(0)?,
            row.get::<_, i64>(1)?,
            row.get::<_, i64>(2)?,
            row.get::<_, i64>(3)?,
          ))
        },
      )
      .optional()?;
    let Some((status, created, total, completed)) = job else {
      return Err(Error::NotFound);
    };
    let mut query = runtime.db.prepare(
      "SELECT payload FROM job_pages WHERE job_id = ?1 ORDER BY idx LIMIT 100 OFFSET ?2",
    )?;
    let offset = offset.min(MAX_PAGES);
    let mut rows = query.query(params![id, offset as i64])?;
    let mut data = Vec::new();
    let mut bytes = 0;
    while let Some(row) = rows.next()? {
      let payload = row.get_ref(0)?.as_str().map_err(|_| Error::Storage)?;
      if bytes + payload.len() > MAX_RESULT_BYTES {
        break;
      }
      bytes += payload.len();
      data.push(serde_json::from_str::<Value>(payload).map_err(|_| Error::Storage)?);
    }
    let next = (offset + data.len() < completed as usize).then_some(offset + data.len());
    Ok(JobStatus {
      value: json!({
        "success": true, "status": if status == "queued" { "scraping" } else { &status },
        "total": total, "completed": completed, "creditsUsed": 0,
        "expiresAt": timestamp(created + TTL_SECONDS),
        "expiresInSeconds": (created + TTL_SECONDS - now()).max(0),
        "data": data
      }),
      next,
      queued: status == "queued",
    })
  }

  pub(crate) fn errors(&self, kind: Kind, id: &str) -> Result<Value, Error> {
    let runtime = self.locked()?;
    let (errors, robots) = runtime
      .db
      .query_row(
        "SELECT errors, robots_blocked FROM jobs WHERE id = ?1 AND kind = ?2 AND created > ?3",
        params![id, kind.name(), now() - TTL_SECONDS],
        |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
      )
      .optional()?
      .ok_or(Error::NotFound)?;
    Ok(json!({
      "success": true,
      "errors": serde_json::from_str::<Value>(&errors).map_err(|_| Error::Storage)?,
      "robotsBlocked": serde_json::from_str::<Value>(&robots).map_err(|_| Error::Storage)?
    }))
  }

  pub(crate) fn cancel(&self, kind: Kind, id: &str) -> Result<bool, Error> {
    let mut runtime = self.locked()?;
    let status: String = runtime
      .db
      .query_row(
        "SELECT status FROM jobs WHERE id = ?1 AND kind = ?2 AND created > ?3",
        params![id, kind.name(), now() - TTL_SECONDS],
        |row| row.get(0),
      )
      .optional()?
      .ok_or(Error::NotFound)?;
    if status == "cancelled" {
      return Ok(true);
    }
    if !matches!(status.as_str(), "scraping" | "queued") {
      return Ok(false);
    }
    runtime.db.execute(
      "UPDATE jobs SET status = 'cancelled', finished = ?2 WHERE id = ?1",
      params![id, now()],
    )?;
    if let Some(task) = runtime.tasks.remove(id) {
      task.abort();
    }
    Ok(true)
  }

  pub(crate) fn summary(&self) -> Result<Summary, Error> {
    let (active, waiting, success): (i64, i64, Option<i64>) = self.locked()?.db.query_row(
      "SELECT count(CASE WHEN status = 'scraping' THEN 1 END), count(CASE WHEN status = 'queued' THEN 1 END),
        max(CASE WHEN status = 'completed' THEN finished END) FROM jobs WHERE created > ?1",
      [now() - TTL_SECONDS], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
    )?;
    Ok(Summary {
      active,
      waiting,
      max: self.max_concurrency,
      most_recent_success: success.and_then(timestamp),
    })
  }

  pub(crate) fn active_crawls(&self) -> Result<Value, Error> {
    let runtime = self.locked()?;
    let mut query = runtime.db.prepare(
      "SELECT id, created FROM jobs WHERE kind = 'crawl' AND status IN ('queued', 'scraping') AND created > ?1 ORDER BY created",
    )?;
    let rows = query.query_map([now() - TTL_SECONDS], |row| {
      Ok(json!({"id": row.get::<_, String>(0)?, "createdAt": timestamp(row.get(1)?)}))
    })?;
    Ok(json!({"success": true, "crawls": rows.collect::<Result<Vec<_>, _>>()?}))
  }
}

impl Job {
  pub(crate) fn append(&self, value: &Value) -> Result<(), ScrapeError> {
    let store_error =
      |_| ScrapeError::new(500, "JOB_STORAGE_FAILED", "Could not write a job result.");
    let payload = value.to_string();
    if payload.len() > MAX_RESULT_BYTES {
      return Err(ScrapeError::new(
        413,
        "JOB_RESULT_TOO_LARGE",
        "A job result exceeded the page size limit.",
      ));
    }
    let mut runtime = self.jobs.locked().map_err(store_error)?;
    let tx = runtime
      .db
      .transaction()
      .map_err(|_| store_error(Error::Storage))?;
    let wrote = tx
      .execute(
        "UPDATE jobs SET bytes = bytes + ?2, total = total + (kind = 'crawl')
        WHERE id = ?1 AND status = 'scraping' AND bytes + ?2 <= ?3",
        params![self.id, payload.len() as i64, MAX_JOB_BYTES as i64],
      )
      .map_err(|_| store_error(Error::Storage))?;
    if wrote == 0 {
      return Err(ScrapeError::new(
        413,
        "JOB_RESULT_LIMIT",
        "The job is inactive or exceeded the total result size limit.",
      ));
    }
    tx.execute(
      "INSERT INTO job_pages(job_id, payload) VALUES (?1, ?2)",
      params![self.id, payload],
    )
    .map_err(|_| store_error(Error::Storage))?;
    tx.commit().map_err(|_| store_error(Error::Storage))
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  async fn wait_completed(jobs: &Jobs, id: &str) {
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
      loop {
        if jobs.status(Kind::Crawl, id, 0).unwrap().value["status"] == "completed" {
          break;
        }
        tokio::task::yield_now().await;
      }
    })
    .await
    .unwrap();
  }

  #[tokio::test]
  async fn pages_survive_restart_and_return_a_cursor() {
    let dir = std::env::temp_dir().join(format!("firecrawl-jobs-test-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir(&dir).unwrap();
    let path = dir.join("jobs.sqlite");
    let jobs = Jobs::open(Some(&path), 1).unwrap();
    let id = jobs
      .start(Kind::Crawl, 0, |job| async move {
        for n in 0..105 {
          job.append(&json!({"n": n}))?;
        }
        Ok(Outcome::default())
      })
      .unwrap();
    wait_completed(&jobs, &id).await;
    jobs.locked().unwrap().db.execute(
      "INSERT INTO jobs(id, kind, status, created, total) VALUES('interrupted', 'crawl', 'scraping', ?1, 0)", [now()]
    ).unwrap();
    drop(jobs);
    let restored = Jobs::open(Some(&path), 1).unwrap();
    let first = restored.status(Kind::Crawl, &id, 0).unwrap();
    assert_eq!(first.value["completed"], 105);
    assert_eq!(first.next, Some(100));
    let last = restored.status(Kind::Crawl, &id, 100).unwrap();
    assert_eq!(last.value["data"].as_array().unwrap().len(), 5);
    assert_eq!(last.value["data"][0]["n"], 100);
    assert!(last.next.is_none());
    assert_eq!(
      restored
        .status(Kind::Crawl, "interrupted", 0)
        .unwrap()
        .value["status"],
      "failed"
    );
    assert_eq!(
      restored.errors(Kind::Crawl, "interrupted").unwrap()["errors"][0]["code"],
      "SERVER_RESTARTED"
    );
    drop(restored);
    std::fs::remove_dir_all(dir).unwrap();
  }

  #[tokio::test]
  async fn cancel_is_terminal_and_kind_specific() {
    let jobs = Jobs::open(None, 1).unwrap();
    let id = jobs
      .start(Kind::Batch, 0, |_| std::future::pending())
      .unwrap();
    assert!(matches!(
      jobs.cancel(Kind::Crawl, &id),
      Err(Error::NotFound)
    ));
    assert!(jobs.cancel(Kind::Batch, &id).unwrap());
    assert!(jobs.cancel(Kind::Batch, &id).unwrap());
    assert_eq!(
      jobs.status(Kind::Batch, &id, 0).unwrap().value["status"],
      "cancelled"
    );
    assert_eq!(jobs.summary().unwrap().active, 0);
    assert_eq!(jobs.summary().unwrap().waiting, 0);
  }
}

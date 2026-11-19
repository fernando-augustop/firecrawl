# Running the fork on Linux

Build from the repository root with `cargo build --manifest-path apps/local/Cargo.toml --locked --release`. Install the binary as `/opt/firecrawl-local/bin/firecrawl-local` and copy the service unit to `/etc/systemd/system/`. Install a Chromium package with a working user-namespace sandbox. The service runs as a dedicated systemd user and stores job results in SQLite.

Create `/etc/firecrawl-local/service.env` for the deployment. This example uses ports for a parallel test installation:

```dotenv
FIRECRAWL_CHROME_PATH=/usr/lib/chromium/chromium
FIRECRAWL_LOCAL_PORT=13013
FIRECRAWL_LOCAL_ALLOWED_HOST=crawler.example.internal
FIRECRAWL_LOCAL_PUBLIC_URL=https://crawler.example.internal:13013
FIRECRAWL_LOCAL_SEARXNG_ENDPOINT=http://127.0.0.1:13015/search
```

Set `search.formats` in the SearXNG settings to include `json`. Keep the SearXNG port local and configure its own server secret. Put any extraction credentials in root-readable `/etc/firecrawl-local/extraction.env` with mode 0600. The optional OpenAI-compatible integration uses `FIRECRAWL_LOCAL_LLM_ENDPOINT`, `FIRECRAWL_LOCAL_LLM_MODEL`, and `FIRECRAWL_LOCAL_LLM_KEY`. Only explicit `json`, `summary`, or `question` requests send scraped text to that provider.

Reload systemd and use `systemctl enable --now firecrawl-local-fork.service`. Serve the API through a private proxy and pass the configured Host header. Check `/health`, `/ready`, and `/v2/search` through the proxy. Cold Chromium startup is expected for the first rendering request after 45 seconds of browser inactivity.

The fork provides local v2 search, v1/v2 scrape, map, crawl and batch endpoints, SQLite result pagination, and optional extraction and crawl parameter generation. Job results expire after one hour. Interrupted jobs retain any completed pages and expose a restart error. Webhook delivery follows the configured network policy and retries transient failures within the running process. Search quality and quotas depend on the configured engines.

The implementation covers a subset of the hosted Firecrawl API, including selected scrape formats and PDF options. Custom request headers are scoped to the requested origin. When enabled on a request, `skipTlsVerification` applies to a separate browser pool. Browser actions, cloud-specific products, and hosted anti-bot capabilities require their upstream services.

For an upgrade, keep the running binary and its environment to roll back. Test the replacement and dependent clients on the parallel port, then change only the private proxy target and the local endpoint used by clients. Drain in-flight jobs before an endpoint switch. Keep the previous job database and search configuration until rollback is no longer needed.

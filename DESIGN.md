# Design decisions

Rationale and trade-offs for the Cloudsmith take-home. Data flow: [ARCHITECTURE.md](ARCHITECTURE.md). How to run: [README.md](README.md).

---

## Evaluation mapping

| Criterion | What we did | Evidence |
|-----------|-------------|----------|
| **Code structure** | API, ingest, and domain models separated; schema in `EventsTable` / `EventRecord` | `api/`, `backend/processing/`, `backend/models/events/` |
| **Data handling** | Pydantic validation; DLQ; dedupe by `event_id` (earliest `timestamp`); out-of-order OK | `ingest.py`, `EventsTable._upsert_partition`, `tests/test_ingestion.py` |
| **Tenant isolation** | Hive `tenant_id=` partitions; scoped reads + SQL guard | `EventsTable.list_events`, `tests/test_tenant_isolation.py` |
| **API design** | Tenant in path; filter query params; Pydantic → 422 | `api/app.py`, `tests/test_api.py` |
| **Tests** | Ingest, dedupe, DLQ, filters, isolation, retention | `tests/` |

| Exercise requirement | How we addressed it |
|---------------------|-------------------|
| Ingestion | JSONL → validate → DLQ → dedupe → Parquet |
| Duplicates | `row_number()` merge; earliest `timestamp` wins |
| Malformed | Rejects to `rejects.parquet` with reason |
| Tenant isolation | Partition path + SQL + API path |
| Query filters | `start_time`, `end_time`, `action`, `package`, pagination |
| Retention | Delete `event_date=` dirs past `retention_days` |

---

## 1. Parquet data lake (no persistent DuckDB)

**Decision:** Durable storage is hive-partitioned Parquet under `data/data_lake/`. DuckDB is in-memory only — no tables, no `audit.duckdb` — used for `read_parquet`, merge SQL, and `COPY`.

**Pros:** Single on-disk source of truth; tenant/date folders match API and retention; no external DB to run; fits batch ingest within the time box.

**Cons:** Partition rewrite on ingest is slower than OLTP upsert; many small files without compaction; API scans files — latency grows with partition count.

**Alternatives considered:**

| Approach | Why not here |
|----------|----------------|
| DuckDB persistent tables + export | Two stores; sync overhead |
| Append-only Parquet, dedupe on read | Duplicates on disk; every query pays |
| Postgres / SQLite primary | Extra service; OLTP-shaped for an analytics audit log |

---

## 2. Partition merge + `tenant_id` / `event_date` keys

**Decision:** Per affected `(tenant_id, event_date)`: union new rows with existing Parquet, dedupe (`row_number()` by `event_id`, earliest `timestamp`), atomic swap via `data_lake_tmp`. Single curated stage — no bronze layer in repo.

**Pros:** Clean data at query time; handles duplicates and out-of-order events; atomic swap safe for readers; only **touched** partitions rewrite (usually today, not full history).

**Cons:** O(partition size) per rewrite; whole day replaced per touch; hot-day ingest cost grows with volume.

**Alternatives considered:**

| Approach | Why not here |
|----------|----------------|
| Append-only + compaction job | Valid at scale; more moving parts for ~2h scope |
| Full medallion (bronze/silver/gold) | Right long-term; overkill for one event type now |
| Hourly partitions on curated lake | Merge-on-write would rewrite the hot hour constantly; daily is enough for date-range API filters |

**Partition keys:** Tenant first (every query is tenant-scoped), then `event_date` from event `timestamp` UTC (not wall clock) — enables path pruning on time filters and retention by dropping date folders. Hourly buckets: reasonable on **append-only bronze** later; keep silver daily unless moving to Delta/Iceberg.

---

## 3. Validation (`EventRecord`)

**Decision:** One Pydantic model for JSONL ingest and API responses. Invalid rows → DLQ. Columns and partition keys in `EventsTable`, not config.

**Pros:** No schema drift between ingest, API, and tests; OpenAPI from types; field-level 422 errors.

**Cons:** Schema changes need code deploy; no runtime schema plug-in.

**Alternatives considered:** JSON Schema in config (flexible but drifts from code); validate only at ingest (API shape could diverge).

**Rules:** `tenant_id` `^[a-zA-Z0-9_]+$`; `action` download \| upload \| delete; ISO-8601 `timestamp`; required `event_id`, `tenant_id`, `action`, `package`, `timestamp`, `actor`.

---

## 4. Dead-letter queue

**Decision:** Rejects go to `data/dlq/events/rejects.parquet` with `source_file`, `line_number`, `raw_line`, `reason`, `rejected_at`.

**Pros:** Bad lines never enter tenant partitions; original line preserved; same Parquet stack as events.

**Cons:** DLQ is a growing file; no replay workflow in scope.

**Alternatives considered:** Drop malformed silently (fails exercise); inline error file per batch (harder to query).

---

## 5. Dedupe semantics

**Decision:** Same `event_id` → keep earliest `timestamp`. In-file first, then on partition merge. `inserted` = new `event_id`s not already in lake.

**Pros:** Matches exercise duplicate/out-of-order cases; no sorted input required; late duplicate with earlier ts can correct stored row.

**Cons:** Merge must read existing partition; cross-partition same `event_id` is unlikely but would not dedupe across dates.

**Alternatives considered:** Last-write-wins (wrong for audit); dedupe only in-file (duplicates across batches remain).

---

## 6. Tenant isolation

**Decision:** Enforce at storage path (`tenant_id={id}/**`), SQL (`WHERE tenant_id = ?`), API path (`/tenants/{tenant_id}/events`), and tests.

**Pros:** Defence in depth; path pruning avoids reading other tenants' files; easy to explain in review.

**Cons:** No auth — any caller can pass any `tenant_id` (exercise scope).

**Alternatives considered:** SQL-only filter without path layout (risk if glob too broad); shared table with RLS in Postgres (different architecture).

---

## 7. Retention

**Decision:** Delete `event_date=YYYY-MM-DD` directories when date &lt; `today(UTC) - retention_days` (default 90).

**Pros:** Meets exercise requirement; O(days) per tenant; no row deletes in Parquet.

**Cons:** Whole calendar day retained or dropped together; no hour-level policy.

**Alternatives considered:** Row-level delete in Parquet (slow/awkward); TTL in a database (needs primary DB).

---

## 8. Query API

**Decision:** `GET /tenants/{tenant_id}/events` — tenant required in path; optional `start_time`, `end_time`, `action`, `package`; `limit`/`offset`. Reads via `EventsTable.list_events()` on Parquet.

**Pros:** Tenant isolation is structural; filters map to SQL; shared storage layer with ingest; OpenAPI documented.

**Cons:** Parquet scan per request — fine for take-home scale, not sub-second at huge volume.

**Alternatives considered:** GraphQL (more flexible, more scope); POST body filters (works but less REST-obvious for tenant resource).

---

## 9. Extensible pipeline pattern

**Decision:** `events` pipeline: `EventsTable` + `EventRecord` + registry in `db/tables.py`. Same validate → DLQ → merge flow reusable for other event types.

**Pros:** Add a new artifact event type without rewriting ingest mechanics; tenant/partition rules stay consistent.

**Cons:** New type still needs code (table + model), not config-only.

**Alternatives considered:** One giant polymorphic table (messy schema); separate microservice per event type (heavy for take-home).

---

## 10. Ingest rate control (N files per run)

**Decision:** Each `audit-ingest` run processes at most **`ingest_batch_size`** landing JSONL files (default **5**, from `config/events.config`). Pending files without a `.processed` marker are picked in sorted filename order; orphans with a stale `.processing` marker are retried first. Operators re-run the CLI until `ingest_complete: true`, or use optional **`--watch`** to poll landing on an interval (`watch_interval_seconds` in config, overridable with `--interval`).

**Pros:** Caps work per invocation — bounded memory, partition rewrites, and DLQ writes per cycle; demonstrates incremental batching after `audit-split-input`; `inventory_before` / `inventory_after` make backlog visible; idempotent markers mean safe retries; watch mode sleeps between batch cycles (rate-limited polling for dev).

**Cons:** Not real-time — latency is “next batch cycle” (sleep + up to `ingest_batch_size` files); file-level granularity only; multiple concurrent ingest processes on the same landing dir are not safe; large backlogs need many cycles unless `ingest_batch_size` is raised or `--watch` sleep is lowered.

**Alternatives considered:**

| Approach | Why not here |
|----------|----------------|
| Ingest all landing files in one run | Simple for tiny demos; no rate control story; one failure or huge file blocks the whole run |
| Line-level streaming (Kafka, etc.) | Right at scale; out of scope for file-drop JSONL exercise |
| Cron only (no `ingest_batch_size`) | Still need a per-run cap if each cron tick shouldn't drain 1 000 batches |
| `--watch` as default | Surprising for scripts/CI; one-shot ingest remains the default |

**Config knobs:** `ingest_batch_size` (files per cycle), `watch_interval_seconds` (sleep between batch cycles when `--watch` is set).

---

## Future improvements

**Delta Lake / Iceberg** — Replace full partition rewrites with native merge/upsert on the same `tenant_id` + `event_date` layout. *Why:* ingest cost on hot days is the main scaling pain; table format fixes it without changing API or folder semantics.

**Medallion (bronze → silver → gold)** — Bronze keeps raw JSONL/Parquet for replay; silver stays deduped facts (today's lake); gold adds rollups. *Why:* audit compliance often needs immutable raw lineage separate from queryable facts.

**Compaction** — Coalesce small `part_*.parquet` within each partition on a schedule. *Why:* many ingest runs create file sprawl that slows API `read_parquet` even when row count is modest.

**Hot tier for recent queries** — e.g. last 7 days in Postgres/Redis; older data stays Parquet. Route in `list_events()` by `timestamp`. *Why:* audit UIs hit “today” repeatedly; cold Parquet is fine for historical search.

**Schema versioning** — `events_v2.jsonl`, parallel `EventRecord` models, migration on ingest. *Why:* production event shapes change; version avoids breaking existing partitions.

**Auth (JWT / API keys)** — Bind credential `tenant_id` to URL `{tenant_id}`. *Why:* path-only isolation is not security; exercise deliberately skipped it.

**Delta/Iceberg before hourly silver partitions** — If daily merge still hurts, upgrade table format first; use hourly only on append-only bronze. *Why:* hourly + merge-on-write multiplies rewrite churn on the curated path.

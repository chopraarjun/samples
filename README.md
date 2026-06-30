# Artifact Access Audit Service

A Python service that ingests artifact access events from JSONL, validates and dedupes them, stores them in hive-partitioned Parquet, and exposes a tenant-scoped query API.

Built for the Cloudsmith engineering take-home exercise — ingest imperfect multi-tenant `events.jsonl`, store with tenant isolation, query via API, and apply retention.

**Further reading:** [ARCHITECTURE.md](ARCHITECTURE.md) (components & data flow) · [DESIGN.md](DESIGN.md) (decisions & trade-offs)

![System overview](docs/images/system-overview.svg)

---

## Exercise requirements — how we addressed them

| Requirement | Approach | Where to look |
|-------------|----------|---------------|
| **1. Ingestion** — read `events.jsonl`, handle duplicates & malformed rows | CLI reads `data/landing/events/*.jsonl`; Pydantic validates each line; malformed JSON and invalid fields → DLQ Parquet; valid rows deduped by `event_id` (earliest `timestamp` wins); partition upsert into data lake | `backend/processing/ingestion/`, `EventRecord`, `EventsTable` |
| **2. Tenant isolation** — partition by tenant; no cross-tenant reads | Hive partitions `tenant_id=*/event_date=*/`; queries scoped to one tenant path + SQL `WHERE tenant_id = ?`; API requires `tenant_id` in URL | `EventsTable.list_events`, `GET /tenants/{tenant_id}/events`, `tests/test_tenant_isolation.py` |
| **3. Query API** — filter by tenant (required), time range, action, package | FastAPI read-only service; optional `start_time`, `end_time`, `action` (`download` \| `upload` \| `delete`), `package`; pagination via `limit` / `offset` | `api/app.py`, `EventQueryParams` |
| **4. Retention** — configurable cleanup of old events | `audit-retention` deletes `event_date=` partitions older than `retention_days` (default 90) | `backend/processing/retention/`, `config/events.config` |

**Imperfect data in `events.jsonl`**

| Issue | Handling |
|-------|----------|
| Duplicate `event_id` | In-file dedupe, then merge dedupe on upsert (`row_number()` — earliest timestamp kept) |
| Out-of-order timestamps | No ordering assumed at ingest; dedupe rule picks canonical row by earliest timestamp |
| Malformed JSON / invalid fields | Line skipped; row appended to `data/dlq/events/rejects.parquet` with `source_file`, `line_number`, `reason` |

**What we're evaluated on** (per exercise brief) — see [DESIGN.md § Evaluation mapping](DESIGN.md#evaluation-mapping):

- **Code structure** — API vs `processing/` CLIs vs `models/` + config
- **Data handling** — validation, DLQ, dedupe semantics
- **Tenant isolation** — path + SQL + API + tests
- **API design** — REST tenant path, typed query params, OpenAPI
- **Tests** — ingest, dedupe, DLQ, API filters, tenant isolation, retention (not coverage %)

---

## Requirements

- Python 3.11+

## Setup

From the project root:

```bash
python -m venv .venv
.venv\Scripts\activate        # Windows
# source .venv/bin/activate   # macOS/Linux

pip install -e ".[dev]"
```

`pip install -e .` registers the `audit-*` command shortcuts. Without that step, run the same logic via `python -m …` (from project root, with `src` on `PYTHONPATH`):

```bash
# Windows
set PYTHONPATH=src

# macOS/Linux
# export PYTHONPATH=src
```

Re-run `pip install -e .` after pulling changes that move modules or CLI entry points.

---

## Quick start (end-to-end)

From the project root after [setup](#setup). Set `PYTHONPATH=src` if you skipped `pip install -e .` (Windows: `set PYTHONPATH=src`). Shorthand `audit-*` commands need `pip install -e .`.

### 1. Put `events.jsonl` in the landing directory

Get JSONL into `data/landing/events/` before ingest. **Split** (recommended) creates many small batches; **copy only** skips split and ingests one file in a single run.

#### Option A — Split into batches

Break the source file into `batch_20260630T215014Z_0001.jsonl`, `batch_20260630T215014Z_0002.jsonl`, … for incremental ingest (5 files per `audit-ingest` run by default). Each split run shares one UTC timestamp; sequence numbers restart at `_0001`.

**Command**

```bash
python -m audit_service.backend.processing.ingestion.split_input <pipeline> <source.jsonl> [--lines-per-file N] [--overwrite | --append]
# audit-split-input <pipeline> <source.jsonl> [--lines-per-file N] [--overwrite | --append]
```

| If landing already has `batch_*.jsonl` | Landing files | Tracking markers | Data lake |
|----------------------------------------|---------------|------------------|-----------|
| *(default)* | **Fail** | unchanged | unchanged |
| `--overwrite` | **Overwrite** — delete batches, split fresh timestamped group from `_0001` | remove markers **only for deleted batch files** | unchanged |
| `--append` | **Add** — new files after highest batch number | unchanged | unchanged |

`--append` is not overwrite. For a full wipe of lake + all markers, use `audit-reset events -y` (see Reset below).

Stdout JSON includes `mode`, `lines_read`, `lines_written`, skips, `batch_files`, `tracking_markers_removed`, etc.

**Example**

```bash
python -m audit_service.backend.processing.ingestion.split_input events events.jsonl
# audit-split-input events events.jsonl
```

Replace existing batches (overwrite landing + clear their markers):

```bash
python -m audit_service.backend.processing.ingestion.split_input events events.jsonl --overwrite
# audit-split-input events events.jsonl --overwrite
```

Add more batches (keep existing landing files and markers):

```bash
python -m audit_service.backend.processing.ingestion.split_input events events.jsonl --append
# audit-split-input events events.jsonl --append
```

#### Option B — Copy only (no split)

Single file in landing — one ingest run processes the whole file. Fine for a quick demo; skip batching behaviour.

**Command**

```bash
mkdir -p data/landing/events
cp <source.jsonl> data/landing/events/
# Windows: mkdir data\landing\events & copy events.jsonl data\landing\events\
```

**Example**

```bash
mkdir -p data/landing/events
cp events.jsonl data/landing/events/events.jsonl
# Windows: mkdir data\landing\events & copy events.jsonl data\landing\events\events.jsonl
```

---

### Reset (optional — before a clean re-run)

Clears **data lake + DLQ + all tracking markers** in one command. Does not ingest.

**Command**

```bash
python -m audit_service.backend.processing.reset <pipeline> [-y] [--landing]
# audit-reset <pipeline> [-y] [--landing]
```

**Example**

```bash
python -m audit_service.backend.processing.reset events -y
# audit-reset events -y

python -m audit_service.backend.processing.ingestion.ingest events
# audit-ingest events
```

`--landing` also deletes `batch_*.jsonl` in landing (use before a fresh split).

---

### 2. Ingest landing files into the data lake

Validate, dedupe, and write Parquet. Default: **5 files per run** — repeat until JSON output shows `"ingest_complete": true`.

**Command**

```bash
python -m audit_service.backend.processing.ingestion.ingest <pipeline> [--watch [--interval SECS]] [--log-mode off|console|file|both] [--log-level LEVEL]
# audit-ingest <pipeline> [--watch [--interval SECS]] [--log-mode ...] [--log-level LEVEL]
```

**Example**

```bash
python -m audit_service.backend.processing.ingestion.ingest events
# audit-ingest events

python -m audit_service.backend.processing.ingestion.ingest events --watch
# audit-ingest events --watch
```

Run again until complete if you used **split** (~10 batches → at least 2 runs), or use **`--watch`** to poll automatically. **Copy only** usually needs one run. Logs: `data/logs/events/`.

---

### 3. Start the query API

**Command**

```bash
python -m uvicorn audit_service.main:app --reload --host 127.0.0.1 --port 8000
# uvicorn audit_service.main:app --reload --host 127.0.0.1 --port 8000
```

**Swagger UI:** [http://127.0.0.1:8000/docs](http://127.0.0.1:8000/docs) — interactive API explorer (Try it out on each endpoint).

**Example**

```bash
python -m uvicorn audit_service.main:app --reload --host 127.0.0.1 --port 8000
```

---

### 4. Query events for a tenant

Open these in a browser after starting the API. Prefer **Swagger** to try filters interactively: [http://127.0.0.1:8000/docs](http://127.0.0.1:8000/docs)

**URLs**

| Endpoint | URL |
|----------|-----|
| Swagger UI | [http://127.0.0.1:8000/docs](http://127.0.0.1:8000/docs) |
| ReDoc | [http://127.0.0.1:8000/redoc](http://127.0.0.1:8000/redoc) |
| Health | [http://127.0.0.1:8000/health](http://127.0.0.1:8000/health) |
| Events | `http://127.0.0.1:8000/tenants/{tenant_id}/events` (+ optional query params) |

**Examples**

- [http://127.0.0.1:8000/health](http://127.0.0.1:8000/health)
- [http://127.0.0.1:8000/tenants/acme_corp/events](http://127.0.0.1:8000/tenants/acme_corp/events)
- [http://127.0.0.1:8000/tenants/acme_corp/events?action=download&package=requests&limit=10](http://127.0.0.1:8000/tenants/acme_corp/events?action=download&package=requests&limit=10)

---

## CLI commands

Default `<pipeline>` is `events` → `config/events.config` and `data/*/events/`.

| Shorthand | Full form |
|-----------|-----------|
| `audit-split-input` | `python -m audit_service.backend.processing.ingestion.split_input` |
| `audit-reset` | `python -m audit_service.backend.processing.reset` |
| `audit-ingest` | `python -m audit_service.backend.processing.ingestion.ingest` |
| `audit-retention` | `python -m audit_service.backend.processing.retention.retention` |

Requires `pip install -e .` for shorthand; otherwise use `python -m` with `PYTHONPATH=src`.

### `audit-split-input`

```bash
python -m audit_service.backend.processing.ingestion.split_input <pipeline> <source.jsonl> [--lines-per-file N] [--output-dir PATH] [--overwrite | --append]
# audit-split-input <pipeline> <source.jsonl> [--lines-per-file N] [--output-dir PATH] [--overwrite | --append]
```

| Flag | Default | Description |
|------|---------|-------------|
| `--lines-per-file` | `100` | Max JSON lines per batch file |
| `--output-dir` | from config | Override landing directory |
| `--overwrite` | off | Overwrite: delete existing `batch_*.jsonl` and their tracking markers, split a fresh timestamped group from `_0001` |
| `--append` | off | Add new batches after the highest number; does not delete landing files or markers |

Default (no flag): **fail** if `batch_*.jsonl` already exist.

```bash
python -m audit_service.backend.processing.ingestion.split_input events events.jsonl
python -m audit_service.backend.processing.ingestion.split_input events events.jsonl --lines-per-file 50
python -m audit_service.backend.processing.ingestion.split_input events events.jsonl --overwrite
python -m audit_service.backend.processing.ingestion.split_input events events.jsonl --append
```

**Output:** JSON summary — `mode`, `lines_read`, `lines_written`, skips, `batch_files`, `existing_batches_before`, `batches_removed`, `tracking_markers_removed`.

### `audit-reset`

Wipe stored pipeline data so you can re-ingest from scratch. **Does not run ingest.**

**Deletes**

| Path | What |
|------|------|
| `data/data_lake/{pipeline}/` | Parquet events (API query source) |
| `data/data_lake_tmp/{pipeline}/` | Staging for partition rewrites |
| `data/dlq/{pipeline}/` | Rejected JSONL lines (`rejects.parquet`) |
| `data/tracking/{pipeline}/` | `.processing` / `.processed` markers |

**Kept by default:** `data/landing/{pipeline}/` JSONL files — add `--landing` to also delete `batch_*.jsonl`.

**Not touched:** `data/logs/`, source `events.jsonl` in project root.

Without `-y`, the CLI prints a formatted list of paths and asks `Proceed with reset? [y/N]`.

```bash
python -m audit_service.backend.processing.reset <pipeline> [-y] [--landing] [--log-mode off|console|file|both] [--log-level LEVEL]
# audit-reset <pipeline> [-y] [--landing] [--log-mode ...] [--log-level ...]
```

| Flag | Default | Description |
|------|---------|-------------|
| `-y`, `--yes` | off | Skip confirmation prompt |
| `--landing` | off | Also delete `batch_*.jsonl` in landing (use before a fresh split) |

```bash
python -m audit_service.backend.processing.reset events -y
python -m audit_service.backend.processing.reset events -y --landing
```

**Output:** Human-readable counts, then JSON (`rows_deleted`, `dlq_deleted`, `tracking_markers_removed`, `landing_batches_removed`).

### `audit-ingest`

```bash
python -m audit_service.backend.processing.ingestion.ingest <pipeline> [--watch [--interval SECS]] [--log-mode off|console|file|both] [--log-level DEBUG|INFO|WARNING|ERROR]
# audit-ingest <pipeline> [--watch [--interval SECS]] [--log-mode ...] [--log-level ...]
```

| Flag | Default | Description |
|------|---------|-------------|
| `--watch` | off | Keep running: ingest one batch, sleep, repeat (Ctrl+C to stop) |
| `--interval SECS` | from config | **Requires `--watch`.** Sleep after each batch cycle; overrides `watch_interval_seconds` in pipeline config |
| `--log-mode` | `both` | Where step logs go |
| `--log-level` | `INFO` | Log verbosity |

```bash
python -m audit_service.backend.processing.ingestion.ingest events
python -m audit_service.backend.processing.ingestion.ingest events --watch
python -m audit_service.backend.processing.ingestion.ingest events --watch --interval 15
python -m audit_service.backend.processing.ingestion.ingest events --log-mode file --log-level DEBUG
```

**Watch mode:** processes up to `ingest_batch_size` files, then **sleeps** (`watch_interval_seconds`, default 30s), then runs the next batch. Repeats until Ctrl+C — including when caught up (polls for newly dropped landing files).

**What it does**

1. Scans `data/landing/events/*.jsonl` for files without a `.processed` marker in `data/tracking/events/`.
2. Processes at most `ingest_batch_size` files per run (from `config/events.config`).
3. Per file: parse → validate → DLQ bad rows → dedupe → upsert into `data/data_lake/events/`.
4. Writes a `.processed` marker so the file is not ingested again.
5. Reports **landing inventory counts** before/after (`total`, `processed`, `pending`, `processing`) and `ingest_complete` when all batches are done. Landing JSONL files **stay in place** — ingest marks them processed; it does not delete them (use `audit-reset --landing` to remove).

**Output:** JSON summary on stdout (`IngestionBatchSummary` — counts + this batch's `results`; not the full landing file list).

```json
{
  "files_attempted": 5,
  "files_succeeded": 5,
  "files_failed": 0,
  "ingest_complete": false,
  "processed_files": [
    "batch_20260630T222851Z_0001.jsonl",
    "batch_20260630T222851Z_0002.jsonl"
  ],
  "failed_files": [],
  "inventory_before": {
    "total": 29,
    "processed": 0,
    "pending": 29,
    "processing": 0
  },
  "inventory_after": {
    "total": 29,
    "processed": 5,
    "pending": 24,
    "processing": 0
  },
  "results": []
}
```

`total` stays **29** until you reset or delete landing files — only `processed` / `pending` change. Per-file landing names are in the log at **DEBUG** only.

Re-run ingest until `ingest_complete` is `true`. Step-by-step detail is in the log file when `--log-mode` is `file` or `both`.

### `audit-retention`

```bash
python -m audit_service.backend.processing.retention.retention <pipeline>
# audit-retention <pipeline>
```

Uses `retention_days` from `config/events.config` (default 90). Deletes `data/data_lake/events/tenant_id=*/event_date=*` folders older than the cutoff.

```bash
python -m audit_service.backend.processing.retention.retention events
# audit-retention events
```

**What it does**

Deletes `data/data_lake/events/tenant_id=*/event_date=*` folders where `event_date` is older than `today(UTC) - retention_days`.

**Output:**

```json
{
  "rows_deleted": 1200,
  "partitions_deleted": 15
}
```

### Scheduling (cron example)

```cron
*/5 * * * *  cd /path/to/samples && PYTHONPATH=src python -m audit_service.backend.processing.ingestion.ingest events
0 2 * * *    cd /path/to/samples && PYTHONPATH=src python -m audit_service.backend.processing.retention.retention events
# With pip install -e .: audit-ingest events / audit-retention events
```

---

## Query API

The API is **read-only**. Ingest and retention run via CLI (or cron), not on API startup.

```bash
python -m uvicorn audit_service.main:app --reload --host 127.0.0.1 --port 8000
# uvicorn audit_service.main:app --reload --host 127.0.0.1 --port 8000
```

**Swagger UI:** [http://127.0.0.1:8000/docs](http://127.0.0.1:8000/docs)  
**ReDoc:** [http://127.0.0.1:8000/redoc](http://127.0.0.1:8000/redoc)  
**OpenAPI JSON:** [http://127.0.0.1:8000/openapi.json](http://127.0.0.1:8000/openapi.json)

### Endpoints

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/docs` | Swagger UI (interactive) |
| `GET` | `/redoc` | ReDoc API reference |
| `GET` | `/health` | Liveness check |
| `GET` | `/tenants/{tenant_id}/events` | List events for one tenant |

### Query parameters (`/tenants/{tenant_id}/events`)

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `start_time` | ISO-8601 datetime | *(none)* | Inclusive lower bound on `timestamp` |
| `end_time` | ISO-8601 datetime | *(none)* | Inclusive upper bound on `timestamp` |
| `action` | `download` \| `upload` \| `delete` | *(none)* | Filter by action |
| `package` | string | *(none)* | Filter by package name |
| `limit` | int (1–1000) | **`100`** | Page size |
| `offset` | int (≥ 0) | **`0`** | Pagination offset |

*(none)* = omit the parameter — no filter on that dimension. Swagger shows these defaults on each field when you open **Try it out**.

### API examples

Swagger (recommended): [http://127.0.0.1:8000/docs](http://127.0.0.1:8000/docs) — open **GET /tenants/{tenant_id}/events**, **Try it out** (fields pre-filled with sample tenant/filters), **Execute**.

Direct browser links:

- All events for a tenant: [http://127.0.0.1:8000/tenants/acme_corp/events](http://127.0.0.1:8000/tenants/acme_corp/events)
- Time range + action + package: [http://127.0.0.1:8000/tenants/globex_inc/events?start_time=2025-03-15T00:00:00%2B00:00&end_time=2025-03-15T23:59:59%2B00:00&action=download&package=globex-api](http://127.0.0.1:8000/tenants/globex_inc/events?start_time=2025-03-15T00:00:00%2B00:00&end_time=2025-03-15T23:59:59%2B00:00&action=download&package=globex-api)
- Pagination: [http://127.0.0.1:8000/tenants/initech/events?limit=50&offset=0](http://127.0.0.1:8000/tenants/initech/events?limit=50&offset=0)

### Event JSON shape (ingest input & API response)

```json
{
  "event_id": "evt_1",
  "tenant_id": "acme_corp",
  "action": "download",
  "package": "requests",
  "version": "2.32.0",
  "timestamp": "2025-03-15T10:00:00+00:00",
  "actor": "ci-runner-03"
}
```

`version` is optional. `action` must be `download`, `upload`, or `delete`. `tenant_id` must match `^[a-zA-Z0-9_]+$`.

---

## Data layout

```
data/
  landing/events/        ← drop .jsonl files here for audit-ingest
  tracking/events/       ← .processing / .processed markers (not event data)
  data_lake/events/      ← hive-partitioned Parquet (source of truth)
    tenant_id=acme_corp/
      event_date=2025-03-15/
        part_<uuid>.parquet
  dlq/events/            ← rejected lines (malformed / validation errors)
    rejects.parquet      ← columns: source_file, line_number, raw_line, reason, rejected_at
  logs/events/           ← ingest step logs (audit-ingest --log-mode file|both)
config/
  app.config             ← paths (shared)
  events.config          ← pipeline knobs (ingest batch size, retention)
```

Override config directory: `set AUDIT_CONFIG_DIR=C:\path\to\config` (Windows) or `export AUDIT_CONFIG_DIR=/path/to/config`.

---

## Configuration

**`config/app.config`** — filesystem paths

```toml
data_lake_dir = "./data/data_lake"
dlq_dir = "./data/dlq"
landing_dir = "./data/landing"
tracking_dir = "./data/tracking"
logs_dir = "./data/logs"
```

**`config/events.config`** — pipeline operational settings

```toml
ingest_batch_size = 5        # max JSONL files per audit-ingest run
retention_days = 90          # partition age cutoff for audit-retention
watch_interval_seconds = 30  # sleep after each batch when audit-ingest --watch is used
```

Table schema, column names, and validation rules live in code (`EventRecord`, `EventsTable`) — not in TOML. See [DESIGN.md](DESIGN.md).

---

## Tests

```bash
python -m pytest -q
```

| Area | Test file |
|------|-----------|
| Ingestion, dedupe, DLQ, tracking markers | `tests/test_ingestion.py` |
| Tenant isolation | `tests/test_tenant_isolation.py` |
| API filters, pagination, validation | `tests/test_api.py` |
| Retention | `tests/test_retention.py` |
| Parquet partition writes | `tests/test_db.py` |
| Reset, watch mode | `tests/test_reset.py`, `tests/test_ingest_watch.py` |
| Split input | `tests/test_split_input.py` |

---

## Code quality

Dev dependencies include **Black** (format), **isort** (imports), **Flake8** (lint), and **pre-commit** hooks.

```bash
pip install -e ".[dev]"
pre-commit install          # install git hooks (runs on commit)
pre-commit run --all-files  # format + lint entire repo now
```

Run tools manually:

```bash
python -m black src tests
python -m isort src tests
python -m flake8 src tests
python -m pytest -q
```

Config: `pyproject.toml` (`[tool.black]`, `[tool.isort]`), `.flake8`, `.pre-commit-config.yaml`.

---

## Approach, trade-offs & shortcuts

**Approach (summary)** — Parquet data lake as source of truth; DuckDB in-memory for SQL merge/query only; write path via CLI (`audit-ingest`, `audit-retention`), read path via FastAPI. Details in [DESIGN.md](DESIGN.md).

**Trade-offs & shortcuts** (time-box friendly):

| Area | Choice | Why / cost |
|------|--------|------------|
| Storage | Parquet files, not a database service | Zero infra; partition rewrite on ingest instead of row-level OLTP |
| Ingest trigger | Batch CLI + tracking markers, not streaming | Simple idempotency; data is eventually consistent until next ingest run |
| Large source file | Optional `audit-split-input` → landing batches | Demonstrates file-level batching with `ingest_batch_size`; single-file drop also works |
| Auth | None on API | Exercise scope; production would bind JWT/API-key `tenant_id` to URL |

**With more time** — compaction for small Parquet files, DLQ replay CLI, API authentication, metrics/alerting on ingest summaries. Full list in [DESIGN.md § Future improvements](DESIGN.md#future-improvements).

---

## Project layout (high level)

```
src/audit_service/
  api/                   # FastAPI query service
  backend/
    config/              # app + pipeline config loaders
    db/                  # in-memory DuckDB SQL engine
    models/              # Pydantic models + EventsTable (Parquet I/O)
    processing/
      ingestion/         # audit-ingest, audit-split-input
      reset/             # audit-reset
      retention/         # audit-retention
    storage/             # Parquet path helpers
docs/
  images/                # architecture diagrams (SVG)
config/                  # TOML runtime config
tests/
```

See [ARCHITECTURE.md](ARCHITECTURE.md) for component diagrams and data flow.

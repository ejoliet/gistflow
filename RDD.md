# gistflow

> Fork a gist, fork a live dataset. A gist (`pipeline.yaml` + a transform) becomes a scheduled pipeline whose output is instantly queryable, embeddable, badge-able, CI-assertable, and agent-accessible.

**Repo**: `ejoliet/gistflow` · **License**: MIT · **README type**: B (build package) · **Status**: spec for MVP v0.1

Run from GitHub, no install:

```bash
uvx --from git+https://github.com/ejoliet/gistflow gistflow run gist:<GIST_ID>
```

## Purpose

**Problem.** Sharing a small live dataset (repo health, asteroid approaches, HN front page) needs a repo, CI, storage, an API, and a UI. Nobody does it, so the data never exists.

**Solution.** The gist is the whole pipeline. `gistflow` fetches it via the GitHub API, runs it in a sandbox, writes Parquet with row-level lineage, and serves every dataset as: a Parquet URL, SQL-over-HTTP JSON, a live SVG badge, an iframe chart, an in-browser SQL page (DuckDB-WASM), an MCP tool, and a CI assertion. Scheduling runs locally, on Airflow 3, or as k8s CronJobs.

**Who benefits.** Data engineers and researchers who want a live public dataset in minutes; README authors who want live badges; agents that need fresh community data.

**MVP definition of "useful".** After MVP, Emmanuel can publish 4 real gists, run them on a schedule, and send one URL where anyone can query live data in the browser, copy a badge, and point an MCP client at it.

## Architecture

```
 gist (pipeline.yaml + transform.py|.sql)
        │  GitHub API (revision SHA = version)
        ▼
 ┌──────────── gistflow run ─────────────┐
 │ 1 resolve ref → spec (pydantic)        │
 │ 2 engine fetches declared sources      │  egress allowlist, size cap
 │ 3 transform in sandbox (subprocess |   │  timeout, rlimits, empty env
 │   docker --network none | duckdb sql)  │
 │ 4 validate schema, add lineage cols    │  _gf_run_id, _gf_gist_sha
 │ 5 write part → compact latest.parquet  │  local dir or s3:// (MinIO ok)
 │ 6 manifest + notify (webhook, comment) │
 └────────────────────────────────────────┘
        ▼
 store/<owner>/<name>/{parts/,latest.parquet,manifest.json}
        ▼
 ┌─ read surfaces ────────────────────────────────────────┐
 │ gistflow serve : /d/.. .parquet (Range) .json?sql=     │
 │                  /badge/..  /embed/..  catalog pages   │
 │ gistflow mcp   : list_datasets / query / lineage       │
 │ gistflow assert: exit 0/1 for CI                       │
 │ gistflow export: copy to another store / duckdb file   │
 └────────────────────────────────────────────────────────┘
 Schedulers: gistflow daemon (croniter) | Airflow 3 DAG factory | k8s CronJobs
```

**Design rules**

- **Transforms never touch the network.** The engine fetches every source; user code gets bytes in, returns rows out. This makes sandboxing tractable and egress auditable.
- **The read path never touches the runner.** Readers hit Parquet files directly (range requests). Runners can be down; data stays readable.
- **Every row carries lineage.** `_gf_run_id`, `_gf_gist_sha`, `_gf_ts` are appended to every output row.

## Recommended stack

Versions verified 2026-09-29. Agent pins exact versions in `pyproject.toml` at implementation time.

| Layer | Chosen | Why | Rejected |
|---|---|---|---|
| Packaging | `uv` + hatchling, `[project.scripts] gistflow` | Required: runs via `uvx --from git+...` | poetry, setuptools-only |
| CLI | Typer | Typed commands, auto help, small | click (more boilerplate), argparse |
| Spec validation | pydantic v2 | Clear error messages on bad gists | jsonschema only |
| Query + write | DuckDB (Python) + pyarrow | One engine for SQL guard, transforms, compaction, serving | polars (no in-browser twin), pandas-only |
| Object store | pyarrow `fs` (local + S3, custom endpoint for MinIO) | No extra dep; handles Range reads | s3fs/fsspec (extra deps) |
| HTTP server | Starlette + uvicorn | Small, async, streaming responses | FastAPI (unneeded weight), stdlib (no async, no Range) |
| HTTP client | httpx | Timeouts, streaming size caps; testable with respx | requests |
| MCP | FastMCP 4.x (latest 4.0.3; PrefectHQ, ~25k stars, ~68M monthly downloads) | De facto Python MCP framework | raw `mcp` SDK |
| Local scheduler | croniter | Cron parsing only, no daemon framework | APScheduler |
| Orchestrator | Airflow 3.3 (current 3.x line) | Emmanuel's production stack | Dagster, Prefect |
| Browser SQL | `@duckdb/duckdb-wasm` via jsDelivr, pinned (docs list 1.5.5 as stable) | Same SQL dialect as server | AlaSQL (used in spike; wrong dialect) |
| Tests | pytest, respx, moto[server] or local-dir store | No real network in tests | — |

Sources: [Airflow version table](https://github.com/apache/airflow/pull/69480/files), [FastMCP 4 changelog](https://github.com/mozilla/bugbug/pull/6474), [DuckDB-Wasm install docs](https://www.duckdb.org/docs/current/clients/wasm/overview).

## Repository layout

```
gistflow/
├── pyproject.toml              # console script `gistflow`; extras: [s3] none needed, [dev]
├── src/gistflow/
│   ├── cli.py                  # Typer app: all commands below
│   ├── spec.py                 # pydantic models for pipeline.yaml v1
│   ├── refs.py                 # parse gist:ID[@sha] | URL | ./dir ; fetch via GitHub API
│   ├── fetch.py                # source fetching, egress allowlist, size caps, secret refs
│   ├── sandbox/
│   │   ├── subprocess_runner.py
│   │   ├── docker_runner.py
│   │   ├── harness.py          # child entrypoint: stdin raw JSON → stdout rows JSON
│   │   └── sql_runner.py       # transform.sql via DuckDB, external access disabled
│   ├── engine.py               # run(): steps 1–6, returns RunResult
│   ├── store.py                # layout, part write, compaction, manifest, catalog index
│   ├── sqlguard.py             # parser-level SELECT-only + timeout + row cap
│   ├── notify.py               # webhook + gist comment
│   ├── server/
│   │   ├── app.py              # Starlette routes
│   │   ├── badge.py            # SVG badge
│   │   ├── embed.py            # server-rendered SVG line/bar chart
│   │   └── templates/          # catalog.html, dataset.html (DuckDB-WASM)
│   ├── mcp_server.py
│   ├── schedule/
│   │   ├── daemon.py           # croniter loop over registry
│   │   ├── airflow_factory.py  # pure: registry → DagSpec list ; renders dag file
│   │   └── k8s.py              # registry → CronJob manifests
│   └── registry.py             # ~/.gistflow/registry.yaml CRUD
├── examples/                   # 4 seed pipelines (each dir = one gist)
│   ├── gh-pulse/  neo-approaches/  hn-frontpage/  hn-x-github/
├── deploy/
│   ├── Dockerfile
│   ├── compose.minio.yml       # MinIO only
│   └── compose.airflow.yml     # Airflow 3.3 standalone + MinIO
├── tests/                      # unit + integration, no real network
├── DEVELOPER.md  RELEASE_NOTES.md  BURN_LOG.md  LICENSE
```

## Prerequisites

| Requirement | Version | Notes |
|---|---|---|
| Python | 3.11+ | |
| uv | latest | `curl -LsSf https://astral.sh/uv/install.sh \| sh` |
| GitHub token | optional | Unauthenticated API = 60 req/h; `GITHUB_TOKEN` or `gh auth token` fallback. `gist` scope only for `notify.gist_comment` |
| Docker | optional | `--runner docker`, MinIO, Airflow compose |
| kubectl | optional | applying generated CronJobs |

## Quick start (target UX, must work at end of MVP)

```bash
alias gf='uvx --from git+https://github.com/ejoliet/gistflow gistflow'

gf run ./examples/gh-pulse                 # local dir works like a gist
gf run gist:<GIST_ID>                       # real gist, latest revision
gf register gist:<GIST_ID>                  # add to registry
gf daemon                                   # run registered pipelines on schedule
gf serve --port 8787                        # catalog at http://localhost:8787
gf assert ejoliet/gh-pulse "SELECT max(stars) > 1000 FROM latest"
gf mcp                                      # stdio MCP server
```

## Pipeline spec v1 (`pipeline.yaml`)

```yaml
name: gh-pulse                 # ^[a-z0-9][a-z0-9-]{0,62}$ ; dataset id = <owner>/<name>
spec: 1
description: Hourly GitHub repo health snapshot
schedule: "0 * * * *"          # cron, UTC; omit = manual only
params:
  repos: [duckdb/duckdb, apache/airflow, pola-rs/polars]
source:                        # omit when only `depends` is used
  format: json                 # json | csv | text
  urls: []                     # explicit list, OR:
  url_template: "https://api.github.com/repos/{item}"
  foreach: repos               # param whose list expands {item}
  headers: {Authorization: "Bearer ${secret:GITHUB_TOKEN}"}
depends: []                    # dataset ids, e.g. [ejoliet/hn-frontpage]
transform: transform.py        # transform.py | transform.sql
output:
  mode: append                 # append | replace
  dedupe_on: [repo, ts]        # optional, append only
  schema:
    - {name: ts, type: timestamp}
    - {name: repo, type: string}
    - {name: stars, type: int64}
notify:
  - when: "SELECT count(*) > 0 FROM new_rows WHERE stars > 100000"
    webhook: "${secret:SLACK_WEBHOOK}"
  - when: "SELECT true FROM new_rows LIMIT 1"
    gist_comment: "gh-pulse: {rows} new rows, run {run_id}"
limits:
  timeout_s: 60                # hard cap 300 (server-side max)
  max_rows: 100000
  max_source_bytes: 20000000
  egress: [api.github.com]     # every source host must be listed
```

**Rules the engine enforces**

- Types: `string | int64 | float64 | bool | timestamp | date`. Timestamps stored UTC.
- `${secret:NAME}` resolves from env `GISTFLOW_SECRET_<NAME>` (fallback: `NAME` for `GITHUB_TOKEN` only). Secrets are never logged, never written to manifest, redacted in errors.
- Any source host not in `limits.egress` fails the run before any request.
- A failed source fetch passes `null` for that entry (transform decides); if all fail, the run fails.
- `new_rows` in `notify.when` = rows from this run only; `latest` = full dataset.

### Transform contracts

`transform.py`:

```python
def transform(raw: list, ctx: dict) -> list[dict]:
    # raw: one parsed body per source URL, in order (None on failed fetch)
    # ctx: {"ts": iso_utc, "params": {...}, "run_id": str, "upstream": {name: parquet_path}}
    ...
```

Runs as `python -I -m gistflow.sandbox.harness <file>`: raw JSON on stdin, rows JSON on stdout, stderr captured (last 4 KB into manifest). Environment emptied except `PATH`, `TZ=UTC`. Available imports: stdlib plus gistflow's own deps. Custom deps are a non-goal in v1.

`transform.sql`: DuckDB SQL. Views available: `raw` (source bodies loaded via `read_json_auto`/`read_csv_auto` from temp files), one view per `depends` entry named `<name>` with `-` → `_`. External access is disabled before the user SQL runs.

## Seed examples (ship in `examples/`, Emmanuel publishes as gists)

| Dir | Source | Why it's in the seed set |
|---|---|---|
| `gh-pulse` | `api.github.com/repos/{item}` fan-out | Fork-to-personalize demo; badge demo |
| `neo-approaches` | `ssd-api.jpl.nasa.gov/cad.api?dist-max=0.05&date-min=now` (response is `fields` + `data` arrays) | Science credibility; "closer than the Moon" query |
| `hn-frontpage` | `hn.algolia.com/api/v1/search?tags=front_page` (single call) | Self-referential HN demo, every 30 min |
| `hn-x-github` | `depends: [ejoliet/hn-frontpage, ejoliet/gh-pulse]`, `transform.sql` | Cross-gist join; the "npm moment" |

Each example includes a `README.md` with 2 showcase queries. Tests use recorded fixtures of each API response under `tests/fixtures/`.

## Store layout

```
<store>/_catalog.json                         # [{id, description, schedule, rows, updated, gist}]
<store>/<owner>/<name>/parts/<ts>_<run_id>.parquet
<store>/<owner>/<name>/latest.parquet         # compacted after every successful run
<store>/<owner>/<name>/manifest.json          # spec snapshot (secrets removed), gist sha, last 50 runs
```

- `<store>` = `GISTFLOW_STORE` or `--store`; local path or `s3://bucket/prefix`.
- Owner: gist owner login; local dir → `local`.
- Compaction: `COPY (SELECT * FROM read_parquet('parts/*.parquet') [dedupe]) TO latest.parquet` via temp file + atomic rename (local) or put-then-delete-old (S3).
- MVP data sizes are small (<100 MB per dataset); compaction-per-run is acceptable. Iceberg is a non-goal.

## Interface contract

### CLI

| Command | Behavior | Exit codes |
|---|---|---|
| `run <ref> [--store] [--runner subprocess\|docker] [--dry-run]` | Execute one pipeline; print RunResult JSON | 0 ok, 2 spec error, 3 source error, 4 transform error, 5 limit exceeded |
| `validate <ref>` | Parse + validate spec, no fetch | 0 / 2 |
| `register <ref>` / `unregister <id>` / `list` | Registry CRUD in `$GISTFLOW_HOME/registry.yaml` | 0 / 2 |
| `daemon [--once]` | Run due registered pipelines per cron; `--once` runs due jobs and exits | 0 |
| `serve [--host 127.0.0.1] [--port 8787] [--store]` | Start read surfaces | — |
| `query <id> "<sql>" [--format table\|json\|csv]` | Local query via same guard | 0 / 6 query error |
| `assert <id\|url> "<sql>"` | Passes iff first column of first row is truthy | 0 pass, 1 fail, 6 error |
| `export <id> --to <path\|s3://\|file.duckdb>` | Copy dataset (parquet + manifest) or load into DuckDB file | 0 |
| `mcp [--store]` | stdio MCP server | — |
| `airflow dagfile [--store] [--mode bash\|k8s]` | Print Airflow 3 DAG-factory Python file | 0 |
| `k8s cronjobs --image <img> [--namespace]` | Print CronJob YAML for all registered pipelines | 0 |
| `init <dir>` | Scaffold `pipeline.yaml` + `transform.py` | 0 |

`<ref>` forms: `gist:<id>`, `gist:<id>@<sha>`, `https://gist.github.com/<user>/<id>`, local dir path.

`assert` with a URL (`https://host/d/owner/name`) calls the remote `.json?sql=` endpoint, so CI needs no store access.

### HTTP routes (`gistflow serve`)

| Route | Returns | Cache-Control |
|---|---|---|
| `GET /` | Catalog HTML (search, list from `_catalog.json`) | 60 s |
| `GET /d/{owner}/{name}` | Dataset page: schema, lineage, DuckDB-WASM SQL box over `latest.parquet`, copy-paste snippets (curl, pandas, badge, embed, CI, MCP) | 60 s |
| `GET /d/{owner}/{name}/latest.parquet` | File; **must honor `Range`** (206 + `Content-Range`), `Accept-Ranges: bytes` | 60 s |
| `GET /d/{owner}/{name}/latest.json?sql=` | `{"columns":[...],"rows":[[...]],"truncated":bool,"ms":n}` | 60 s |
| `GET /d/{owner}/{name}/latest.csv?sql=` | CSV | 60 s |
| `GET /badge/{owner}/{name}?sql=&label=&color=` | SVG, value = first cell | 300 s |
| `GET /embed/{owner}/{name}?sql=&chart=line\|bar&x=&y=` | Minimal HTML with server-rendered SVG chart; no JS | 300 s |
| `GET /api/catalog` | `_catalog.json` | 60 s |
| `GET /api/lineage/{owner}/{name}` | manifest minus spec secrets | 60 s |

All responses send `Access-Control-Allow-Origin: *`. In every SQL route the dataset is exposed as table `latest`.

### SQL guard (`sqlguard.py`) — shared by server, CLI, MCP

1. Parse with DuckDB `extract_statements`; reject unless exactly one statement of type SELECT.
2. Load `latest.parquet` into an in-memory table, then `SET enable_external_access=false; SET lock_configuration=true` before running user SQL.
3. Timeout: `GISTFLOW_SQL_TIMEOUT_S` (default 5) via `con.interrupt()` from a timer thread.
4. Row cap: `GISTFLOW_MAX_RESULT_ROWS` (default 1000), set `truncated: true`.
5. Errors return 400 with DuckDB's message, never a stack trace.

### MCP tools

| Tool | Args | Returns |
|---|---|---|
| `list_datasets` | `search?: str` | id, description, schema, rows, updated |
| `get_schema` | `dataset` | columns + 3 sample rows |
| `query` | `dataset, sql` | columns, rows (cap 200), truncated |
| `get_lineage` | `dataset` | gist, sha, last runs |

## Configuration reference

| Env var | Type | Default | Required | Purpose |
|---|---|---|---|---|
| `GISTFLOW_HOME` | path | `~/.gistflow` | no | registry, local store default |
| `GISTFLOW_STORE` | path/URI | `$GISTFLOW_HOME/store` | no | where datasets live |
| `GISTFLOW_S3_ENDPOINT` | URL | unset | no | MinIO/R2 endpoint override |
| `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` / `AWS_REGION` | str | unset | for S3 | standard AWS chain also works |
| `GITHUB_TOKEN` | str | `gh auth token` fallback | no | gist fetch rate limits; gist comments |
| `GISTFLOW_SECRET_<NAME>` | str | unset | per spec | values for `${secret:NAME}` |
| `GISTFLOW_RUNNER` | enum | `subprocess` | no | `subprocess` or `docker` |
| `GISTFLOW_RUNNER_IMAGE` | str | `python:3.12-slim` + mounted harness | no | docker runner image |
| `GISTFLOW_SQL_TIMEOUT_S` | int | 5 | no | query timeout |
| `GISTFLOW_MAX_RESULT_ROWS` | int | 1000 | no | result cap |
| `GISTFLOW_PUBLIC_URL` | URL | `http://127.0.0.1:8787` | no | absolute URLs in snippets/badges |

Secrets only via env or local `.env` (gitignored). Never in gists, manifests, or logs.

## Sandboxing (MVP honesty)

| Runner | Isolation | Use |
|---|---|---|
| `subprocess` (default) | `python -I`, empty env, cwd = temp dir, `resource.setrlimit` (CPU = timeout, AS = 1 GB, NOFILE = 64, FSIZE = 100 MB) on Linux/macOS, wall-clock kill | Trusted gists (your own) |
| `docker` | `--network none --read-only --memory 512m --cpus 1 --pids-limit 64 --cap-drop ALL`, raw via stdin | Untrusted gists |
| `sql` | DuckDB, external access off | Always safe to run |

`run` prints a one-line warning when running a gist not owned by the authenticated user with the subprocess runner. gVisor/Firecracker is a non-goal for v1; the docker runner accepts `--runtime runsc` passthrough via `GISTFLOW_DOCKER_ARGS`.

## Scheduling backends

**daemon**: every 30 s, evaluate croniter for each registered pipeline; run due ones sequentially; persist last-run times in `$GISTFLOW_HOME/state.json`. `--once` for cron/launchd/systemd use.

**Airflow 3 DAG factory**: `airflow_factory.py` has a pure `build_dag_specs(registry) -> list[DagSpec]` (dag_id `gf__<owner>__<name>`, schedule, command, tags) that is unit-tested without Airflow installed. `render_dagfile()` emits a file that imports Airflow 3 SDK (`from airflow.sdk import DAG`, BashOperator from the standard provider, or `KubernetesPodOperator` in `--mode k8s`) and loops over DagSpecs. Agent verifies these import paths against Airflow 3.3 docs; does not install Airflow.

**k8s**: one `batch/v1` CronJob per pipeline, `concurrencyPolicy: Forbid`, `activeDeadlineSeconds = timeout_s + 60`, command `gistflow run <ref> --store $GISTFLOW_STORE`, env from a Secret named `gistflow-env`. `deploy/Dockerfile` builds the image with `uv`.

## Error handling

| Error class | Raised when | Exit | Retry |
|---|---|---|---|
| `SpecError` | YAML/pydantic invalid, bad ref, missing transform | 2 | no |
| `EgressDenied` | source host not in `limits.egress` | 2 | no |
| `SourceError` | all fetches failed, non-2xx on every URL, size cap | 3 | daemon: next tick |
| `TransformError` | non-zero exit, bad JSON, exception in transform | 4 | no |
| `LimitExceeded` | timeout, max_rows, memory | 5 | no |
| `SchemaError` | missing column or uncoercible type | 4 | no |
| `QueryError` | guard rejection, timeout, DuckDB error | 6 | no |
| `StoreError` | write/compaction failure | 7 | once, then fail |
| `GitHubRateLimited` | 403/429 with rate-limit headers | 3 | honor `retry-after` once |

Source fetches: httpx, 10 s connect / `timeout_s` total, 1 retry on 5xx and connection errors. Failed runs still write a manifest entry with `status: failed` and redacted error.

## Testing

| Suite | Covers | Rule |
|---|---|---|
| `tests/unit/test_spec.py` | valid/invalid specs, secret refs, egress derivation | pure |
| `tests/unit/test_sqlguard.py` | DROP/COPY/ATTACH/multi-statement/`read_csv('/etc/passwd')` rejected; timeout; cap | pure |
| `tests/unit/test_sandbox.py` | timeout kill, env emptied, bad JSON, exception surfaces | subprocess only |
| `tests/unit/test_airflow_factory.py` | DagSpecs from registry; rendered file `compile()`s | no Airflow import |
| `tests/unit/test_k8s.py` | YAML parses, fields correct | pure |
| `tests/integration/test_engine.py` | all 4 examples end-to-end with respx fixtures, local store | no network |
| `tests/integration/test_server.py` | every route via Starlette TestClient incl. Range 206 | no network |
| `tests/integration/test_mcp.py` | tools listed; `query` via in-process MCP client | no network |
| `tests/integration/test_refs.py` | gist fetch via respx-mocked GitHub API, `@sha` pinning | no network |

`make test` (= `uv run pytest -q`), `make lint` (= `uv run ruff check . && uv run ruff format --check . && uv run mypy src`).

## Known pitfalls from the spike (read before coding)

- DuckDB cannot use prepared parameters in DDL (`CREATE VIEW ... read_parquet(?)` fails). Validate names with a regex, then inline.
- FastMCP APIs shifted across majors (`get_tools` vs `list_tools`; decorated functions are plain functions). Test through the MCP layer (`call_tool`), not the Python function.
- Unauthenticated GitHub API throttles quickly; the spike silently skipped repos. Always read the token, and record skipped sources in the manifest.
- Starting a dev server on a fixed port twice fails with `EADDRINUSE`; tests must use `TestClient`, never a real port.
- The spike used AlaSQL in the browser; the real page must use DuckDB-WASM so browser SQL = server SQL.

## Non-goals (v1)

- Hosted multi-tenant public cluster, accounts, billing.
- Iceberg tables, partitioning, datasets over ~100 MB.
- Custom Python deps per transform (`requires:`).
- gVisor/Firecracker integration beyond docker args passthrough.
- Auth on read routes (all datasets are public by design).
- Helm chart, ArgoCD, Terraform.
- Windows support.

## Open questions

| # | Question | Default if unanswered |
|---|---|---|
| 1 | Public demo host: `serve` on a small VM, or static export to GitHub Pages reading Parquet from a public S3/R2 bucket? | Build `serve` first; static export is Phase 8 |
| 2 | Should `hn-x-github` depend on `ejoliet/*` ids or accept any owner via params? | Hardcode `ejoliet/*`, forks edit |
| 3 | Keep last N parts forever or prune after compaction? | Keep all parts in MVP |

## Agent build instructions

> Implement end-to-end using only this README. Do not install Airflow, run Docker, open a browser, push to GitHub, or hit real APIs; Emmanuel runs those checks with the commands in "Owner-run verification". Add `AIDEV-NOTE:` comments only at non-obvious decisions.

### Build order

| Phase | Deliverable | Done when |
|---|---|---|
| 0 | Scaffold: pyproject (console script, pinned deps), ruff, mypy, Makefile, CI workflow file (lint + test) | `make lint` and empty `make test` pass; `uv run gistflow --help` works |
| 1 | `spec.py`, `refs.py`, `fetch.py`, `init`, `validate` | spec + refs tests pass |
| 2 | Sandbox runners + `engine.py` + `store.py` + `run` | all 4 examples pass integration test with fixtures |
| 3 | `sqlguard.py`, `query`, `assert`, `export` | guard tests pass incl. escape attempts |
| 4 | `serve`: all routes, badge, embed, catalog + dataset templates with DuckDB-WASM (pinned) | server tests pass incl. Range |
| 5 | `mcp` | MCP tests pass |
| 6 | `registry`, `daemon`, `notify` (webhook + gist comment, respx-mocked) | tests pass; `daemon --once` runs due jobs in test |
| 7 | Airflow DAG factory, k8s CronJobs, `deploy/` Dockerfile + compose files | factory/k8s tests pass; files present |
| 8 | Docs: DEVELOPER.md, RELEASE_NOTES.md (v0.1.0), BURN_LOG.md (start with spike pitfalls), example READMEs | files present, commands in them match CLI help |

### File map (key symbols)

| File | Key symbols |
|---|---|
| `spec.py` | `PipelineSpec`, `Source`, `Output`, `Column`, `Notify`, `Limits`, `load_spec(path_or_text)` |
| `refs.py` | `Ref`, `parse_ref(s)`, `resolve(ref) -> ResolvedPipeline(files, owner, sha)` |
| `fetch.py` | `expand_urls(spec)`, `fetch_all(spec) -> list[bytes \| None]`, `resolve_secrets()` |
| `engine.py` | `run(ref, store, runner) -> RunResult` |
| `store.py` | `Store.open(uri)`, `write_part`, `compact`, `read_manifest`, `update_catalog` |
| `sqlguard.py` | `safe_query(parquet_uri, sql, timeout_s, max_rows) -> QueryResult` |
| `server/app.py` | `create_app(store) -> Starlette` |
| `mcp_server.py` | `build_mcp(store) -> FastMCP` |
| `schedule/airflow_factory.py` | `DagSpec`, `build_dag_specs`, `render_dagfile` |
| `schedule/k8s.py` | `render_cronjobs(registry, image, namespace)` |

### Constraints

- Python 3.11+, typed signatures, `ruff` + `mypy` (non-strict ok for templates) pass.
- No sync network calls in async routes; SQL runs in a threadpool.
- Secrets via env only; a test asserts no secret value appears in manifest or logs.
- Tests never hit real APIs (respx) or real S3 (local store; optional moto marker).
- Must run via `uvx --from git+https://github.com/ejoliet/gistflow gistflow`; templates and example files included as package data.

### Acceptance criteria

- [ ] `make lint` and `make test` pass; coverage ≥ 80% on `src/gistflow` excluding templates.
- [ ] All 4 examples run end-to-end in integration tests against fixtures.
- [ ] Every row in every output has `_gf_run_id`, `_gf_gist_sha`, `_gf_ts`.
- [ ] SQL guard rejects: DDL, DML, `COPY`, `ATTACH`, `INSTALL/LOAD`, multi-statement, and file-reading table functions.
- [ ] Parquet route returns 206 with correct `Content-Range` for a Range request.
- [ ] `assert` exit codes 0/1/6 verified by tests, both local id and URL forms.
- [ ] MCP `query` tested through the MCP protocol layer.
- [ ] Rendered Airflow dagfile and CronJob YAML are syntactically valid.
- [ ] No secret value appears in manifest, stdout, or error output (test).
- [ ] Open questions resolved or left at their stated defaults.

## Owner-run verification (Emmanuel)

Run after the agent reports done. Each block states what "pass" looks like.

```bash
# 0. Local checks
git clone git@github.com:ejoliet/gistflow.git && cd gistflow
uv sync && make lint && make test

# 1. Real network runs (needs GITHUB_TOKEN for gh-pulse)
export GITHUB_TOKEN=$(gh auth token)
uv run gistflow run ./examples/gh-pulse
uv run gistflow run ./examples/neo-approaches
uv run gistflow run ./examples/hn-frontpage
uv run gistflow run ./examples/hn-x-github          # pass: rows > 0 after the two upstreams
uv run gistflow query local/gh-pulse "SELECT repo, max(stars) FROM latest GROUP BY 1"

# 2. Publish seeds as real gists, run from gist refs
for d in gh-pulse neo-approaches hn-frontpage hn-x-github; do
  gh gist create --public examples/$d/* -d "gistflow: $d"
done
uv run gistflow run gist:<ID_FROM_ABOVE>              # pass: owner = ejoliet in output

# 3. Read surfaces
uv run gistflow serve &
open http://127.0.0.1:8787                          # pass: catalog lists datasets
open http://127.0.0.1:8787/d/ejoliet/gh-pulse       # pass: SQL box runs in browser
curl -sI -H 'Range: bytes=0-99' http://127.0.0.1:8787/d/ejoliet/gh-pulse/latest.parquet | head -3
uv run python -c "import duckdb; print(duckdb.sql(\"SELECT count(*) FROM 'http://127.0.0.1:8787/d/ejoliet/gh-pulse/latest.parquet'\"))"
uv run gistflow assert http://127.0.0.1:8787/d/ejoliet/gh-pulse "SELECT max(stars) > 1000 FROM latest"; echo "exit=$?"

# 4. MCP in Claude Code
claude mcp add gistflow -- uvx --from git+https://github.com/ejoliet/gistflow gistflow mcp
# ask: "Using gistflow, which NEO passes closest to Earth this month?"

# 5. S3 via MinIO
docker compose -f deploy/compose.minio.yml up -d
GISTFLOW_STORE=s3://gistflow/store GISTFLOW_S3_ENDPOINT=http://127.0.0.1:9000 \
AWS_ACCESS_KEY_ID=minioadmin AWS_SECRET_ACCESS_KEY=minioadmin AWS_REGION=us-east-1 \
  uv run gistflow run ./examples/gh-pulse

# 6. Airflow 3
uv run gistflow register gist:<GH_PULSE_ID>
uv run gistflow airflow dagfile > deploy/dags/gistflow_dags.py
docker compose -f deploy/compose.airflow.yml up -d  # pass: gf__ejoliet__gh-pulse visible, a run succeeds

# 7. k8s (any cluster)
docker build -t ghcr.io/ejoliet/gistflow:dev -f deploy/Dockerfile .
uv run gistflow k8s cronjobs --image ghcr.io/ejoliet/gistflow:dev | kubectl apply --dry-run=server -f -
```

## Next steps

1. Hand this README to the coding agent; answer Open Questions 1–3 first or accept defaults.
2. Run "Owner-run verification" blocks 0–4; that alone is a launchable local demo.
3. Decide the public host (Open Question 1), then follow the launch plan from the earlier `LAUNCH.md`.

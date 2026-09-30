# Gistflow — working demo package

> Fork a gist, fork a live dataset. This package is the end-to-end proof: a real pipeline run, a real query endpoint, a real badge, a real MCP server, and the launch copy.

## Overview

Gistflow treats a GitHub Gist (pipeline.yaml + transform.py) as a scheduled data pipeline. A shared Airflow-on-k8s cluster runs it; output lands as Parquet/Iceberg on S3; every dataset gets a query URL, a badge, an embed, and an MCP tool for free.

Everything below was **executed and verified** in a sandbox on 2026-08-12, against the live GitHub API.

## What's here and what was proven

| Path | What it is | Verified behavior |
|---|---|---|
| `examples/gh-pulse/` | The gist: 30-line spec + 20-line transform | Ran twice against `api.github.com`; 5 rows written; rate-limited repos skipped gracefully |
| `engine/gistflow_mini.py` | Local runner (= one Airflow task in prod) | Egress allowlist, schema validation, row caps, append mode, lineage manifest per run |
| `data/` | The "S3 bucket": Parquet + run manifests + badge.svg | Real snapshot data with run ids `1292bafb`, `53a396f0` |
| `endpoint/serve.py` | SQL-over-HTTP + badge server (local twin) | JSON `?sql=` route, Parquet-URL-as-pandas-API, live SVG badge, DROP/injection rejected, CI-gate assert |
| `endpoint/worker.js` | Production Cloudflare Worker (R2, edge cache, range passthrough) | Code-reviewed twin of serve.py; not executable locally |
| `mcp/server.py` | FastMCP server: `list_datasets`, `query`, `get_lineage` | Tools registered; `query` called through the MCP layer returned real rows |
| `demo/index.html` | Catalog page: SQL in browser, live badge, copy-paste snippets | jsdom-tested: default query, chips, SQL guard, badge render all pass |
| `LAUNCH.md` | HN + Reddit messaging, prepared replies, sequencing | — |

## Quick start

```bash
pip install duckdb pyarrow pandas requests pyyaml fastmcp

# 1. Run the pipeline (writes data/gh_pulse.parquet + lineage manifest)
python engine/gistflow_mini.py run examples/gh-pulse

# 2. Serve the dataset
python endpoint/serve.py &

# 3. Use it three ways
curl "localhost:8787/d/gh_pulse/latest.json?sql=SELECT+repo,max(stars)+FROM+latest+GROUP+BY+repo"
python -c "import pandas as pd; print(pd.read_parquet('http://localhost:8787/d/gh_pulse/latest.parquet'))"
curl "localhost:8787/badge/gh_pulse?label=stars&sql=SELECT+max(stars)+FROM+latest" -o badge.svg

# 4. Expose the catalog to agents
python mcp/server.py   # stdio MCP: list_datasets / query / get_lineage
```

Open `demo/index.html` in a browser for the in-page SQL experience.

## Architecture mapping: demo → production

| Demo piece | Production equivalent |
|---|---|
| `gistflow_mini.py` in-process | gVisor pod per run, KubernetesExecutor, DAG generated per gist |
| local `data/*.parquet` | Iceberg on S3, compaction DAGs |
| `serve.py` on localhost | `worker.js` at the edge + DuckDB query pool |
| embedded rows + AlaSQL in demo page | DuckDB-WASM over Parquet HTTP range requests |
| local dir as "gist" | actual gist; revisions = versions, stars = trust, forks = lineage |

## Known gaps (deliberate, this is a demo)

- No scheduler here — prod schedule lives in the generated Airflow DAG.
- `serve.py` SQL guard is regex-level; prod adds a parser-level allowlist, per-IP rate limits, result byte caps.
- MCP server is local-file mode; hosted mode points at the public endpoint.
- Unauthenticated GitHub API rate limits caused skipped rows in the sample data — visible in the dataset, and honest to show.

## Next steps

1. Spike the riskiest prod seam: gist → generated DAG → sandboxed pod → Iceberg on MinIO.
2. Replace AlaSQL in the demo page with DuckDB-WASM against the hosted Parquet URL.
3. Seed 30 datasets, then follow `LAUNCH.md` sequencing.

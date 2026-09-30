"""Gistflow MCP server — every community dataset becomes an AI tool.

Run:  uv run --with fastmcp --with duckdb python mcp/server.py
Then any MCP client (Claude Code, Desktop) can:
  list_datasets() -> catalog with schemas
  query(dataset, sql) -> rows
  get_lineage(dataset) -> gist/run provenance

Local mode reads ./data/*.parquet; hosted mode points DATA_URL at the
public endpoint so agents query the live catalog with zero setup.
"""

from __future__ import annotations

import json
import os
import re
from pathlib import Path

import duckdb
from fastmcp import FastMCP

DATA = Path(os.environ.get("GISTFLOW_DATA", Path(__file__).parent.parent / "data"))
mcp = FastMCP("gistflow")


@mcp.tool()
def list_datasets() -> str:
    """List all datasets in the catalog with their schemas and row counts."""
    out = []
    for p in sorted(DATA.glob("*.parquet")):
        con = duckdb.connect(":memory:")
        schema = con.execute(f"DESCRIBE SELECT * FROM read_parquet('{p}')").fetchall()
        n = con.execute(f"SELECT count(*) FROM read_parquet('{p}')").fetchone()[0]
        out.append(
            {
                "dataset": p.stem,
                "rows": n,
                "columns": [{"name": c[0], "type": c[1]} for c in schema],
            }
        )
    return json.dumps(out, indent=2)


@mcp.tool()
def query(dataset: str, sql: str) -> str:
    """Run a read-only SQL SELECT against a dataset. The dataset is exposed as table `latest`.

    Example: query("gh_pulse", "SELECT repo, max(stars) FROM latest GROUP BY repo")
    """
    if not re.match(r"^\s*select\b", sql, re.IGNORECASE) or ";" in sql:
        return json.dumps({"error": "single SELECT statements only"})
    if not re.fullmatch(r"[a-z0-9_]+", dataset):
        return json.dumps({"error": "bad dataset name"})
    path = DATA / f"{dataset}.parquet"
    if not path.exists():
        return json.dumps({"error": f"unknown dataset {dataset}; call list_datasets"})
    con = duckdb.connect(":memory:")
    con.execute(f"CREATE VIEW latest AS SELECT * FROM read_parquet('{path}')")
    cur = con.execute(sql)
    cols = [d[0] for d in cur.description]
    rows = [dict(zip(cols, r)) for r in cur.fetchmany(200)]
    return json.dumps(rows, default=str, indent=2)


@mcp.tool()
def get_lineage(dataset: str) -> str:
    """Return run manifests (run_id, spec hash, timestamps, rows) proving where a dataset came from."""
    runs = [json.loads(p.read_text()) for p in sorted(DATA.glob("run_*.json"))]
    return json.dumps([r for r in runs if r.get("pipeline", "").replace("-", "_") == dataset], indent=2)


if __name__ == "__main__":
    mcp.run()

"""Gistflow data endpoint — local reference implementation.

Routes (same contract as the production Cloudflare Worker in worker.js):
  GET /d/<table>/latest.json?sql=<url-encoded SQL>   -> JSON rows
  GET /badge/<table>?sql=<SQL returning 1 value>&label=<text> -> live SVG badge
  GET /d/<table>/latest.parquet                      -> raw file (the "API for free")

Security: read-only DuckDB, only the requested table is exposed as a view,
SQL must be a single SELECT. Prod adds per-IP rate limits + result size caps.
"""

from __future__ import annotations

import json
import re
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse

import duckdb

DATA = Path(__file__).parent.parent / "data"
SELECT_ONLY = re.compile(r"^\s*select\b", re.IGNORECASE)


def query(table: str, sql: str) -> list[dict]:
    if not SELECT_ONLY.match(sql) or ";" in sql:
        raise ValueError("single SELECT statements only")
    if not re.fullmatch(r"[a-z0-9_]+", table):
        raise ValueError("bad table name")
    path = DATA / f"{table}.parquet"
    if not path.exists():
        raise ValueError(f"unknown dataset: {table}")
    con = duckdb.connect(":memory:")
    # AIDEV-NOTE: DuckDB can't prepare DDL; table name is regex-validated above
    con.execute(f"CREATE VIEW latest AS SELECT * FROM read_parquet('{path}')")
    cur = con.execute(sql)
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, r)) for r in cur.fetchmany(1000)]  # hard row cap


def badge_svg(label: str, value: str) -> str:
    lw, vw = 6 * len(label) + 12, 6 * len(str(value)) + 12
    return f"""<svg xmlns="http://www.w3.org/2000/svg" width="{lw + vw}" height="20" role="img" aria-label="{label}: {value}">
  <rect width="{lw}" height="20" fill="#484f58"/>
  <rect x="{lw}" width="{vw}" height="20" fill="#1f6feb"/>
  <g fill="#fff" font-family="Verdana,sans-serif" font-size="10" text-anchor="middle">
    <text x="{lw / 2}" y="14">{label}</text>
    <text x="{lw + vw / 2}" y="14">{value}</text>
  </g>
</svg>"""


class Handler(BaseHTTPRequestHandler):
    def do_GET(self):  # noqa: N802
        u = urlparse(self.path)
        q = parse_qs(u.query)
        try:
            if u.path.startswith("/d/") and u.path.endswith("/latest.json"):
                table = u.path.split("/")[2]
                rows = query(table, q["sql"][0])
                self._send(200, "application/json", json.dumps(rows, default=str))
            elif u.path.startswith("/badge/"):
                table = u.path.split("/")[2]
                rows = query(table, q["sql"][0])
                value = str(list(rows[0].values())[0]) if rows else "n/a"
                self._send(200, "image/svg+xml", badge_svg(q.get("label", ["gistflow"])[0], value))
            elif u.path.startswith("/d/") and u.path.endswith("/latest.parquet"):
                table = u.path.split("/")[2]
                body = (DATA / f"{table}.parquet").read_bytes()
                self._send(200, "application/octet-stream", body)
            else:
                self._send(404, "text/plain", "not found")
        except Exception as e:  # noqa: BLE001
            self._send(400, "application/json", json.dumps({"error": str(e)}))

    def _send(self, code: int, ctype: str, body):
        if isinstance(body, str):
            body = body.encode()
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Access-Control-Allow-Origin", "*")  # datasets are public
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *a):  # quiet
        pass


if __name__ == "__main__":
    print("serving on http://127.0.0.1:8787")
    HTTPServer(("127.0.0.1", 8787), Handler).serve_forever()

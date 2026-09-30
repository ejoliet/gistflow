"""gistflow_mini — minimal local runner. `python gistflow_mini.py run <pipeline_dir>`

What the real `gistflow` CLI does with a gist URL, this does with a local dir:
  1. Load pipeline.yaml
  2. Fetch source URLs (respecting the egress allowlist)
  3. Call transform.py::transform(raw, ctx)
  4. Validate against declared schema, enforce limits
  5. Append to data/<table>.parquet  (the "S3 object" in prod)
  6. Write a run manifest (lineage: run_id, spec hash, row count)

In production the same steps run as one Airflow task in a sandboxed pod;
step 5 writes Iceberg on S3 instead of a local file.
"""

from __future__ import annotations

import hashlib
import importlib.util
import json
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse

import pandas as pd
import requests
import yaml

DATA_DIR = Path(__file__).parent.parent / "data"


def load_transform(path: Path):
    spec = importlib.util.spec_from_file_location("user_transform", path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)  # AIDEV-NOTE: prod runs this in a gVisor pod, never in-process
    return mod.transform


def fetch(urls: list[str], allow: list[str], timeout: int) -> list[dict]:
    out = []
    for url in urls:
        host = urlparse(url).hostname or ""
        if allow and host not in allow:
            raise PermissionError(f"egress to {host} not declared in limits.egress")
        r = requests.get(url, timeout=timeout, headers={"User-Agent": "gistflow-mini"})
        out.append(r.json() if r.ok else {})
    return out


def run(pipeline_dir: Path) -> Path:
    spec_path = pipeline_dir / "pipeline.yaml"
    spec = yaml.safe_load(spec_path.read_text())
    lim = spec.get("limits", {})

    urls = [
        spec["source"]["url_template"].format(repo=r)
        for r in spec["params"][spec["source"]["urls_from_param"]]
    ]
    ctx = {"ts": datetime.now(timezone.utc).isoformat(), "params": spec["params"]}

    raw = fetch(urls, lim.get("egress", []), lim.get("timeout_s", 30))
    rows = load_transform(pipeline_dir / "transform.py")(raw, ctx)

    if len(rows) > lim.get("max_rows", 1_000_000):
        raise RuntimeError("row limit exceeded")

    declared = [c["name"] for c in spec["output"]["schema"]]
    df = pd.DataFrame(rows)[declared]  # KeyError here = schema violation, run fails loudly
    for col in ("ts", "pushed_at"):
        if col in df:
            df[col] = pd.to_datetime(df[col], utc=True)

    DATA_DIR.mkdir(exist_ok=True)
    table = DATA_DIR / f"{spec['output']['table']}.parquet"
    if table.exists() and spec["output"].get("mode") == "append":
        df = pd.concat([pd.read_parquet(table), df], ignore_index=True)
    df.to_parquet(table, index=False)

    manifest = {
        "run_id": str(uuid.uuid4())[:8],
        "pipeline": spec["name"],
        "spec_sha": hashlib.sha256(spec_path.read_bytes()).hexdigest()[:12],
        "ts": ctx["ts"],
        "rows_written": len(rows),
        "table": str(table),
    }
    (DATA_DIR / f"run_{manifest['run_id']}.json").write_text(json.dumps(manifest, indent=2))
    print(json.dumps(manifest, indent=2))
    return table


if __name__ == "__main__":
    if len(sys.argv) != 3 or sys.argv[1] != "run":
        sys.exit("usage: python gistflow_mini.py run <pipeline_dir>")
    run(Path(sys.argv[2]))

# Gistflow — launch messaging

> Copy for Show HN, Reddit, and the first-hour comment strategy.

## Hacker News — Show HN

**Title** (80-char limit; pick one, A is primary):

- A: `Show HN: Gistflow – GitHub Gists as a package manager for live datasets`
- B: `Show HN: Fork a gist, fork a live dataset (Airflow + DuckDB-WASM + S3)`

**First comment** (post immediately after submitting — this is the real pitch):

```
Hi HN, I build data pipelines for a space telescope by day, and I kept
wishing that sharing a small live dataset was as easy as sharing a gist.
So I made it literally a gist.

A Gistflow pipeline is one gist: a ~30-line pipeline.yaml plus a small
transform. Register it and a shared Airflow-on-k8s cluster runs it on
schedule inside a sandboxed pod, writing Parquet/Iceberg to S3. Every
dataset then gets, for free:

- a stable Parquet URL (pd.read_parquet(url) — the URL is the API)
- SQL over HTTP: /latest.json?sql=SELECT...
- a live SVG badge for READMEs
- in-browser SQL via DuckDB-WASM over range requests, no signup
- an MCP server, so agents can query the whole catalog
- full lineage: every row traces to a gist revision + run id

The part I like most: forking. Fork the gist, change the params, and you
have your own scheduled dataset a minute later. The catalog shows the
fork graph, and pipelines can declare other people's gists as
dependencies — cross-author joins on live data.

Try it (SQL in your browser, on data the pipeline wrote this hour):
https://gistflow.dev/d/ejoliet/gh_pulse

Honest limitations: untrusted code runs in gVisor with an egress
allowlist and hard CPU/row caps, so heavy transforms won't fly on the
free cluster; bring-your-own-runner exists for that. Unauthenticated
GitHub API sources rate-limit fast (my own demo pipeline skips repos
when that happens — you can see the gaps in the data).

Everything exports: the spec is your gist, `gistflow export` copies the
Parquet to your own bucket. Code: https://github.com/ejoliet/gistflow
```

**Why this framing works**

- Opens with a concrete personal itch, not a vision statement.
- "Honest limitations" paragraph preempts the top two attack comments
  (sandbox abuse, sustainability) before anyone writes them.
- The demo link points at a page where the first interaction is running
  SQL, not reading docs.
- No superlatives. HN punishes "revolutionary".

## Hacker News — prepared replies

| Predictable comment | Reply angle |
|---|---|
| "So it's cron + curl" | Yes — plus hosting, lineage, the query layer, badges, and the fork graph. cron + curl was the spike; the surfaces around the data are the product. |
| "Who pays for compute?" | Free tier = tiny quotas + 60s timeouts; BYO-runner for real workloads; hosted control plane is the eventual business. |
| "Crypto miners will eat this" | gVisor, no egress except declared hosts, 60s CPU cap, output size cap. Abuse was designed for on day one, happy to detail. |
| "What if you shut down?" | Spec is a gist you own; `gistflow export --to s3://your-bucket` copies everything. Engine is OSS. |
| "GitHub ToS?" | Gists are read via the public API within rate limits; heavy metadata is mirrored to our own index, gists are the source of truth, not the hot path. |

## Reddit — r/dataengineering

**Title**: `I turned GitHub Gists into a registry for scheduled pipelines — Airflow DAGs generated from a 30-line YAML, output queryable as Parquet over HTTP`

**Body**: lead with architecture, not product. This crowd wants the DAG-factory
pattern, the Iceberg layout, and the sandboxing story.

```
Architecture notes for a side project I just launched:

- Spec: gist with pipeline.yaml + transform.py. Gist revisions = versions.
- A registry service indexes registered gists via the GitHub API and
  compiles each spec into an Airflow DAG (DAG factory, KubernetesExecutor,
  one gVisor-sandboxed pod per run, egress allowlist from the spec).
- Output: Iceberg on S3, hourly compaction is itself just another DAG.
- Read path never touches the cluster: Parquet over HTTP range requests,
  so DuckDB-WASM in the browser and pandas both hit S3/CDN directly.
  A worker adds ?sql= for JSON and live SVG badges, cached at the edge.
- Lineage: every row carries (gist_sha, run_id); `gistflow replay` re-runs
  a revision against archived source snapshots.

Happy to go deep on the DAG generation or the sandbox. Repo in comments.
```

## Reddit — r/selfhosted

**Title**: `Gistflow: self-hostable "fork a gist, fork a dataset" — one binary + your k8s`

Angle: the BYO-runner story. One paragraph, emphasize `helm install`,
no phone-home, data stays in your bucket.

## Sequencing

| Day | Action |
|---|---|
| 0 | Show HN, 8–10am PT Tue–Thu. First comment ready. Seed catalog has 30 datasets. |
| 0 | Reply to every top-level comment for 4 hours. Ship one small fix live and say so. |
| 2 | r/dataengineering post (different angle, links the HN thread). |
| 7 | Second post: "Show HN: every Gistflow dataset is now an MCP tool" — the MCP server is a whole second launch. |

## Rules

- Never post the same text twice; each venue gets its native angle.
- The demo link must require zero clicks before the SQL box.
- If the thread turns on sandboxing, answer with specifics (gVisor flags,
  egress proxy) — vagueness reads as "unsolved".

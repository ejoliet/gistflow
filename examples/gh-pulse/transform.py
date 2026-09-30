"""gh-pulse transform. The ONLY code a pipeline author writes.

Contract: transform(raw: list[dict], ctx: dict) -> list[dict]
- raw: one parsed JSON body per source URL, in order
- ctx: {"ts": iso-utc-string, "params": {...from pipeline.yaml...}}
- return: rows matching output.schema in pipeline.yaml
"""


def transform(raw: list[dict], ctx: dict) -> list[dict]:
    rows = []
    for body in raw:
        if not body or "full_name" not in body:
            continue  # AIDEV-NOTE: skip rate-limited/missing repos, don't fail the run
        rows.append(
            {
                "ts": ctx["ts"],
                "repo": body["full_name"],
                "stars": body["stargazers_count"],
                "forks": body["forks_count"],
                "open_issues": body["open_issues_count"],
                "subscribers": body["subscribers_count"],
                "pushed_at": body["pushed_at"],
            }
        )
    return rows

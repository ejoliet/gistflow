// Gistflow edge endpoint — Cloudflare Worker (production twin of serve.py).
// Bindings: DATA = R2 bucket with <owner>/<table>/latest.parquet
//           QUERY_POOL = container/DO running DuckDB for /latest.json
// Routes:
//   GET /d/:owner/:table/latest.parquet  -> stream from R2 (range requests pass through,
//                                           so DuckDB-WASM clients read only needed bytes)
//   GET /d/:owner/:table/latest.json?sql=... -> proxy to query pool, cache 60s
//   GET /badge/:owner/:table?sql=...&label=... -> SVG, cache 300s

const SELECT_ONLY = /^\s*select\b/i;

export default {
  async fetch(req, env, ctx) {
    const url = new URL(req.url);
    const [, kind, owner, table, tail] = url.pathname.split("/");

    if (kind === "d" && tail === "latest.parquet") {
      // AIDEV-NOTE: pass Range through — this is what makes "parquet URL = API" cheap
      const obj = await env.DATA.get(`${owner}/${table}/latest.parquet`, {
        range: req.headers.get("Range") ?? undefined,
      });
      if (!obj) return new Response("unknown dataset", { status: 404 });
      return new Response(obj.body, {
        status: req.headers.has("Range") ? 206 : 200,
        headers: cors({ "Content-Type": "application/octet-stream" }),
      });
    }

    if ((kind === "d" && tail === "latest.json") || kind === "badge") {
      const sql = url.searchParams.get("sql") ?? "";
      if (!SELECT_ONLY.test(sql) || sql.includes(";"))
        return json({ error: "single SELECT statements only" }, 400);

      const cacheKey = new Request(req.url); // GET + full query string
      const cached = await caches.default.match(cacheKey);
      if (cached) return cached;

      const rows = await runQuery(env, owner, table, sql); // query pool, row-capped
      let resp;
      if (kind === "badge") {
        const label = url.searchParams.get("label") ?? `${owner}/${table}`;
        const value = rows.length ? String(Object.values(rows[0])[0]) : "n/a";
        resp = new Response(badgeSvg(label, value), {
          headers: cors({ "Content-Type": "image/svg+xml", "Cache-Control": "public, max-age=300" }),
        });
      } else {
        resp = json(rows, 200, { "Cache-Control": "public, max-age=60" });
      }
      ctx.waitUntil(caches.default.put(cacheKey, resp.clone()));
      return resp;
    }
    return new Response("not found", { status: 404 });
  },
};

async function runQuery(env, owner, table, sql) {
  const r = await env.QUERY_POOL.fetch("http://pool/query", {
    method: "POST",
    body: JSON.stringify({ dataset: `${owner}/${table}`, sql, maxRows: 1000 }),
  });
  if (!r.ok) throw new Error(await r.text());
  return r.json();
}

const cors = (h) => ({ "Access-Control-Allow-Origin": "*", ...h });
const json = (obj, status = 200, extra = {}) =>
  new Response(JSON.stringify(obj), { status, headers: cors({ "Content-Type": "application/json", ...extra }) });

function badgeSvg(label, value) {
  const lw = 6 * label.length + 12, vw = 6 * value.length + 12;
  return `<svg xmlns="http://www.w3.org/2000/svg" width="${lw + vw}" height="20" role="img" aria-label="${label}: ${value}">
  <rect width="${lw}" height="20" fill="#484f58"/><rect x="${lw}" width="${vw}" height="20" fill="#1f6feb"/>
  <g fill="#fff" font-family="Verdana,sans-serif" font-size="10" text-anchor="middle">
  <text x="${lw / 2}" y="14">${label}</text><text x="${lw + vw / 2}" y="14">${value}</text></g></svg>`;
}

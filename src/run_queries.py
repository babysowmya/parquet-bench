import argparse, csv, glob, json, os, resource, subprocess, sys, time
from datetime import timedelta
import duckdb, psutil

QUERIES = {
 "point_lookup": "SELECT * FROM {src} WHERE user_id = {user_id}",
 "range_1day": "SELECT COUNT(*) FROM {src} WHERE event_ts BETWEEN TIMESTAMP '{d0}' AND TIMESTAMP '{d1}'",
 "country_filter": "SELECT AVG(value) FROM {src} WHERE country = 'US'",
 "full_agg": "SELECT event_type, COUNT(*) FROM {src} GROUP BY event_type",
 "topk_week": "SELECT user_id, SUM(value) FROM {src} WHERE event_ts BETWEEN TIMESTAMP '{w0}' AND TIMESTAMP '{w1}' GROUP BY user_id ORDER BY SUM(value) DESC LIMIT 10",
}
HEADER = ["layout","row_group_rows","files","total_bytes","query","cache","latency_ms","bytes_read",
          "row_groups_total","row_groups_pruned","peak_rss_mb"]

def connect():
    con = duckdb.connect()
    con.execute("SET threads=2"); con.execute("SET memory_limit='2GB'")
    return con

def src_expr(root): return f"read_parquet('{root}/**/*.parquet', hive_partitioning=true)"

def peak_rss_mb():
    r = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return r / (1024 * 1024) if sys.platform == "darwin" else r / 1024

def find_scan(node):
    name = f"{node.get('operator_type','')}{node.get('operator_name','')}{node.get('name','')}".upper()
    if "SCAN" in name: return node
    for c in node.get("children", []):
        r = find_scan(c)
        if r: return r

def run_once(con, sql, prof):
    con.execute("PRAGMA enable_profiling='json'")
    try: con.execute(f"PRAGMA profiling_output='{prof}'")
    except Exception: con.execute(f"PRAGMA profile_output='{prof}'")
    p = psutil.Process(); b0 = p.io_counters().read_chars
    t0 = time.perf_counter(); con.execute(sql).fetchall()
    lat = (time.perf_counter() - t0) * 1000
    b = p.io_counters().read_chars - b0
    try: scan = find_scan(json.load(open(prof)))
    except Exception: scan = None
    return {"latency_ms": lat, "bytes_read": b, "peak_rss_mb": peak_rss_mb(), "scan": scan}

def worker(payload):
    con = connect(); prof = f"/tmp/prof_{os.getpid()}.json"
    n = 1 if payload["cache"] == "cold" else payload["runs"] + 1
    res = [run_once(con, payload["sql"], prof) for _ in range(n)]
    if payload["cache"] == "warm": res = res[1:]
    print("RESULT" + json.dumps(res))

def call_worker(pr):
    o = subprocess.run([sys.executable, __file__, "--worker", json.dumps(pr)], capture_output=True, text=True)
    if o.returncode != 0:
        print("WORKER FAILED:", o.stderr[-800:])
        return None
    return json.loads(o.stdout.split("RESULT")[-1])

def params(raw):
    con = connect()
    n, mn = con.execute(f"SELECT count(*), min(event_ts) FROM read_parquet('{raw}')").fetchone()
    uid = con.execute(f"SELECT user_id FROM read_parquet('{raw}') ORDER BY user_id LIMIT 1 OFFSET {n//2}").fetchone()[0]
    d0 = mn.replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=10)
    w0 = d0 + timedelta(days=10)
    return {"user_id": uid, "d0": str(d0), "d1": str(d0 + timedelta(days=1)), "w0": str(w0), "w1": str(w0 + timedelta(days=7))}

def compute_prune(root, P):
    con = connect()
    con.execute(f"""CREATE TEMP TABLE m AS SELECT file_name,row_group_id,path_in_schema,
        stats_min_value mn, stats_max_value mx FROM parquet_metadata('{root}/**/*.parquet')""")
    total = con.execute("SELECT count(*) FROM (SELECT DISTINCT file_name,row_group_id FROM m)").fetchone()[0]
    spec = {"point_lookup": ("user_id","BIGINT",str(P["user_id"]),str(P["user_id"]),str(P["user_id"])),
            "range_1day": ("event_ts","TIMESTAMP",f"TIMESTAMP '{P['d0']}'",f"TIMESTAMP '{P['d1']}'",None),
            "country_filter": ("country","VARCHAR","'US'","'US'","US"),
            "topk_week": ("event_ts","TIMESTAMP",f"TIMESTAMP '{P['w0']}'",f"TIMESTAMP '{P['w1']}'",None)}
    out = {"full_agg": (total, 0)}
    for q, (col, typ, lo, hi, hv) in spec.items():
        has = con.execute(f"SELECT count(*) FROM m WHERE path_in_schema='{col}'").fetchone()[0]
        if has:
            pr = con.execute(f"""SELECT count(*) FROM m WHERE path_in_schema='{col}' AND
                NOT (CAST(mn AS {typ}) <= {hi} AND CAST(mx AS {typ}) >= {lo})""").fetchone()[0]
        elif hv:
            pr = con.execute(f"""SELECT count(*) FROM (SELECT DISTINCT file_name,row_group_id FROM m
                WHERE file_name NOT LIKE '%{col}={hv}/%')""").fetchone()[0]
        else: pr = 0
        out[q] = (total, pr)
    return out

def checksum(root):
    return connect().execute(f"SELECT SUM(CAST(ROUND(value*100) AS BIGINT)) FROM {src_expr(root)}").fetchone()[0]

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", default="data/layouts"); ap.add_argument("--out", default="results/layout.csv")
    ap.add_argument("--raw", default="data/raw/events.parquet"); ap.add_argument("--runs", type=int, default=5)
    ap.add_argument("--worker"); a = ap.parse_args()
    if a.worker: return worker(json.loads(a.worker))

    P = params(a.raw)
    ref = connect().execute(f"SELECT SUM(CAST(ROUND(value*100) AS BIGINT)) FROM read_parquet('{a.raw}')").fetchone()[0]
    layouts = sorted(d for d in os.listdir(a.input) if os.path.isdir(os.path.join(a.input, d)))
    for L in layouts:
        root = os.path.join(a.input, L)
        if os.path.exists(os.path.join(root, "_PARTIAL")): print("skip checksum (partial):", L); continue
        if checksum(root) != ref: sys.exit(f"INTEGRITY FAILURE: SUM(value) mismatch in {L}")
    print("checksum OK across", len(layouts), "layouts")

    os.makedirs(os.path.dirname(a.out) or ".", exist_ok=True); os.makedirs("results/profiles", exist_ok=True)
    f = open(a.out, "w", newline=""); w = csv.DictWriter(f, fieldnames=HEADER); w.writeheader()
    for L in layouts:
        root = os.path.join(a.input, L)
        files = glob.glob(f"{root}/**/*.parquet", recursive=True)
        meta = json.load(open(f"{root}/_meta.json")) if os.path.exists(f"{root}/_meta.json") else {}
        prune = compute_prune(root, P)
        n_runs = 2 if len(files) > 20000 else a.runs
        for q, tpl in QUERIES.items():
            sql = tpl.format(src=src_expr(root), **P)
            runs = []
            for _ in range(n_runs):
                res = call_worker({"sql": sql, "cache": "cold", "runs": 1})
                if res: runs.append(("cold", res[0]))
            res = call_worker({"sql": sql, "cache": "warm", "runs": n_runs})
            if res: runs += [("warm", r) for r in res]
            if not runs:
                print("SKIPPED (all runs failed):", L, q); continue
            json.dump(runs[0][1]["scan"], open(f"results/profiles/{L}__{q}.json", "w"), indent=1)
            for cache, r in runs:
                w.writerow({"layout": L, "row_group_rows": meta.get("row_group_rows", ""), "files": len(files),
                            "total_bytes": sum(os.path.getsize(x) for x in files), "query": q, "cache": cache,
                            "latency_ms": round(r["latency_ms"], 3), "bytes_read": r["bytes_read"],
                            "row_groups_total": prune[q][0], "row_groups_pruned": prune[q][1],
                            "peak_rss_mb": round(r["peak_rss_mb"], 1)})
            f.flush(); print("done", L, q)
    f.close()

if __name__ == "__main__": main()
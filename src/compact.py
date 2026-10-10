import argparse, collections, csv, glob, json, os, resource, shutil, sys, time
import duckdb, pyarrow as pa, pyarrow.compute as pc, pyarrow.parquet as pq
from common import scaled_rg

def rss_mb():
    r = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return r / (1024 * 1024) if sys.platform == "darwin" else r / 1024

def sumcents(t):
    return pc.sum(pc.cast(pc.round(pc.multiply(t["value"], 100)), pa.int64())).as_py() or 0

def q(root):
    return duckdb.sql(f"SELECT count(*), SUM(CAST(ROUND(value*100) AS BIGINT)) FROM read_parquet('{root}/**/*.parquet')").fetchone()

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", default="data/layouts/tiny_files")
    ap.add_argument("--out", default="data/layouts/compacted")
    ap.add_argument("--stats", default="results/compaction.csv")
    a = ap.parse_args()
    files = sorted(glob.glob(os.path.join(a.input, "*.parquet")))
    by_day = collections.defaultdict(list)
    for p in files:
        by_day[os.path.basename(p)[:8]].append(p)
    total_rows = sum(pq.ParquetFile(p).metadata.num_rows for p in files)
    rg = scaled_rg(total_rows, max(1, len(by_day)))
    shutil.rmtree(a.out, ignore_errors=True); os.makedirs(a.out)
    t0 = time.time(); rows_in = 0; sum_in = 0
    for day, ps in sorted(by_day.items()):
        t = pa.concat_tables([pq.read_table(p) for p in ps])
        rows_in += t.num_rows; sum_in += sumcents(t)
        pq.write_table(t.sort_by("user_id"), os.path.join(a.out, f"{day}.parquet"), row_group_size=rg)
    wall, peak = time.time() - t0, rss_mb()
    json.dump({"row_group_rows": rg}, open(os.path.join(a.out, "_meta.json"), "w"))
    rows_out, sum_out = q(a.out)
    rows_chk, sum_chk = q(a.input)
    ok = rows_in == rows_out == rows_chk and sum_in == sum_out == sum_chk
    os.makedirs(os.path.dirname(a.stats) or ".", exist_ok=True)
    with open(a.stats, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["input_files", "output_files", "wall_s", "peak_rss_mb", "rows_before", "rows_after", "checksum_match"])
        w.writerow([len(files), len(by_day), round(wall, 2), round(peak, 1), rows_in, rows_out, str(ok).lower()])
    sys.exit(0 if ok else "CHECKSUM MISMATCH")

if __name__ == "__main__":
    main()
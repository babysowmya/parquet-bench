import argparse, csv, json, os, shutil, time
from datetime import datetime, timezone
import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
from common import scaled_rg

US = 1_000_000
UNIT = {"minute": 60 * US, "hour": 3600 * US, "day": 86400 * US}
FMT = {"minute": "%Y%m%d_%H%M", "hour": "%Y%m%d_%H", "day": "%Y%m%d"}
DEFAULT_RG = 122_880

def buckets(table, unit):
    keys = table["event_ts"].cast(pa.int64()).to_numpy() // UNIT[unit]
    order = np.argsort(keys, kind="stable")
    table, keys = table.take(pa.array(order)), keys[order]
    cuts = np.flatnonzero(np.diff(keys)) + 1
    starts, ends = np.concatenate([[0], cuts]), np.concatenate([cuts, [len(keys)]])
    for s, e in zip(starts, ends):
        yield int(keys[s]), table.slice(int(s), int(e - s))

def stamp(key, unit):
    return datetime.fromtimestamp(key * UNIT[unit] / US, tz=timezone.utc)

def write_meta(root, rg):
    json.dump({"row_group_rows": rg}, open(os.path.join(root, "_meta.json"), "w"))

def flat(table, root, unit, rg, sort_col=None):
    os.makedirs(root, exist_ok=True)
    for key, part in buckets(table, unit):
        if sort_col:
            part = part.sort_by(sort_col)
        pq.write_table(part, os.path.join(root, stamp(key, unit).strftime(FMT[unit]) + ".parquet"), row_group_size=rg)
    write_meta(root, rg)

def hive_date(table, root, with_country):
    os.makedirs(root, exist_ok=True)
    for key, part in buckets(table, "day"):
        d = os.path.join(root, "date=" + stamp(key, "day").strftime("%Y-%m-%d"))
        if not with_country:
            os.makedirs(d, exist_ok=True)
            pq.write_table(part, os.path.join(d, "part-0.parquet"), row_group_size=DEFAULT_RG)
        else:
            for c in pc.unique(part["country"]).to_pylist():
                cd = os.path.join(d, f"country={c}")
                os.makedirs(cd, exist_ok=True)
                sub = part.filter(pc.equal(part["country"], c)).drop_columns(["country"])
                pq.write_table(sub, os.path.join(cd, "part-0.parquet"), row_group_size=DEFAULT_RG)
    write_meta(root, DEFAULT_RG)

def user_partitions(table, root, cap, stats_csv):
    """Induced failure: partition by high-cardinality user_id."""
    os.makedirs(root, exist_ok=True)
    t0, files, err, partial = time.time(), 0, "", False
    t = table.sort_by("user_id")
    uid = t["user_id"].to_numpy()
    cuts = np.flatnonzero(np.diff(uid)) + 1
    starts, ends = np.concatenate([[0], cuts]), np.concatenate([cuts, [len(uid)]])
    try:
        for s, e in zip(starts, ends):
            if files >= cap:
                partial = True
                break
            d = os.path.join(root, f"user_id={uid[s]}")
            os.makedirs(d, exist_ok=True)
            pq.write_table(t.slice(int(s), int(e - s)).drop_columns(["user_id"]), os.path.join(d, "part-0.parquet"))
            files += 1
    except OSError as ex:
        err, partial = str(ex), True
    if partial:
        open(os.path.join(root, "_PARTIAL"), "w").write("incomplete")
    write_meta(root, DEFAULT_RG)
    total = sum(os.path.getsize(os.path.join(dp, f)) for dp, _, fs in os.walk(root) for f in fs if f.endswith(".parquet"))
    os.makedirs(os.path.dirname(stats_csv), exist_ok=True)
    with open(stats_csv, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["distinct_users", "files_written", "total_bytes", "partial", "error", "build_s"])
        w.writerow([len(starts), files, total, partial, err, round(time.time() - t0, 2)])

def build_all_layouts(input_file: str, output_dir: str):
    table = pq.read_table(input_file)
    rg_small = scaled_rg(table.num_rows)
    shutil.rmtree(output_dir, ignore_errors=True)
    L = lambda n: os.path.join(output_dir, n)
    flat(table, L("tiny_files"), "minute", DEFAULT_RG)
    flat(table, L("hourly"), "hour", DEFAULT_RG)
    flat(table, L("daily"), "day", DEFAULT_RG)
    flat(table, L("daily_rg_10k"), "day", 10_000)
    flat(table, L("daily_rg_1m"), "day", 1_000_000)
    flat(table, L("daily_rg_small"), "day", rg_small)      # unsorted control
    hive_date(table, L("partitioned_by_date"), False)
    hive_date(table, L("partitioned_by_date_country"), True)
    flat(table, L("sorted_by_user"), "day", rg_small, "user_id")
    flat(table, L("sorted_by_ts"), "day", rg_small, "event_ts")
    user_partitions(table, L("partitioned_by_user_id"), int(os.getenv("MAX_USER_PARTITIONS", 60000)),
                    "results/failure_user_partition.csv")

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", default="data/raw/events.parquet")
    ap.add_argument("--out_dir", default="data/layouts")
    a = ap.parse_args()
    build_all_layouts(a.input, a.out_dir)
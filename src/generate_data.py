import argparse, os
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

COUNTRIES = ("US IN GB DE FR CA AU BR JP CN MX ES IT NL SE NO DK FI PL PT IE CH AT BE CZ GR TR RU UA ZA "
             "NG EG KE SA AE IL PK BD ID MY SG TH VN PH KR NZ AR CL CO PE").split()
EVENTS = ["click", "view", "purchase", "scroll", "login", "logout", "search", "add_to_cart"]
DEVICES = ["ios", "android", "web"]
START = np.datetime64("2026-01-01T00:00:00", "us")
SCHEMA = pa.schema([("event_ts", pa.timestamp("us")), ("user_id", pa.int64()),
                    ("country", pa.string()), ("event_type", pa.string()),
                    ("device", pa.string()), ("value", pa.float64())])

def generate_events(num_rows: int, out_path: str, seed: int = 42, n_users: int = 50000, batch: int = 1_000_000):
    rng = np.random.default_rng(seed)
    os.makedirs(os.path.dirname(out_path) or ".", exist_ok=True)
    span_us = 90 * 24 * 3600 * 1_000_000
    done = 0
    with pq.ParquetWriter(out_path, SCHEMA, compression="snappy") as w:
        while done < num_rows:
            n = min(batch, num_rows - done)
            ts = START + rng.integers(0, span_us, n).astype("timedelta64[us]")
            tbl = pa.table({
                "event_ts": pa.array(ts, type=pa.timestamp("us")),
                "user_id": pa.array(rng.integers(1, n_users + 1, n), type=pa.int64()),
                "country": pa.array(rng.choice(np.array(COUNTRIES), n)),
                "event_type": pa.array(rng.choice(np.array(EVENTS), n)),
                "device": pa.array(rng.choice(np.array(DEVICES), n)),
                "value": pa.array(np.round(rng.uniform(0, 500, n), 2)),
            }, schema=SCHEMA)
            w.write_table(tbl)
            done += n

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--rows", type=int, default=int(os.getenv("NUM_ROWS_DEFAULT", 200_000_000)))
    ap.add_argument("--out_dir", default="data/raw")
    ap.add_argument("--seed", type=int, default=int(os.getenv("SEED", 42)))
    a = ap.parse_args()
    generate_events(a.rows, os.path.join(a.out_dir, "events.parquet"), a.seed, int(os.getenv("NUM_USERS", 50000)))
    print("wrote", a.rows, "rows")
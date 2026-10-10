import csv, os, subprocess, sys, duckdb, pytest

def sh(*a): subprocess.run([sys.executable, *a], check=True)

@pytest.fixture(scope="session")
def run(tmp_path_factory):
    d = str(tmp_path_factory.mktemp("w"))
    raw, lay, res = f"{d}/raw", f"{d}/layouts", f"{d}/layout.csv"
    sh("src/generate_data.py", "--rows", "10000", "--out_dir", raw)
    sh("src/build_layouts.py", "--input", f"{raw}/events.parquet", "--out_dir", lay)
    sh("src/compact.py", "--input", f"{lay}/tiny_files", "--out", f"{lay}/compacted", "--stats", f"{d}/compaction.csv")
    sh("src/run_queries.py", "--input", lay, "--out", res, "--raw", f"{raw}/events.parquet", "--runs", "2")
    return d

def test_raw(run):
    assert duckdb.sql(f"SELECT count(*) FROM '{run}/raw/events.parquet'").fetchone()[0] == 10000

def test_layouts(run):
    for n in "tiny_files hourly daily daily_rg_10k daily_rg_1m partitioned_by_date partitioned_by_date_country sorted_by_user sorted_by_ts partitioned_by_user_id".split():
        assert os.path.isdir(f"{run}/layouts/{n}")

def test_csv(run):
    rows = list(csv.DictReader(open(f"{run}/layout.csv")))
    assert len({r["query"] for r in rows}) == 5 and {r["cache"] for r in rows} == {"cold", "warm"}
    pr = lambda l: max(int(r["row_groups_pruned"]) for r in rows if r["layout"] == l and r["query"] == "point_lookup")
    assert pr("sorted_by_user") > 0 and pr("daily") == 0

def test_compaction(run):
    r = list(csv.DictReader(open(f"{run}/compaction.csv")))[0]
    assert r["rows_before"] == r["rows_after"] and r["checksum_match"] == "true"
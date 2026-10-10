import pandas as pd
d = pd.read_csv("results/layout.csv")
g = d.groupby(["layout", "query", "cache"]).agg(lat=("latency_ms", "median"), pruned=("row_groups_pruned", "max"),
        total=("row_groups_total", "max"), bytes=("bytes_read", "median")).reset_index()
g["pct_pruned"] = (100 * g.pruned / g.total).round(1)
print("\n== range_1day median latency (ms) =="); print(g[g["query"] == "range_1day"].pivot(index="layout", columns="cache", values="lat"))
print("\n== point_lookup pruning =="); print(g[(g["query"] == "point_lookup") & (g.cache == "warm")][["layout", "pruned", "total", "pct_pruned", "lat"]])
rg = g[g.layout.isin(["daily_rg_10k", "daily", "daily_rg_1m"]) & (g.cache == "warm")]
print("\n== fastest row-group size per query =="); print(rg.loc[rg.groupby("query").lat.idxmin()][["query", "layout", "lat"]])
t, dly = g[(g.layout == "tiny_files") & (g["query"] == "range_1day")].set_index("cache").lat, g[(g.layout == "daily") & (g["query"] == "range_1day")].set_index("cache").lat
print("\n== latency ratio tiny_files / daily (range_1day) =="); print((t / dly).round(1))
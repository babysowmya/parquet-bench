import os

def scaled_rg(total_rows, days=90):
    cap = int(os.getenv("SORTED_ROW_GROUP_ROWS", "1000"))
    return max(10, min(cap, total_rows // days // 8))
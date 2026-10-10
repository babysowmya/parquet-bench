NUM_ROWS ?= 1000000

layouts:
	python src/generate_data.py --rows $(NUM_ROWS) --out_dir data/raw
	python src/build_layouts.py --input data/raw/events.parquet --out_dir data/layouts

queries:
	python src/compact.py --input data/layouts/tiny_files --out data/layouts/compacted --stats results/compaction.csv
	python src/run_queries.py --input data/layouts --out results/layout.csv

test:
	pytest -q tests

.PHONY: layouts queries test
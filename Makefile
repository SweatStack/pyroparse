unexport CONDA_PREFIX

.PHONY: test build bench

test:
	uv run python -m pytest tests/ -v $(pytestargs)

build:
	uv run maturin develop --release

benchmark:
	uv run python scripts/benchmark.py

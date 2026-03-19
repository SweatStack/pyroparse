.PHONY: test build

test:
	uv run python -m pytest tests/ -v $(pytestargs)

build:
	uv run maturin develop --release

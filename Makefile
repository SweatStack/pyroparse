unexport CONDA_PREFIX

.PHONY: test build benchmark server build_server

test:
	uv run python -m pytest tests/ -v $(pytestargs)

build:
	uv run maturin develop --release

benchmark:
	uv run python scripts/benchmark.py

server:
	uv run --extra server uvicorn server.app:app --reload

build_server:
	docker build -t pyroparse . && docker run -p 8000:8000 pyroparse

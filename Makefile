unexport CONDA_PREFIX

.PHONY: test build benchmark benchmark_http server build_server publish wheels generate-profile

test:
	uv run python -m pytest tests/ -v $(pytestargs)

build:
	uv run maturin develop --release

benchmark:
	uv run python scripts/benchmark.py

benchmark_http:
	@echo "Starting server..."
	@uv run --extra server uvicorn server.app:app --host 127.0.0.1 --port 8000 & \
	SERVER_PID=$$!; \
	trap "kill $$SERVER_PID 2>/dev/null" EXIT; \
	sleep 2; \
	uv run --extra scripts --extra server python scripts/benchmark_http.py $(if $(remote),--remote $(remote)); \
	kill $$SERVER_PID 2>/dev/null

server:
	uv run --extra server uvicorn server.app:app --reload

build_server:
	docker build -t pyroparse . && docker run -p 8000:8000 pyroparse

publish:
	@echo "Publishing is handled by CI on tag push. See .github/workflows/ci.yml"
	@echo "To build wheels locally for testing: make wheels"
	@exit 1

wheels:
	./build.sh

generate-profile:
	uv run --with openpyxl --with tomli python scripts/generate_profile.py

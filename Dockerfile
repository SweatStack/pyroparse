# -- Builder -------------------------------------------------------------------

FROM python:3.12-alpine AS builder

RUN apk add --no-cache curl gcc musl-dev

RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --profile minimal
ENV PATH="/root/.cargo/bin:${PATH}"

WORKDIR /build
COPY Cargo.toml pyproject.toml README.md ./
COPY src/ src/

RUN pip install --no-cache-dir maturin[patchelf] && \
    maturin build --release --strip -o /wheels

# -- Runtime -------------------------------------------------------------------

FROM python:3.12-alpine

WORKDIR /app
COPY --from=builder /wheels /wheels
COPY server/ .

RUN pip install --no-cache-dir /wheels/*.whl starlette uvicorn python-multipart && \
    rm -rf /wheels

EXPOSE 8000
CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8000"]

"""Pyroparse HTTP server — convert FIT files to Parquet or CSV."""

from __future__ import annotations

import io
import tempfile
from pathlib import Path

import pyarrow.csv as pcsv
import pyarrow.parquet as pq
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import HTMLResponse, Response
from starlette.routing import Route

from pyroparse import Activity

_HTML = (Path(__file__).parent / "index.html").read_text()


async def index(_request: Request) -> HTMLResponse:
    return HTMLResponse(_HTML)


async def convert(request: Request) -> Response:
    form = await request.form()
    upload = form["file"]
    fmt = form.get("format", "parquet")
    columns_raw = form.get("columns", "standard")

    if columns_raw in ("standard", ""):
        columns = None
    elif columns_raw == "all":
        columns = "all"
    else:
        columns = [c.strip() for c in columns_raw.split(",") if c.strip()]

    contents = await upload.read()

    with tempfile.NamedTemporaryFile(suffix=".fit", delete=False) as tmp:
        tmp.write(contents)
        tmp_path = Path(tmp.name)

    try:
        activity = Activity.load_fit(tmp_path, columns=columns)
    finally:
        tmp_path.unlink()

    stem = Path(upload.filename).stem if upload.filename else "output"

    if fmt == "csv":
        buf = io.BytesIO()
        pcsv.write_csv(activity.data, buf)
        return Response(
            content=buf.getvalue(),
            media_type="text/csv",
            headers={"Content-Disposition": f'attachment; filename="{stem}.csv"'},
        )

    buf = io.BytesIO()
    activity.to_parquet(buf)
    return Response(
        content=buf.getvalue(),
        media_type="application/octet-stream",
        headers={"Content-Disposition": f'attachment; filename="{stem}.parquet"'},
    )


app = Starlette(routes=[Route("/", index), Route("/convert", convert, methods=["POST"])])

"""Pyroparse HTTP server — convert FIT files to Parquet or CSV."""

from __future__ import annotations

import io
import zipfile
from pathlib import Path

import pyarrow as pa
import pyarrow.csv as pcsv
import pyarrow.parquet as pq
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import HTMLResponse, JSONResponse, Response
from starlette.routing import Route

from pyroparse import Activity, Session
from pyroparse._errors import MultipleActivitiesError
from pyroparse._schema import select_columns

_HTML = (Path(__file__).parent / "index.html").read_text()


def _parse_columns(raw: str | None) -> list[str] | str | None:
    if raw is None or raw in ("standard", ""):
        return None
    if raw == "all":
        return "all"
    return [c.strip() for c in raw.split(",") if c.strip()]


def _write_table(table: pa.Table, fmt: str) -> bytes:
    buf = io.BytesIO()
    if fmt == "csv":
        pcsv.write_csv(table, buf)
    else:
        pq.write_table(table, buf, compression="zstd")
    return buf.getvalue()


def _activity_filename(stem: str, index: int, ext: str, multi: bool) -> str:
    if multi:
        return f"{stem}_{index}_pyroparse.{ext}"
    return f"{stem}_pyroparse.{ext}"


async def index(_request: Request) -> HTMLResponse:
    return HTMLResponse(_HTML)


async def convert(request: Request) -> Response:
    form = await request.form()
    upload = form["file"]
    fmt = form.get("format", "parquet")
    columns = _parse_columns(form.get("columns") or form.get("columns_list"))
    allow_multi = form.get("allow_multi", "").lower() == "true"
    ext = "csv" if fmt == "csv" else "parquet"

    contents = await upload.read()
    stem = Path(upload.filename).stem if upload.filename else "output"

    # Parse the file.
    try:
        activities = [Activity.load_fit(contents, columns=columns)]
    except MultipleActivitiesError as exc:
        if not allow_multi:
            return JSONResponse(
                {"error": f"FIT file contains {exc.count} activities. "
                 "Set allow_multi=true to process all activities (returns zip)."},
                status_code=400,
            )
        session = Session.load_fit(contents)
        activities = session.activities

    # Single file response (no allow_multi flag).
    if not allow_multi:
        table = select_columns(activities[0].data, columns)
        filename = _activity_filename(stem, 0, ext, multi=False)
        content_type = "text/csv" if fmt == "csv" else "application/octet-stream"
        return Response(
            content=_write_table(table, fmt),
            media_type=content_type,
            headers={"Content-Disposition": f'attachment; filename="{filename}"'},
        )

    # Zip response (always include index for predictable parsing).
    zip_buf = io.BytesIO()
    with zipfile.ZipFile(zip_buf, "w", zipfile.ZIP_DEFLATED) as zf:
        for i, act in enumerate(activities):
            table = select_columns(act.data, columns)
            zf.writestr(
                _activity_filename(stem, i, ext, multi=True),
                _write_table(table, fmt),
            )

    return Response(
        content=zip_buf.getvalue(),
        media_type="application/zip",
        headers={"Content-Disposition": f'attachment; filename="{stem}_pyroparse.zip"'},
    )


app = Starlette(routes=[Route("/", index), Route("/convert", convert, methods=["POST"])])

"""Pyroparse HTTP server — convert FIT files to Parquet or CSV."""

from __future__ import annotations

import io
import zipfile
from pathlib import Path

import pyarrow.csv as pcsv
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import HTMLResponse, JSONResponse, Response
from starlette.routing import Route

from pyroparse import Activity, Course, Session
from pyroparse._errors import FileTypeMismatchError, MultipleActivitiesError

_HTML = (Path(__file__).parent / "index.html").read_text()


def _parse_columns(raw: str | None) -> list[str] | str | None:
    if raw is None or raw in ("standard", ""):
        return None
    if raw == "all":
        return "all"
    return [c.strip() for c in raw.split(",") if c.strip()]


def _serialize_activity(activity: Activity, fmt: str) -> bytes:
    buf = io.BytesIO()
    if fmt == "csv":
        pcsv.write_csv(activity.data, buf)
    else:
        activity.to_parquet(buf)
    return buf.getvalue()


def _serialize_course(course: Course, fmt: str) -> bytes:
    buf = io.BytesIO()
    if fmt == "csv":
        pcsv.write_csv(course.track, buf)
    else:
        course.to_parquet(buf)
    return buf.getvalue()


def _filename(stem: str, index: int | None, ext: str) -> str:
    if index is not None:
        return f"{stem}_{index}_pyroparse.{ext}"
    return f"{stem}_pyroparse.{ext}"


async def index(_request: Request) -> HTMLResponse:
    return HTMLResponse(_HTML)


async def activity(request: Request) -> Response:
    form = await request.form()
    upload = form["file"]
    fmt = form.get("format", "parquet")
    columns = _parse_columns(form.get("columns") or form.get("columns_list"))
    ext = "csv" if fmt == "csv" else "parquet"

    contents = await upload.read()
    stem = Path(upload.filename).stem if upload.filename else "output"

    try:
        act = Activity.load_fit(contents, columns=columns)
    except FileTypeMismatchError as exc:
        return JSONResponse(
            {"error": f"This is a {exc.actual} file. Use POST /{exc.actual} instead."},
            status_code=400,
        )
    except MultipleActivitiesError as exc:
        return JSONResponse(
            {"error": f"FIT file contains {exc.count} activities. "
             "Use POST /session instead."},
            status_code=400,
        )

    filename = _filename(stem, None, ext)
    content_type = "text/csv" if fmt == "csv" else "application/octet-stream"
    return Response(
        content=_serialize_activity(act, fmt),
        media_type=content_type,
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


async def session(request: Request) -> Response:
    form = await request.form()
    upload = form["file"]
    fmt = form.get("format", "parquet")
    columns = _parse_columns(form.get("columns") or form.get("columns_list"))
    ext = "csv" if fmt == "csv" else "parquet"

    contents = await upload.read()
    stem = Path(upload.filename).stem if upload.filename else "output"

    try:
        sess = Session.load_fit(contents, columns=columns)
    except FileTypeMismatchError as exc:
        return JSONResponse(
            {"error": f"This is a {exc.actual} file. Use POST /{exc.actual} instead."},
            status_code=400,
        )

    zip_buf = io.BytesIO()
    with zipfile.ZipFile(zip_buf, "w", zipfile.ZIP_DEFLATED) as zf:
        for i, act in enumerate(sess.activities):
            zf.writestr(
                _filename(stem, i, ext),
                _serialize_activity(act, fmt),
            )

    return Response(
        content=zip_buf.getvalue(),
        media_type="application/zip",
        headers={"Content-Disposition": f'attachment; filename="{stem}_pyroparse.zip"'},
    )


async def course(request: Request) -> Response:
    form = await request.form()
    upload = form["file"]
    fmt = form.get("format", "parquet")
    ext = "csv" if fmt == "csv" else "parquet"

    contents = await upload.read()
    stem = Path(upload.filename).stem if upload.filename else "output"

    try:
        crs = Course.load_fit(contents)
    except ValueError:
        # Course.load_fit raises ValueError (from Rust) for wrong file types.
        return JSONResponse(
            {"error": "This is not a course file. Use POST /activity or POST /session instead."},
            status_code=400,
        )

    filename = _filename(stem, None, ext)
    content_type = "text/csv" if fmt == "csv" else "application/octet-stream"
    return Response(
        content=_serialize_course(crs, fmt),
        media_type=content_type,
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


app = Starlette(routes=[
    Route("/", index),
    Route("/activity", activity, methods=["POST"]),
    Route("/session", session, methods=["POST"]),
    Route("/course", course, methods=["POST"]),
])

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

HTML_FORM = """<!doctype html>
<html>
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Pyroparse</title>
  <style>
    * { box-sizing: border-box; margin: 0; padding: 0; }
    body { font-family: system-ui, sans-serif; max-width: 480px; margin: 4rem auto; padding: 0 1rem; color: #1a1a1a; }
    h1 { font-size: 1.5rem; margin-bottom: .25rem; }
    p { color: #666; margin-bottom: 2rem; font-size: .9rem; }
    form { display: flex; flex-direction: column; gap: 1rem; }
    label { font-size: .85rem; font-weight: 500; }
    select, input[type=file] { padding: .5rem; border: 1px solid #ccc; border-radius: 4px; font-size: .9rem; }
    button { padding: .6rem; background: #1a1a1a; color: #fff; border: none; border-radius: 4px; font-size: .9rem; cursor: pointer; }
    button:hover { background: #333; }
  </style>
</head>
<body>
  <h1>Pyroparse</h1>
  <p>Upload a FIT file, get Parquet or CSV back.</p>
  <form action="/convert" method="post" enctype="multipart/form-data">
    <label for="file">FIT file</label>
    <input type="file" id="file" name="file" accept=".fit" required>
    <label for="format">Output format</label>
    <select id="format" name="format">
      <option value="parquet">Parquet</option>
      <option value="csv">CSV</option>
    </select>
    <button type="submit">Convert</button>
  </form>
</body>
</html>"""


async def index(_request: Request) -> HTMLResponse:
    return HTMLResponse(HTML_FORM)


async def convert(request: Request) -> Response:
    form = await request.form()
    upload = form["file"]
    fmt = form.get("format", "parquet")

    contents = await upload.read()

    with tempfile.NamedTemporaryFile(suffix=".fit", delete=False) as tmp:
        tmp.write(contents)
        tmp_path = Path(tmp.name)

    try:
        activity = Activity.load_fit(tmp_path)
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

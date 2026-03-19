"""Download FIT files for the last 1000 activities from SweatStack.

Install deps: uv pip install -e ".[scripts]"
Run: uv run python scripts/download_fit_files.py
"""

from __future__ import annotations

import sys
from pathlib import Path

import httpx
import sweatstack
from sweatstack import Client

DATA_DIR = Path(__file__).resolve().parent.parent / "data" / "fit"
BATCH_SIZE = 100
TOTAL = 1000


def download_fit_files() -> None:
    sweatstack.authenticate()
    client: Client = sweatstack.client._default_client

    DATA_DIR.mkdir(parents=True, exist_ok=True)

    downloaded = 0
    skipped = 0

    for offset in range(0, TOTAL, BATCH_SIZE):
        activities = sweatstack.get_activities(limit=BATCH_SIZE, offset=offset)
        if not activities:
            break

        for activity in activities:
            sport = str(activity.sport.value) if hasattr(activity.sport, "value") else str(activity.sport)
            filename = f"{activity.start_local:%Y%m%d_%H%M%S}_{sport}_{activity.id}.fit"
            dest = DATA_DIR / filename

            if dest.exists():
                skipped += 1
                continue

            url = f"/api/v1/activities/{activity.id}/download"
            try:
                with client._http_client() as http:
                    resp = http.get(url)
                    resp.raise_for_status()

                dest.write_bytes(resp.content)
                downloaded += 1
                total = downloaded + skipped
                print(f"[{total}/{TOTAL}] {filename}")

            except httpx.HTTPStatusError as e:
                print(f"  SKIP {activity.id}: HTTP {e.response.status_code}", file=sys.stderr)

    print(f"\nDone: {downloaded} downloaded, {skipped} already existed.")


if __name__ == "__main__":
    download_fit_files()

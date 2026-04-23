#!/usr/bin/env python3
"""Verify the deployed GitHub Pages site is reachable and not stale."""

from __future__ import annotations

import argparse
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

import pandas as pd


DEFAULT_WORKBOOK = Path("exports/addsub/加仓减仓分表版20260312_20260409.xlsx")


def latest_date_from_workbook(path: Path) -> str:
    raw = pd.read_excel(path, sheet_name="Raw_Data")
    raw["统计日期"] = pd.to_datetime(raw["统计日期"], errors="coerce")
    latest = raw["统计日期"].max()
    if pd.isna(latest):
        raise RuntimeError("Raw_Data 中无法解析最新统计日期")
    return latest.strftime("%Y-%m-%d")


def fetch(url: str, timeout: int) -> tuple[int, str]:
    request = urllib.request.Request(
        url,
        headers={
            "User-Agent": "baoying-dashboard-qa/1.0",
            "Cache-Control": "no-cache",
        },
    )
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            body = response.read().decode("utf-8", errors="ignore")
            return int(response.status), body
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="ignore")
        return int(exc.code), body


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", required=True, help="GitHub Pages URL to check")
    parser.add_argument("--workbook", type=Path, default=DEFAULT_WORKBOOK)
    parser.add_argument("--attempts", type=int, default=12)
    parser.add_argument("--sleep", type=int, default=15)
    parser.add_argument("--timeout", type=int, default=25)
    args = parser.parse_args()

    url = args.url.rstrip("/") + "/"
    latest_date = latest_date_from_workbook(args.workbook)
    expected_markers = ["核心看板", latest_date]

    last_status = None
    last_excerpt = ""
    for attempt in range(1, args.attempts + 1):
        status, body = fetch(url, args.timeout)
        last_status = status
        last_excerpt = body[:240].replace("\n", " ")
        marker_ok = all(marker in body for marker in expected_markers)
        print(
            f"[online-check] attempt={attempt}/{args.attempts} "
            f"url={url} status={status} latest_date={latest_date} marker_ok={marker_ok}"
        )
        if status == 200 and marker_ok:
            print("[online-check] PASS GitHub Pages is reachable and current")
            return 0
        if attempt < args.attempts:
            time.sleep(args.sleep)

    print("[online-check] FAIL GitHub Pages did not become healthy", file=sys.stderr)
    print(f"[online-check] last_status={last_status}", file=sys.stderr)
    print(f"[online-check] last_excerpt={last_excerpt}", file=sys.stderr)
    return 1


if __name__ == "__main__":
    raise SystemExit(main())

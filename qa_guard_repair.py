#!/usr/bin/env python3
"""Preflight/rebuild/QA guard for add-sub dashboard publishing.

This script is intentionally conservative:
- preflight: fail fast when build scripts cannot compile.
- rebuild: regenerate HTML/docs from the current workbook, repairing stale pages.
- post: run workbook QA + strict HTML data-block consistency checks.

If any check fails, GitHub Actions must stop before publishing Pages.
"""
from __future__ import annotations

import argparse
import py_compile
import subprocess
import sys
from pathlib import Path

BASE = Path(__file__).resolve().parent
OUT = BASE / "exports" / "addsub"
WORKBOOK = OUT / "加仓减仓分表版20260312_20260409.xlsx"
CORE_HTML = OUT / "看板_核心版_20260312_20260409.html"
OPS_HTML = OUT / "运营指标执行看板_动态版.html"
WEAK_HTML = OUT / "竞品弱点雷达附表.html"
DETAIL_XLSX = OUT / "基金详情抓取_中文增强版_20260413.xlsx"
DETAIL_RAW = OUT / "基金详情抓取_中文增强版_20260413_raw.json"
COCKPIT_HTML = OUT / "基金详情运营驾驶舱_20260413.html"
QUICKSTART_HTML = OUT / "看板_新手导航.html"
DOCS = BASE / "docs"

BUILD_SCRIPTS = [
    "update_daily_append_and_dashboard.py",
    "build_core_dashboard_from_split.py",
    "build_ops_metrics_live_dashboard.py",
    "build_competitor_weakness_dashboard.py",
    "build_fund_detail_ops_dashboard.py",
    "build_quickstart_guide.py",
    "build_xhs_crawler_dashboard.py",
    "prepare_pages_bundle.py",
    "qa_validate_pipeline.py",
    "qa_strict_end_to_end.py",
    "run_full_update_pipeline.py",
]


def run(cmd: list[str]) -> None:
    print("$", " ".join(cmd), flush=True)
    subprocess.run(cmd, cwd=BASE, check=True)


def preflight() -> None:
    for name in BUILD_SCRIPTS:
        path = BASE / name
        if not path.exists():
            raise SystemExit(f"missing required script: {path}")
        print(f"[compile] {name}")
        py_compile.compile(str(path), doraise=True)
    print("[OK] preflight compile checks passed")


def rebuild_static() -> None:
    if not WORKBOOK.exists():
        raise SystemExit(f"missing workbook: {WORKBOOK}")
    if not DETAIL_RAW.exists():
        print(f"[WARN] detail raw json missing: {DETAIL_RAW}; investment direction may be blank")
    run([sys.executable, "build_core_dashboard_from_split.py", "--input", str(WORKBOOK), "--output", str(CORE_HTML), "--detail-raw-json", str(DETAIL_RAW)])
    run([sys.executable, "build_ops_metrics_live_dashboard.py", "--input", str(WORKBOOK), "--output", str(OPS_HTML), "--detail-raw-json", str(DETAIL_RAW)])
    run([sys.executable, "build_competitor_weakness_dashboard.py", "--input", str(WORKBOOK), "--output", str(WEAK_HTML), "--detail-raw-json", str(DETAIL_RAW)])
    if DETAIL_XLSX.exists():
        run([sys.executable, "build_fund_detail_ops_dashboard.py", "--input", str(DETAIL_XLSX), "--output", str(COCKPIT_HTML)])
    else:
        print(f"[WARN] detail workbook missing: {DETAIL_XLSX}; skip cockpit rebuild")
    run([sys.executable, "build_quickstart_guide.py", "--output", str(QUICKSTART_HTML)])
    run([sys.executable, "prepare_pages_bundle.py", "--source-dir", str(OUT), "--docs-dir", str(DOCS)])
    print("[OK] static dashboards/docs rebuilt")


def post_qa() -> None:
    run([sys.executable, "qa_validate_pipeline.py", "--workbook", str(WORKBOOK)])
    run([
        sys.executable, "qa_strict_end_to_end.py",
        "--workbook", str(WORKBOOK),
        "--core-html", str(CORE_HTML),
        "--ops-html", str(OPS_HTML),
        "--weak-html", str(WEAK_HTML),
    ])
    print("[OK] post-update QA gate passed")


def main() -> int:
    parser = argparse.ArgumentParser(description="Compile/rebuild/QA guard for fund add-sub dashboards")
    parser.add_argument("--mode", choices=["preflight", "rebuild", "post", "all"], default="all")
    args = parser.parse_args()
    if args.mode in {"preflight", "all"}:
        preflight()
    if args.mode in {"rebuild", "all"}:
        rebuild_static()
    if args.mode in {"post", "all"}:
        post_qa()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

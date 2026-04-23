#!/usr/bin/env python3
"""Run full daily update pipeline and prepare GitHub Pages docs."""

from __future__ import annotations

import argparse
import subprocess
from pathlib import Path


BASE = Path(__file__).resolve().parent
OUT = BASE / "exports" / "addsub"


def run(cmd: list[str]) -> None:
    safe_cmd = cmd[:]
    for key in ["--password"]:
        if key in safe_cmd:
            idx = safe_cmd.index(key)
            if idx + 1 < len(safe_cmd):
                safe_cmd[idx + 1] = "***"
    print("$", " ".join(safe_cmd))
    subprocess.run(cmd, check=True)



def main() -> int:
    parser = argparse.ArgumentParser(description="Run full pipeline: update data + dashboards + docs bundle")
    parser.add_argument("--namespace-name", required=True)
    parser.add_argument("--user-name", required=True)
    parser.add_argument("--password", required=True)
    parser.add_argument("--target", default="https://www.cdollar.cn/leshu-pro/#/e/vq6aKqp5YU")
    parser.add_argument("--browser-id", default="09cb220223cb45410e11e84679b83fb6")
    parser.add_argument("--date", help="Optional YYYY-MM-DD")
    parser.add_argument("--max-funds", type=int, default=None)
    parser.add_argument(
        "--skip-detail-crawl",
        action="store_true",
        help="Skip crawling fund detail pages (recommended for scheduled quick updates).",
    )
    args = parser.parse_args()

    workbook = OUT / "加仓减仓分表版20260312_20260409.xlsx"
    core_html = OUT / "看板_核心版_20260312_20260409.html"
    detail_xlsx = OUT / "基金详情抓取_中文增强版_20260413.xlsx"
    detail_raw = OUT / "基金详情抓取_中文增强版_20260413_raw.json"
    ops_html = OUT / "运营指标执行看板_动态版.html"
    comp_html = OUT / "竞品弱点雷达附表.html"
    cockpit_html = OUT / "基金详情运营驾驶舱_20260413.html"
    quickstart_html = OUT / "看板_新手导航.html"
    xhs_html = OUT / "看板_小红书任务控制台.html"

    cmd = [
        "python3", str(BASE / "update_daily_append_and_dashboard.py"),
        "--namespace-name", args.namespace_name,
        "--user-name", args.user_name,
        "--password", args.password,
        "--target", args.target,
        "--browser-id", args.browser_id,
        "--workbook", str(workbook),
        "--dashboard", str(core_html),
    ]
    if args.date:
        cmd.extend(["--date", args.date])
    run(cmd)

    if not args.skip_detail_crawl:
        cmd = [
            "python3", str(BASE / "crawl_fund_detail_pages.py"),
            "--namespace-name", args.namespace_name,
            "--user-name", args.user_name,
            "--password", args.password,
            "--target", args.target,
            "--browser-id", args.browser_id,
            "--workbook", str(workbook),
            "--output", str(detail_xlsx),
            "--raw-json-output", str(detail_raw),
            "--workers", "8",
        ]
        if args.date:
            cmd.extend(["--date", args.date])
        if args.max_funds:
            cmd.extend(["--max-funds", str(args.max_funds)])
        run(cmd)
    else:
        print("skip_detail_crawl=true (scheduled quick update mode)")

    run([
        "python3", str(BASE / "build_core_dashboard_from_split.py"),
        "--input", str(workbook),
        "--output", str(core_html),
        "--detail-raw-json", str(detail_raw),
    ])
    run([
        "python3", str(BASE / "build_ops_metrics_live_dashboard.py"),
        "--input", str(workbook),
        "--output", str(ops_html),
        "--detail-raw-json", str(detail_raw),
    ])
    run([
        "python3", str(BASE / "build_competitor_weakness_dashboard.py"),
        "--input", str(workbook),
        "--output", str(comp_html),
        "--detail-raw-json", str(detail_raw),
    ])
    if detail_xlsx.exists():
        run([
            "python3", str(BASE / "build_fund_detail_ops_dashboard.py"),
            "--input", str(detail_xlsx),
            "--output", str(cockpit_html),
        ])
    else:
        print(f"warn_detail_xlsx_missing_skip_cockpit_build: {detail_xlsx}")
    run([
        "python3", str(BASE / "build_quickstart_guide.py"),
        "--output", str(quickstart_html),
    ])
    run([
        "python3", str(BASE / "build_xhs_crawler_dashboard.py"),
    ])
    docs_dir = BASE / "docs"
    run([
        "python3", str(BASE / "prepare_pages_bundle.py"),
        "--source-dir", str(OUT),
        "--docs-dir", str(docs_dir),
    ])

    print("pipeline_done")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

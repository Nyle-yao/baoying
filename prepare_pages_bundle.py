#!/usr/bin/env python3
"""Prepare static GitHub Pages bundle from dashboard HTML outputs."""

from __future__ import annotations

import argparse
from pathlib import Path


ROUTE_MAP = {
    'href="/"': 'href="./index.html"',
    "href='/" : "href='./index.html",
    'href="/ops-metrics"': 'href="./ops-metrics.html"',
    'href="/ops-metrics.html"': 'href="./ops-metrics.html"',
    'href="/competitor-weakness"': 'href="./competitor-weakness.html"',
    'href="/competitor-weakness.html"': 'href="./competitor-weakness.html"',
    'href="/metrics-doc"': 'href="./metrics-doc.html"',
    'href="/metrics-doc.html"': 'href="./metrics-doc.html"',
    'href="/fund-detail-cockpit"': 'href="./fund-detail-cockpit.html"',
    'href="/fund-detail-cockpit.html"': 'href="./fund-detail-cockpit.html"',
    'href="/v2-pilot"': 'href="./index.html"',
    'href="/v2-pilot.html"': 'href="./index.html"',
}


FILES = {
    "index.html": "看板_核心版_20260312_20260409.html",
    "ops-metrics.html": "运营指标执行看板_动态版.html",
    "competitor-weakness.html": "竞品弱点雷达附表.html",
    "fund-detail-cockpit.html": "基金详情运营驾驶舱_20260413.html",
    "metrics-doc.html": "运营指标总览看板_运营版.html",
}



def rewrite_html(html: str, static_mode: bool = False) -> str:
    out = html
    for src, dst in ROUTE_MAP.items():
        out = out.replace(src, dst)

    if static_mode:
        out = out.replace('id="btn_update"', 'id="btn_update" disabled title="GitHub Pages静态部署不支持手动更新"')
        out = out.replace('再次爬取检测更新', '自动更新由GitHub Actions执行')

    return out



def write_route_aliases(doc_root: Path) -> None:
    alias_dirs = {
        "ops-metrics": "ops-metrics.html",
        "competitor-weakness": "competitor-weakness.html",
        "metrics-doc": "metrics-doc.html",
        "fund-detail-cockpit": "fund-detail-cockpit.html",
    }
    for d, target in alias_dirs.items():
        route_dir = doc_root / d
        route_dir.mkdir(parents=True, exist_ok=True)
        (route_dir / "index.html").write_text(
            f'<!doctype html><meta charset="utf-8"><meta http-equiv="refresh" content="0; url=../{target}">',
            encoding="utf-8",
        )



def main() -> int:
    parser = argparse.ArgumentParser(description="Prepare static pages bundle")
    parser.add_argument("--source-dir", required=True)
    parser.add_argument("--docs-dir", required=True)
    args = parser.parse_args()

    src = Path(args.source_dir)
    docs = Path(args.docs_dir)
    docs.mkdir(parents=True, exist_ok=True)
    # cleanup retired pages
    stale_files = [docs / "v2-pilot.html", docs / "v2-pilot" / "index.html"]
    for f in stale_files:
        if f.exists():
            f.unlink()
    stale_dir = docs / "v2-pilot"
    if stale_dir.exists():
        try:
            stale_dir.rmdir()
        except OSError:
            pass

    for out_name, src_name in FILES.items():
        in_file = src / src_name
        if not in_file.exists():
            if out_name == "metrics-doc.html":
                # Metrics doc is optional in CI bootstrap; keep publish pipeline alive.
                (docs / out_name).write_text(
                    "<!doctype html><meta charset='utf-8'><title>指标文档</title><body style='font-family:PingFang SC,Microsoft YaHei,sans-serif;padding:24px;'>"
                    "<h2>指标文档暂未生成</h2><p>本次自动更新未产出指标文档文件，核心/动态/竞品/驾驶舱看板不受影响。</p></body>",
                    encoding="utf-8",
                )
                continue
            raise SystemExit(f"missing dashboard file: {in_file}")
        html = in_file.read_text(encoding="utf-8", errors="replace")
        static_mode = out_name == "index.html"
        (docs / out_name).write_text(rewrite_html(html, static_mode=static_mode), encoding="utf-8")

    write_route_aliases(docs)
    print(docs)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

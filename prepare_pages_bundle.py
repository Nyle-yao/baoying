#!/usr/bin/env python3
"""Prepare static GitHub Pages bundle from dashboard HTML outputs."""

from __future__ import annotations

import argparse
from datetime import datetime
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
    'href="/quickstart"': 'href="./quickstart.html"',
    'href="/quickstart.html"': 'href="./quickstart.html"',
    'href="/xhs-crawler"': 'href="./xhs-crawler.html"',
    'href="/xhs-crawler.html"': 'href="./xhs-crawler.html"',
    'href="/v2-pilot"': 'href="./index.html"',
    'href="/v2-pilot.html"': 'href="./index.html"',
}


FILES = {
    "index.html": "看板_核心版_20260312_20260409.html",
    "ops-metrics.html": "运营指标执行看板_动态版.html",
    "competitor-weakness.html": "竞品弱点雷达附表.html",
    "fund-detail-cockpit.html": "基金详情运营驾驶舱_20260413.html",
    "metrics-doc.html": "运营指标总览看板_运营版.html",
    "quickstart.html": "看板_新手导航.html",
    "xhs-crawler.html": "看板_小红书任务控制台.html",
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
        "quickstart": "quickstart.html",
        "xhs-crawler": "xhs-crawler.html",
        "maintenance": "maintenance.html",
    }
    for d, target in alias_dirs.items():
        route_dir = doc_root / d
        route_dir.mkdir(parents=True, exist_ok=True)
        (route_dir / "index.html").write_text(
            f'<!doctype html><meta charset="utf-8"><meta http-equiv="refresh" content="0; url=../{target}">',
            encoding="utf-8",
        )


def write_maintenance_pages(doc_root: Path) -> None:
    generated_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    html = f"""<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>看板正在维护</title>
  <style>
    body {{
      margin:0; min-height:100vh; display:flex; align-items:center; justify-content:center;
      font-family:"PingFang SC","Microsoft YaHei",sans-serif; color:#172033;
      background:linear-gradient(135deg,#f7f1df 0%,#eaf3ff 52%,#f7fbf5 100%);
    }}
    .card {{
      width:min(720px, calc(100vw - 40px)); background:rgba(255,255,255,.88);
      border:1px solid rgba(148,163,184,.35); border-radius:22px; padding:30px;
      box-shadow:0 22px 70px rgba(15,23,42,.12);
    }}
    h1 {{ margin:0 0 12px; font-size:30px; }}
    p {{ margin:8px 0; color:#475569; line-height:1.7; font-size:15px; }}
    .time {{ margin-top:18px; padding:14px; border-radius:14px; background:#f8fafc; border:1px solid #e2e8f0; }}
    .btns {{ display:flex; flex-wrap:wrap; gap:10px; margin-top:20px; }}
    a {{ text-decoration:none; color:#fff; background:#111827; border-radius:999px; padding:10px 14px; font-size:14px; }}
    a.secondary {{ color:#111827; background:#fff; border:1px solid #cbd5e1; }}
    code {{ background:#eef2ff; padding:2px 6px; border-radius:6px; }}
  </style>
</head>
<body>
  <main class="card">
    <h1>看板正在维护</h1>
    <p>当前页面暂时没有正常展示，可能是 GitHub Pages 正在部署、缓存刷新中，或访问了不存在的页面。</p>
    <p>如果这是核心看板入口，请稍后刷新；如果持续打不开，说明 Pages 设置或本次部署需要检查。</p>
    <div class="time">
      <p><strong>页面生成时间：</strong>{generated_at}</p>
      <p><strong>浏览器当前时间：</strong><span id="now">读取中...</span></p>
      <p><strong>处理状态：</strong>系统会在 GitHub Actions 中执行数据 QA、页面生成 QA 和线上可访问性自检。</p>
    </div>
    <div class="btns">
      <a href="./index.html">返回核心看板</a>
      <a class="secondary" href="./quickstart.html">查看新手导航</a>
      <a class="secondary" href="https://github.com/Nyle-yao/baoying/actions">查看部署进度</a>
    </div>
  </main>
  <script>
    function tick() {{
      document.getElementById("now").textContent = new Date().toLocaleString("zh-CN", {{ hour12:false }});
    }}
    tick();
    setInterval(tick, 1000);
  </script>
</body>
</html>
"""
    (doc_root / "maintenance.html").write_text(html, encoding="utf-8")
    (doc_root / "404.html").write_text(html, encoding="utf-8")



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

    # Keep the source freshness probe visible on Pages for audit/debugging.
    freshness = src / "source_freshness.json"
    if freshness.exists():
        (docs / "source_freshness.json").write_text(
            freshness.read_text(encoding="utf-8", errors="replace"),
            encoding="utf-8",
        )
    # Historical link compatibility.
    quickstart = docs / "quickstart.html"
    if quickstart.exists():
        (docs / "quick-start.html").write_text(quickstart.read_text(encoding="utf-8"), encoding="utf-8")

    write_route_aliases(docs)
    write_maintenance_pages(docs)
    print(docs)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

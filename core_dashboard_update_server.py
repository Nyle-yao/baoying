#!/usr/bin/env python3
"""Serve core dashboard and provide /api/update endpoint."""

from __future__ import annotations

import argparse
import json
import os
import subprocess
from datetime import datetime
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any

import pandas as pd


def get_latest_date(workbook: Path) -> str:
    try:
        df = pd.read_excel(workbook, sheet_name="Raw_Data", usecols=["统计日期"])
        vals = pd.to_datetime(df["统计日期"], errors="coerce").dropna()
        if len(vals) == 0:
            return ""
        return vals.max().strftime("%Y-%m-%d")
    except Exception:
        return ""


def main() -> int:
    parser = argparse.ArgumentParser(description="Serve dashboard with update API.")
    parser.add_argument("--host", default=os.getenv("DASHBOARD_HOST", "127.0.0.1"))
    parser.add_argument("--port", type=int, default=8770)
    parser.add_argument("--namespace-name")
    parser.add_argument("--user-name")
    parser.add_argument("--password")
    parser.add_argument("--target", default=os.getenv("CD_TARGET", "https://www.cdollar.cn/leshu-pro/#/e/vq6aKqp5YU"))
    parser.add_argument("--browser-id", default=os.getenv("CD_BROWSER_ID", "09cb220223cb45410e11e84679b83fb6"))
    parser.add_argument(
        "--workbook",
        default=os.getenv("DASHBOARD_WORKBOOK", "/Users/yaoruanxingchen/c/exports/addsub/加仓减仓分表版20260312_20260409.xlsx"),
    )
    parser.add_argument(
        "--dashboard",
        default=os.getenv("DASHBOARD_HTML", "/Users/yaoruanxingchen/c/exports/addsub/看板_核心版_20260312_20260409.html"),
    )
    parser.add_argument(
        "--ops-metrics-dashboard",
        default=os.getenv("OPS_METRICS_HTML", "/Users/yaoruanxingchen/c/exports/addsub/运营指标执行看板_动态版.html"),
    )
    parser.add_argument(
        "--competitor-dashboard",
        default=os.getenv("COMPETITOR_WEAKNESS_HTML", "/Users/yaoruanxingchen/c/exports/addsub/竞品弱点雷达附表.html"),
    )
    parser.add_argument(
        "--metrics-doc-dashboard",
        default=os.getenv("METRICS_DOC_HTML", "/Users/yaoruanxingchen/c/exports/addsub/运营指标总览看板_运营版.html"),
    )
    parser.add_argument(
        "--fund-detail-cockpit-dashboard",
        default=os.getenv("FUND_DETAIL_COCKPIT_HTML", "/Users/yaoruanxingchen/c/exports/addsub/基金详情运营驾驶舱_20260413.html"),
    )
    parser.add_argument(
        "--quickstart-dashboard",
        default=os.getenv("QUICKSTART_HTML", "/Users/yaoruanxingchen/c/exports/addsub/看板_新手导航.html"),
    )
    parser.add_argument(
        "--v2-pilot-dashboard",
        default=os.getenv("V2_PILOT_HTML", "/Users/yaoruanxingchen/c/exports/addsub/看板_V2试运行版.html"),
    )
    args = parser.parse_args()

    args.namespace_name = args.namespace_name or os.getenv("CD_NAMESPACE_NAME")
    args.user_name = args.user_name or os.getenv("CD_USER_NAME")
    args.password = args.password or os.getenv("CD_PASSWORD")
    if not args.namespace_name or not args.user_name or not args.password:
        raise SystemExit("Missing credentials: provide --namespace-name/--user-name/--password or set CD_NAMESPACE_NAME/CD_USER_NAME/CD_PASSWORD")

    workbook = Path(args.workbook)
    dashboard = Path(args.dashboard)
    ops_metrics_dashboard = Path(args.ops_metrics_dashboard)
    competitor_dashboard = Path(args.competitor_dashboard)
    metrics_doc_dashboard = Path(args.metrics_doc_dashboard)
    fund_detail_cockpit_dashboard = Path(args.fund_detail_cockpit_dashboard)
    quickstart_dashboard = Path(args.quickstart_dashboard)
    v2_pilot_dashboard = Path(args.v2_pilot_dashboard)

    class Handler(BaseHTTPRequestHandler):
        def _send_json(self, payload: dict[str, Any], status: int = HTTPStatus.OK) -> None:
            body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def do_GET(self) -> None:
            if self.path == "/" or self.path.startswith("/index.html"):
                if not dashboard.exists():
                    self.send_error(HTTPStatus.NOT_FOUND, "Dashboard not found")
                    return
                body = dashboard.read_bytes()
                self.send_response(HTTPStatus.OK)
                self.send_header("Content-Type", "text/html; charset=utf-8")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
                return
            if self.path in ("/ops-metrics", "/ops-metrics.html"):
                if not ops_metrics_dashboard.exists():
                    self.send_error(HTTPStatus.NOT_FOUND, "Ops metrics dashboard not found")
                    return
                body = ops_metrics_dashboard.read_bytes()
                self.send_response(HTTPStatus.OK)
                self.send_header("Content-Type", "text/html; charset=utf-8")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
                return
            if self.path in ("/competitor-weakness", "/competitor-weakness.html"):
                if not competitor_dashboard.exists():
                    self.send_error(HTTPStatus.NOT_FOUND, "Competitor weakness dashboard not found")
                    return
                body = competitor_dashboard.read_bytes()
                self.send_response(HTTPStatus.OK)
                self.send_header("Content-Type", "text/html; charset=utf-8")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
                return
            if self.path in ("/metrics-doc", "/metrics-doc.html"):
                if not metrics_doc_dashboard.exists():
                    self.send_error(HTTPStatus.NOT_FOUND, "Metrics doc dashboard not found")
                    return
                body = metrics_doc_dashboard.read_bytes()
                self.send_response(HTTPStatus.OK)
                self.send_header("Content-Type", "text/html; charset=utf-8")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
                return
            if self.path in ("/fund-detail-cockpit", "/fund-detail-cockpit.html"):
                if not fund_detail_cockpit_dashboard.exists():
                    self.send_error(HTTPStatus.NOT_FOUND, "Fund detail cockpit dashboard not found")
                    return
                body = fund_detail_cockpit_dashboard.read_bytes()
                self.send_response(HTTPStatus.OK)
                self.send_header("Content-Type", "text/html; charset=utf-8")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
                return
            if self.path in ("/quickstart", "/quickstart.html"):
                if not quickstart_dashboard.exists():
                    self.send_error(HTTPStatus.NOT_FOUND, "Quickstart dashboard not found")
                    return
                body = quickstart_dashboard.read_bytes()
                self.send_response(HTTPStatus.OK)
                self.send_header("Content-Type", "text/html; charset=utf-8")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
                return
            if self.path in ("/v2-pilot", "/v2-pilot.html"):
                if not v2_pilot_dashboard.exists():
                    self.send_error(HTTPStatus.NOT_FOUND, "V2 pilot dashboard not found")
                    return
                body = v2_pilot_dashboard.read_bytes()
                self.send_response(HTTPStatus.OK)
                self.send_header("Content-Type", "text/html; charset=utf-8")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
                return
            if self.path == "/api/status":
                self._send_json(
                    {
                        "latestDate": get_latest_date(workbook),
                        "workbookMtime": datetime.fromtimestamp(workbook.stat().st_mtime).strftime("%Y-%m-%d %H:%M:%S")
                        if workbook.exists()
                        else "",
                    }
                )
                return
            self.send_error(HTTPStatus.NOT_FOUND)

        def do_POST(self) -> None:
            if self.path != "/api/update":
                self.send_error(HTTPStatus.NOT_FOUND)
                return
            try:
                cmd = [
                    "python3",
                    "/Users/yaoruanxingchen/c/update_daily_append_and_dashboard.py",
                    "--namespace-name",
                    args.namespace_name,
                    "--user-name",
                    args.user_name,
                    "--password",
                    args.password,
                    "--target",
                    args.target,
                    "--browser-id",
                    args.browser_id,
                    "--workbook",
                    str(workbook),
                    "--dashboard",
                    str(dashboard),
                ]
                subprocess.run(cmd, check=True, capture_output=True, text=True)
                self._send_json({"ok": True, "latestDate": get_latest_date(workbook)})
            except subprocess.CalledProcessError as exc:
                self._send_json({"ok": False, "error": (exc.stderr or exc.stdout or str(exc))[-5000:]}, HTTPStatus.INTERNAL_SERVER_ERROR)

        def log_message(self, format: str, *args_: Any) -> None:
            return

    server = ThreadingHTTPServer((args.host, args.port), Handler)
    print(f"Dashboard server: http://{args.host}:{args.port}")
    server.serve_forever()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

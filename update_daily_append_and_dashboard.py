#!/usr/bin/env python3
"""Append today's add/sub data to existing workbook and rebuild dashboard."""

from __future__ import annotations

import argparse
import json
import ssl
import subprocess
import time
import urllib.error
import urllib.request
from datetime import date, datetime
from pathlib import Path
from typing import Any

import pandas as pd

BASE_DIR = Path(__file__).resolve().parent

LOGIN_URL = "https://asset.cdollar.cn/yuqing-common/mid/login"
ADD_SUB_URL = "https://asset.cdollar.cn/leshu-dashboard-new/shelf/zfb/getZfbAddSubRankingData"
COMPANY_LIST_URL = "https://asset.cdollar.cn/leshu-dashboard-new/fundCompany/listFundCompany"
DEFAULT_TARGET = "https://www.cdollar.cn/leshu-pro/#/e/vq6aKqp5YU"
DEFAULT_BROWSER_ID = "09cb220223cb45410e11e84679b83fb6"

RANKING_KIND_LABEL = {"add": "加仓榜", "sub": "减仓榜"}
FUND_SCOPE_LABEL = {"all": "全部基金", "stock": "偏股基金", "bond": "偏债基金", "qd": "QD基金"}


def post_json(url: str, payload: dict[str, Any], headers: dict[str, str], retries: int = 3) -> dict[str, Any]:
    body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    last_error: Exception | None = None
    for attempt in range(1, retries + 1):
        req = urllib.request.Request(url, data=body, method="POST")
        req.add_header("Content-Type", "application/json")
        for k, v in headers.items():
            req.add_header(k, v)
        try:
            with urllib.request.urlopen(req, timeout=30) as resp:
                return json.loads(resp.read().decode("utf-8"))
        except urllib.error.HTTPError as exc:
            detail = exc.read().decode("utf-8", errors="replace")
            raise RuntimeError(f"HTTP {exc.code} for {url}: {detail}") from exc
        except (urllib.error.URLError, ssl.SSLError, TimeoutError, ConnectionError) as exc:
            last_error = exc
            if attempt < retries:
                time.sleep(attempt)
                continue
            break
    raise RuntimeError(f"Network error for {url}: {last_error}")


def login(namespace_name: str, user_name: str, password: str, target: str, browser_id: str) -> str:
    payload = {
        "namespaceName": namespace_name,
        "userName": user_name,
        "password": password,
        "remember": True,
        "target": target,
        "browserId": browser_id,
    }
    r = post_json(LOGIN_URL, payload, {})
    if r.get("errCode") != "e0000":
        raise RuntimeError(f"Login failed: {r}")
    token = r.get("body", {}).get("token")
    if not token:
        raise RuntimeError("Login token missing")
    return token


def fetch_company_map(token: str) -> dict[str, str]:
    r = post_json(COMPANY_LIST_URL, {}, {"token": token})
    if r.get("code") != "0000":
        return {}
    out: dict[str, str] = {}
    for it in r.get("data", []):
        cid = str(it.get("id") or "").strip()
        cname = str(it.get("companyName") or "").strip()
        if cid and cname:
            out[cid] = cname
    return out


def is_trading_day(day: date, trading_calendar: Path) -> bool:
    if day.weekday() >= 5:
        return False
    if not trading_calendar.exists():
        return True
    payload = json.loads(trading_calendar.read_text(encoding="utf-8"))
    closed = set(payload.get("non_trading_days", {}).get(day.strftime("%Y"), []))
    return day.strftime("%Y%m%d") not in closed


def norm_code(v: Any) -> str:
    if v is None:
        return ""
    s = str(v).strip()
    if s.endswith(".0"):
        s = s[:-2]
    return s.zfill(6) if s.isdigit() else s


def norm_return_pct(v: Any) -> float | None:
    """Normalize percent fields; some API rows occasionally come scaled by 10000."""
    x = pd.to_numeric(v, errors="coerce")
    if pd.isna(x):
        return None
    x = float(x)
    if abs(x) > 100:
        x = x / 10000.0
    return x


def fetch_one_combo(token: str, date_str: str, ranking_kind: str, fund_scope: str, show_by: int) -> list[dict[str, Any]]:
    type_name = f"{RANKING_KIND_LABEL[ranking_kind]}-{FUND_SCOPE_LABEL[fund_scope]}"
    page_no = 1
    rows: list[dict[str, Any]] = []
    while True:
        payload = {
            "date": date_str,
            "showBy": show_by,
            "type": type_name,
            "pageNo": page_no,
            "pageSize": 200,
        }
        resp = post_json(ADD_SUB_URL, payload, {"token": token})
        if resp.get("code") != "0000":
            raise RuntimeError(f"API failed for {date_str} {type_name}: {resp}")
        data = resp.get("data", {})
        rows.extend(data.get("list", []))
        if not data.get("hasNextPage"):
            break
        page_no += 1
    return rows


def build_new_rows(token: str, day: date, show_by: int, company_map: dict[str, str]) -> pd.DataFrame:
    date_str = day.strftime("%Y-%m-%d")
    out: list[dict[str, Any]] = []
    for k in ["add", "sub"]:
        for s in ["all", "stock", "bond", "qd"]:
            rows = fetch_one_combo(token, date_str, k, s, show_by)
            for r in rows:
                cid = str(r.get("companyId") or "").strip()
                out.append(
                    {
                        "统计日期": date_str,
                        "榜单类型": RANKING_KIND_LABEL[k],
                        "基金范围": FUND_SCOPE_LABEL[s],
                        "榜单名次": r.get("ranking"),
                        "基金简称": r.get("fundName"),
                        "基金代码": norm_code(r.get("fundCode")),
                        "基金类型": r.get("fundType"),
                        "日涨跌幅(%)": norm_return_pct(r.get("dayInc")),
                        "近1月涨跌幅(%)": norm_return_pct(r.get("monthInc")),
                        "近1年涨跌幅(%)": norm_return_pct(r.get("yearInc")),
                        "近7日上榜天数": r.get("onRank7d"),
                        "连续上榜天数": r.get("consecutiveDay"),
                        "名次变动": r.get("rankChange"),
                        "数据更新时间": r.get("updateTime"),
                        "基金公司ID": cid,
                        "基金公司名称": company_map.get(cid, ""),
                    }
                )
    return pd.DataFrame(out)


def append_and_save(workbook: Path, new_df: pd.DataFrame) -> None:
    add_df = pd.read_excel(workbook, sheet_name="加仓榜")
    sub_df = pd.read_excel(workbook, sheet_name="减仓榜")
    raw_df = pd.read_excel(workbook, sheet_name="Raw_Data")

    for df in (add_df, sub_df, raw_df, new_df):
        df["基金代码"] = df["基金代码"].map(norm_code)
        df["统计日期"] = pd.to_datetime(df["统计日期"], errors="coerce").dt.strftime("%Y-%m-%d")
        for c in ["日涨跌幅(%)", "近1月涨跌幅(%)", "近1年涨跌幅(%)"]:
            if c in df.columns:
                df[c] = df[c].map(norm_return_pct)

    add_new = new_df[new_df["榜单类型"] == "加仓榜"]
    sub_new = new_df[new_df["榜单类型"] == "减仓榜"]

    key_cols = ["统计日期", "榜单类型", "基金范围", "基金代码", "基金简称"]
    add_all = pd.concat([add_df, add_new], ignore_index=True).drop_duplicates(subset=key_cols, keep="last")
    sub_all = pd.concat([sub_df, sub_new], ignore_index=True).drop_duplicates(subset=key_cols, keep="last")
    raw_all = pd.concat([raw_df, new_df], ignore_index=True).drop_duplicates(subset=key_cols, keep="last")

    add_all = add_all.sort_values(["统计日期", "基金范围", "榜单名次"], ascending=[False, True, True])
    sub_all = sub_all.sort_values(["统计日期", "基金范围", "榜单名次"], ascending=[False, True, True])
    raw_all = raw_all.sort_values(["统计日期", "榜单类型", "基金范围", "榜单名次"], ascending=[False, True, True, True])

    with pd.ExcelWriter(workbook, engine="openpyxl") as writer:
        add_all.to_excel(writer, sheet_name="加仓榜", index=False)
        sub_all.to_excel(writer, sheet_name="减仓榜", index=False)
        raw_all.to_excel(writer, sheet_name="Raw_Data", index=False)


def main() -> int:
    parser = argparse.ArgumentParser(description="Append today's data and rebuild dashboard.")
    parser.add_argument("--namespace-name", required=True)
    parser.add_argument("--user-name", required=True)
    parser.add_argument("--password", required=True)
    parser.add_argument("--target", default=DEFAULT_TARGET)
    parser.add_argument("--browser-id", default=DEFAULT_BROWSER_ID)
    parser.add_argument("--workbook", required=True)
    parser.add_argument("--dashboard", required=True)
    parser.add_argument("--trading-calendar", default=str(BASE_DIR / "trading_calendar.json"))
    parser.add_argument("--date", help="YYYY-MM-DD; default=today")
    parser.add_argument("--show-by", type=int, default=0)
    args = parser.parse_args()

    day = datetime.strptime(args.date, "%Y-%m-%d").date() if args.date else date.today()
    calendar_path = Path(args.trading_calendar)
    workbook = Path(args.workbook)
    dashboard = Path(args.dashboard)
    if not workbook.exists():
        raise SystemExit(f"Workbook not found: {workbook}")
    if not is_trading_day(day, calendar_path):
        print(f"skip_non_trading_day {day}")
        return 0

    token = login(args.namespace_name, args.user_name, args.password, args.target, args.browser_id)
    company_map = fetch_company_map(token)
    new_df = build_new_rows(token, day, args.show_by, company_map)
    append_and_save(workbook, new_df)

    subprocess.run(
        [
            "python3",
            str(BASE_DIR / "build_core_dashboard_from_split.py"),
            "--input",
            str(workbook),
            "--output",
            str(dashboard),
        ],
        check=True,
    )
    print(f"updated {day} rows={len(new_df)} workbook={workbook} dashboard={dashboard}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

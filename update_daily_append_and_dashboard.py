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


def clean_fund_name(v: Any) -> str:
    if v is None:
        return ""
    s = str(v).strip()
    if s.endswith("-道乐数据"):
        s = s[: -len("-道乐数据")].rstrip()
    return s


def norm_return_pct(v: Any) -> float | None:
    """Normalize percent fields; some API rows occasionally come scaled by 10000."""
    x = pd.to_numeric(v, errors="coerce")
    if pd.isna(x):
        return None
    x = float(x)
    if abs(x) > 100:
        x = x / 10000.0
    return x


def fetch_one_combo_once(token: str, date_str: str, ranking_kind: str, fund_scope: str, show_by: int) -> list[dict[str, Any]]:
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


def combo_signature(rows: list[dict[str, Any]]) -> tuple[str, ...]:
    sig: list[str] = []
    for r in rows:
        sig.append(f"{norm_code(r.get('fundCode'))}|{clean_fund_name(r.get('fundName'))}")
    return tuple(sig)


def fetch_one_combo(token: str, date_str: str, ranking_kind: str, fund_scope: str, show_by: int) -> list[dict[str, Any]]:
    """Fetch with stability guard: retry until two consecutive identical snapshots."""
    best_rows: list[dict[str, Any]] | None = None
    prev_sig: tuple[str, ...] | None = None
    for attempt in range(1, 4):
        rows = fetch_one_combo_once(token, date_str, ranking_kind, fund_scope, show_by)
        sig = combo_signature(rows)
        if prev_sig is not None and sig == prev_sig:
            return rows
        prev_sig = sig
        best_rows = rows
        if attempt < 3:
            time.sleep(1)
    # Source may fluctuate; keep last snapshot but ensure deterministic order usage downstream.
    return best_rows or []


def build_new_rows(token: str, day: date, show_by: int, company_map: dict[str, str]) -> pd.DataFrame:
    date_str = day.strftime("%Y-%m-%d")
    out: list[dict[str, Any]] = []
    for k in ["add", "sub"]:
        for s in ["all", "stock", "bond", "qd"]:
            rows = fetch_one_combo(token, date_str, k, s, show_by)
            for idx, r in enumerate(rows, start=1):
                cid = str(r.get("companyId") or "").strip()
                out.append(
                    {
                        "统计日期": date_str,
                        "榜单类型": RANKING_KIND_LABEL[k],
                        "基金范围": FUND_SCOPE_LABEL[s],
                        # API field "ranking" can contain abnormal values on some days.
                        # Use returned list order to keep consistency with frontend display.
                        "榜单名次": idx,
                        "基金简称": clean_fund_name(r.get("fundName")),
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


def recompute_derived_days(df: pd.DataFrame) -> pd.DataFrame:
    """Recompute near-7 and consecutive days from local historical rows."""
    if df.empty:
        return df
    out = df.copy()
    out["统计日期"] = pd.to_datetime(out["统计日期"], errors="coerce")
    out = out.sort_values(["榜单类型", "基金范围", "统计日期", "榜单名次"], ascending=[True, True, True, True])

    on7 = pd.Series(index=out.index, dtype="Int64")
    consec = pd.Series(index=out.index, dtype="Int64")

    for (rank_type, scope), scope_df in out.groupby(["榜单类型", "基金范围"], sort=False):
        scope_dates = sorted(scope_df["统计日期"].dropna().unique().tolist())
        date_pos = {d: i for i, d in enumerate(scope_dates)}
        for _, g in scope_df.groupby("基金代码", sort=False):
            g = g.sort_values("统计日期")
            seen_dates = g["统计日期"].tolist()
            seen_set = set(seen_dates)

            prev_pos = None
            run = 0
            for i, (idx, row) in enumerate(g.iterrows()):
                d = row["统计日期"]
                p = date_pos.get(d)
                if p is None:
                    continue
                if prev_pos is not None and p == prev_pos + 1:
                    run += 1
                else:
                    run = 1
                prev_pos = p

                left = max(0, p - 6)
                window_dates = scope_dates[left : p + 1]
                cnt7 = sum(1 for wd in window_dates if wd in seen_set)
                on7.loc[idx] = cnt7
                consec.loc[idx] = run

    out["近7日上榜天数"] = on7.fillna(0).astype(int)
    out["连续上榜天数"] = consec.fillna(0).astype(int)
    out["统计日期"] = out["统计日期"].dt.strftime("%Y-%m-%d")
    return out


def reindex_rank(df: pd.DataFrame, has_board_type: bool) -> pd.DataFrame:
    out = df.copy()
    out["统计日期"] = pd.to_datetime(out["统计日期"], errors="coerce")
    out["榜单名次"] = pd.to_numeric(out["榜单名次"], errors="coerce")
    out["__rank_sort__"] = out["榜单名次"].fillna(10**9)
    group_cols = ["统计日期", "基金范围"]
    if has_board_type:
        group_cols = ["统计日期", "榜单类型", "基金范围"]
    out = out.sort_values(group_cols + ["__rank_sort__", "基金代码"], ascending=True)
    out["榜单名次"] = out.groupby(group_cols, sort=False).cumcount() + 1
    out = out.drop(columns=["__rank_sort__"])
    out["统计日期"] = out["统计日期"].dt.strftime("%Y-%m-%d")
    return out


def append_and_save(workbook: Path, new_df: pd.DataFrame) -> None:
    add_df = pd.read_excel(workbook, sheet_name="加仓榜")
    sub_df = pd.read_excel(workbook, sheet_name="减仓榜")
    raw_df = pd.read_excel(workbook, sheet_name="Raw_Data")

    required_cols = {"统计日期", "榜单类型", "基金范围", "基金代码"}
    if new_df is None or new_df.empty:
        print("warn_no_new_rows_skip_append")
        return
    if not required_cols.issubset(set(new_df.columns)):
        missing = sorted(required_cols.difference(set(new_df.columns)))
        print(f"warn_missing_required_columns_skip_append missing={','.join(missing)}")
        return

    for df in (add_df, sub_df, raw_df, new_df):
        df["基金代码"] = df["基金代码"].map(norm_code)
        df["统计日期"] = pd.to_datetime(df["统计日期"], errors="coerce").dt.strftime("%Y-%m-%d")
        for c in ["日涨跌幅(%)", "近1月涨跌幅(%)", "近1年涨跌幅(%)"]:
            if c in df.columns:
                df[c] = df[c].map(norm_return_pct)

    add_new = new_df[new_df["榜单类型"] == "加仓榜"]
    sub_new = new_df[new_df["榜单类型"] == "减仓榜"]
    new_dates = set(new_df["统计日期"].dropna().astype(str).tolist())

    # Day-level replace: remove same day rows first, then append fresh crawl rows.
    if new_dates:
        add_df = add_df[~add_df["统计日期"].astype(str).isin(new_dates)]
        sub_df = sub_df[~sub_df["统计日期"].astype(str).isin(new_dates)]
        raw_df = raw_df[~raw_df["统计日期"].astype(str).isin(new_dates)]

    # Use stable business keys to avoid duplicate rows when display names vary by source text.
    key_cols = ["统计日期", "榜单类型", "基金范围", "基金代码"]
    add_all = pd.concat([add_df, add_new], ignore_index=True).drop_duplicates(subset=key_cols, keep="last")
    sub_all = pd.concat([sub_df, sub_new], ignore_index=True).drop_duplicates(subset=key_cols, keep="last")
    raw_all = pd.concat([raw_df, new_df], ignore_index=True).drop_duplicates(subset=key_cols, keep="last")

    # Recompute derived day-count fields to avoid upstream API anomalies.
    raw_all = recompute_derived_days(raw_all)
    add_all = recompute_derived_days(add_all)
    sub_all = recompute_derived_days(sub_all)
    raw_all = reindex_rank(raw_all, has_board_type=True)
    add_all = reindex_rank(add_all, has_board_type=False)
    sub_all = reindex_rank(sub_all, has_board_type=False)

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

#!/usr/bin/env python3
"""End-to-end sanity checks for cdollar workbook and dashboards."""

from __future__ import annotations

import argparse
import json
import ssl
import sys
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any

import pandas as pd

LOGIN_URL = "https://asset.cdollar.cn/yuqing-common/mid/login"
ADD_SUB_URL = "https://asset.cdollar.cn/leshu-dashboard-new/shelf/zfb/getZfbAddSubRankingData"


def post_json(url: str, payload: dict[str, Any], headers: dict[str, str]) -> dict[str, Any]:
    body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
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
        raise RuntimeError(f"Network error for {url}: {exc}") from exc


def check(condition: bool, ok: str, fail: str, issues: list[str]) -> None:
    if condition:
        print(f"[OK] {ok}")
    else:
        print(f"[FAIL] {fail}")
        issues.append(fail)


def validate_workbook(workbook: Path) -> tuple[pd.DataFrame, list[str]]:
    issues: list[str] = []
    required_sheets = {"Raw_Data", "加仓榜", "减仓榜"}
    xls = pd.ExcelFile(workbook)
    check(required_sheets.issubset(set(xls.sheet_names)), "工作簿包含3个核心sheet", "工作簿缺少核心sheet", issues)

    raw = pd.read_excel(workbook, sheet_name="Raw_Data")
    raw["统计日期"] = pd.to_datetime(raw["统计日期"], errors="coerce")
    raw["基金代码"] = raw["基金代码"].astype(str).str.replace(r"\.0$", "", regex=True).str.zfill(6)
    check(raw["统计日期"].notna().all(), "统计日期可解析", "存在无法解析的统计日期", issues)
    check((raw["基金代码"].str.len() == 6).all(), "基金代码均为6位", "存在非6位基金代码", issues)

    for c in ["日涨跌幅(%)", "近1月涨跌幅(%)", "近1年涨跌幅(%)"]:
        s = pd.to_numeric(raw[c], errors="coerce")
        check((s.isna() | (s.abs() <= 100)).all(), f"{c}范围正常", f"{c}存在绝对值>100的异常", issues)

    # Cross-scope consistency check:
    # same date + board + fund code should have close returns between 全部基金 and non-all scopes.
    drift_bad = 0
    for _, g in raw.groupby(["统计日期", "榜单类型", "基金代码"], dropna=False):
        all_rows = g[g["基金范围"] == "全部基金"]
        non_all = g[g["基金范围"] != "全部基金"]
        if all_rows.empty or non_all.empty:
            continue
        for col, tol_base, tol_scale in [
            ("日涨跌幅(%)", 1.5, 6.0),
            ("近1月涨跌幅(%)", 5.0, 4.0),
            ("近1年涨跌幅(%)", 12.0, 3.0),
        ]:
            base_vals = pd.to_numeric(non_all[col], errors="coerce").dropna()
            if base_vals.empty:
                continue
            base = float(base_vals.median())
            tol = max(tol_base, tol_scale * abs(base) + (0.8 if col == "日涨跌幅(%)" else 3.0))
            for v in pd.to_numeric(all_rows[col], errors="coerce").dropna().tolist():
                if abs(float(v) - base) > tol:
                    drift_bad += 1
    check(drift_bad == 0, "跨基金范围收益口径一致", f"发现 {drift_bad} 条跨基金范围收益漂移", issues)

    s7 = pd.to_numeric(raw["近7日上榜天数"], errors="coerce")
    sc = pd.to_numeric(raw["连续上榜天数"], errors="coerce")
    check(((s7 >= 0) & (s7 <= 7)).fillna(False).all(), "近7日上榜天数在0-7", "近7日上榜天数存在异常", issues)
    check(((sc >= 0) & (sc <= 260)).fillna(False).all(), "连续上榜天数在合理范围", "连续上榜天数存在异常", issues)

    # Rank should be contiguous within each date/type/scope.
    bad_groups = 0
    for _, g in raw.groupby(["统计日期", "榜单类型", "基金范围"]):
        r = pd.to_numeric(g["榜单名次"], errors="coerce").dropna().astype(int).sort_values().tolist()
        if not r:
            continue
        expect = list(range(1, len(r) + 1))
        if r != expect:
            bad_groups += 1
    check(bad_groups == 0, "各分组榜单名次连续", f"有 {bad_groups} 个分组榜单名次不连续", issues)

    latest = raw["统计日期"].max()
    latest_rows = raw[raw["统计日期"] == latest]
    check(len(latest_rows) > 0, f"存在最新日期数据 {latest.date()}", "缺少最新日期数据", issues)
    print(f"[INFO] 最新日期={latest.date()} 行数={len(latest_rows)}")

    hit_latest = latest_rows["基金简称"].astype(str).str.contains("宝盈", na=False).sum()
    print(f"[INFO] 最新日期命中“宝盈”基金名称={hit_latest}")
    return raw, issues


def validate_live_api(
    raw: pd.DataFrame,
    namespace_name: str,
    user_name: str,
    password: str,
    target: str,
    browser_id: str,
    date_str: str,
) -> list[str]:
    issues: list[str] = []
    login_payload = {
        "namespaceName": namespace_name,
        "userName": user_name,
        "password": password,
        "remember": True,
        "target": target,
        "browserId": browser_id,
    }
    login_resp = post_json(LOGIN_URL, login_payload, {})
    token = login_resp.get("body", {}).get("token")
    if not token:
        issues.append("登录失败，无法做实时对照")
        print("[FAIL] 登录失败，无法做实时对照")
        return issues

    raw_date = raw.copy()
    raw_date["统计日期"] = raw_date["统计日期"].dt.strftime("%Y-%m-%d")
    raw_date = raw_date[raw_date["统计日期"] == date_str]

    mapping = [("加仓榜-全部基金", "加仓榜"), ("减仓榜-全部基金", "减仓榜")]
    for api_type, local_type in mapping:
        payload = {"date": date_str, "showBy": 0, "type": api_type, "pageNo": 1, "pageSize": 30}
        resp = post_json(ADD_SUB_URL, payload, {"token": token})
        lst = resp.get("data", {}).get("list", [])
        api_codes = [str(x.get("fundCode", "")).replace(".0", "").zfill(6) for x in lst[:10]]
        loc = raw_date[(raw_date["榜单类型"] == local_type) & (raw_date["基金范围"] == "全部基金")].copy()
        loc = loc.sort_values("榜单名次")
        local_codes = loc["基金代码"].astype(str).head(10).tolist()
        ok = api_codes == local_codes
        check(ok, f"{local_type} Top10代码与实时接口一致", f"{local_type} Top10代码与实时接口不一致", issues)
    return issues


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--workbook", required=True)
    p.add_argument("--namespace-name")
    p.add_argument("--user-name")
    p.add_argument("--password")
    p.add_argument("--target", default="https://www.cdollar.cn/leshu-pro/#/e/vq6aKqp5YU")
    p.add_argument("--browser-id", default="09cb220223cb45410e11e84679b83fb6")
    p.add_argument("--live-date")
    args = p.parse_args()

    workbook = Path(args.workbook)
    raw, issues = validate_workbook(workbook)

    if args.live_date and args.namespace_name and args.user_name and args.password:
        issues.extend(
            validate_live_api(
                raw,
                args.namespace_name,
                args.user_name,
                args.password,
                args.target,
                args.browser_id,
                args.live_date,
            )
        )

    if issues:
        print(f"\n[SUMMARY] FAIL count={len(issues)}")
        for i in issues:
            print(f" - {i}")
        return 1
    print("\n[SUMMARY] PASS all checks")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

#!/usr/bin/env python3
"""Strict end-to-end QA for crawl -> workbook -> html dashboards."""

from __future__ import annotations

import argparse
import json
import re
import ssl
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any

import pandas as pd

LOGIN_URL = "https://asset.cdollar.cn/yuqing-common/mid/login"
ADD_SUB_URL = "https://asset.cdollar.cn/leshu-dashboard-new/shelf/zfb/getZfbAddSubRankingData"

API_TYPE = {
    ("加仓榜", "全部基金"): "加仓榜-全部基金",
    ("加仓榜", "偏股基金"): "加仓榜-偏股基金",
    ("加仓榜", "偏债基金"): "加仓榜-偏债基金",
    ("加仓榜", "QD基金"): "加仓榜-QD基金",
    ("减仓榜", "全部基金"): "减仓榜-全部基金",
    ("减仓榜", "偏股基金"): "减仓榜-偏股基金",
    ("减仓榜", "偏债基金"): "减仓榜-偏债基金",
    ("减仓榜", "QD基金"): "减仓榜-QD基金",
}


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


def ok(cond: bool, msg_ok: str, msg_fail: str, fails: list[str]) -> None:
    if cond:
        print(f"[OK] {msg_ok}")
    else:
        print(f"[FAIL] {msg_fail}")
        fails.append(msg_fail)


def norm_code(v: Any) -> str:
    s = str(v or "").strip()
    if s.endswith(".0"):
        s = s[:-2]
    return s.zfill(6) if s.isdigit() else s


def parse_html_latest(path: Path) -> str | None:
    txt = path.read_text(encoding="utf-8", errors="ignore")
    m = re.search(r'"latest_date"\s*:\s*"(\d{4}-\d{2}-\d{2})"', txt)
    if m:
        return m.group(1)
    return None


def ensure_html_non_empty(path: Path, fails: list[str]) -> None:
    txt = path.read_text(encoding="utf-8", errors="ignore")
    m = re.search(r"const\s+DATA\s*=\s*(\[[\s\S]*?\]);", txt)
    if not m:
        ok(False, "", f"{path.name} 缺少 DATA 数据块", fails)
        return
    try:
        arr = json.loads(m.group(1))
    except Exception:
        ok(False, "", f"{path.name} DATA 数据块解析失败", fails)
        return
    ok(len(arr) > 0, f"{path.name} 含有效数据块 rows={len(arr)}", f"{path.name} DATA 行数为0", fails)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--workbook", required=True)
    ap.add_argument("--core-html", required=True)
    ap.add_argument("--ops-html", required=True)
    ap.add_argument("--weak-html", required=True)
    ap.add_argument("--namespace-name")
    ap.add_argument("--user-name")
    ap.add_argument("--password")
    ap.add_argument("--target", default="https://www.cdollar.cn/leshu-pro/#/e/vq6aKqp5YU")
    ap.add_argument("--browser-id", default="09cb220223cb45410e11e84679b83fb6")
    args = ap.parse_args()

    fails: list[str] = []
    warns: list[str] = []
    wb = Path(args.workbook)
    raw = pd.read_excel(wb, sheet_name="Raw_Data")
    raw["统计日期"] = pd.to_datetime(raw["统计日期"], errors="coerce")
    raw["基金代码"] = raw["基金代码"].map(norm_code)
    latest = raw["统计日期"].max().strftime("%Y-%m-%d")
    print(f"[INFO] workbook_latest={latest}")

    # local structural checks
    for (board, scope), _ in API_TYPE.items():
        g = raw[(raw["统计日期"].dt.strftime("%Y-%m-%d") == latest) & (raw["榜单类型"] == board) & (raw["基金范围"] == scope)].copy()
        ok(len(g) == 30, f"{board}-{scope} 当天30条", f"{board}-{scope} 当天条数={len(g)} 非30", fails)
        codes = g["基金代码"].astype(str).tolist()
        ok(len(codes) == len(set(codes)), f"{board}-{scope} 代码无重复", f"{board}-{scope} 代码重复", fails)
        ranks = pd.to_numeric(g["榜单名次"], errors="coerce").dropna().astype(int).sort_values().tolist()
        ok(ranks == list(range(1, len(ranks) + 1)), f"{board}-{scope} 名次连续", f"{board}-{scope} 名次不连续", fails)

    # html sanity
    core_html = Path(args.core_html)
    ops_html = Path(args.ops_html)
    weak_html = Path(args.weak_html)
    for p in [core_html, ops_html, weak_html]:
        ok(p.exists(), f"{p.name} 文件存在", f"{p.name} 文件不存在", fails)
        if p.exists():
            ensure_html_non_empty(p, fails)
            latest_in_html = parse_html_latest(p)
            if latest_in_html:
                ok(latest_in_html == latest, f"{p.name} 最新日期与工作簿一致", f"{p.name} 最新日期={latest_in_html} 与工作簿={latest} 不一致", fails)

    # live api cross-check for 8 combos top30 code order
    if args.namespace_name and args.user_name and args.password:
        login_payload = {
            "namespaceName": args.namespace_name,
            "userName": args.user_name,
            "password": args.password,
            "remember": True,
            "target": args.target,
            "browserId": args.browser_id,
        }
        tok = post_json(LOGIN_URL, login_payload, {}).get("body", {}).get("token")
        ok(bool(tok), "实时登录成功", "实时登录失败", fails)
        if tok:
            for (board, scope), api_type in API_TYPE.items():
                api_samples: list[list[str]] = []
                for _ in range(3):
                    payload = {"date": latest, "showBy": 0, "type": api_type, "pageNo": 1, "pageSize": 30}
                    resp = post_json(ADD_SUB_URL, payload, {"token": tok})
                    arr = resp.get("data", {}).get("list", [])
                    api_samples.append([norm_code(x.get("fundCode")) for x in arr[:30]])
                loc = raw[
                    (raw["统计日期"].dt.strftime("%Y-%m-%d") == latest)
                    & (raw["榜单类型"] == board)
                    & (raw["基金范围"] == scope)
                ].copy()
                loc = loc.sort_values("榜单名次")
                local_codes = loc["基金代码"].astype(str).head(30).tolist()

                sample_set = {tuple(x) for x in api_samples}
                stable = len(sample_set) == 1
                if not stable:
                    w = f"实时接口抖动 {board}-{scope}：连续3次返回不完全一致"
                    warns.append(w)
                    print(f"[WARN] {w}")

                matched_any = tuple(local_codes) in sample_set
                ok(matched_any, f"实时对照 {board}-{scope} Top30一致(样本内)", f"实时对照 {board}-{scope} Top30不一致(样本外)", fails)

    if fails:
        print(f"\n[SUMMARY] FAIL count={len(fails)} warn={len(warns)}")
        for i in fails:
            print(" -", i)
        if warns:
            print("[WARNINGS]")
            for w in warns:
                print(" -", w)
        return 1

    print(f"\n[SUMMARY] PASS strict end-to-end warn={len(warns)}")
    if warns:
        print("[WARNINGS]")
        for w in warns:
            print(" -", w)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

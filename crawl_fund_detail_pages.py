#!/usr/bin/env python3
"""Crawl cdollar fund detail page data for funds in an existing workbook."""

from __future__ import annotations

import argparse
import concurrent.futures
import json
import ssl
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd


LOGIN_URL = "https://asset.cdollar.cn/yuqing-common/mid/login"
BASE_URL = "https://asset.cdollar.cn/leshu-dashboard-new"
DEFAULT_TARGET = "https://www.cdollar.cn/leshu-pro/#/e/vq6aKqp5YU"
DEFAULT_BROWSER_ID = "09cb220223cb45410e11e84679b83fb6"

FUND_DETAIL_URL = f"{BASE_URL}/channel/product/fundDetail"
USER_FOLLOW_URL = f"{BASE_URL}/channel/product/userFollowData"
RELATED_ARTICLE_URL = f"{BASE_URL}/channel/product/relatedArticle"
FUND_ANALYZE_URL = f"{BASE_URL}/channel/product/zfb/fundAnalyze"


def post_json(url: str, payload: dict[str, Any], headers: dict[str, str], retries: int = 3) -> dict[str, Any]:
    body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    last_error: Exception | None = None
    for attempt in range(1, retries + 1):
        req = urllib.request.Request(url, data=body, method="POST")
        req.add_header("Content-Type", "application/json")
        for key, value in headers.items():
            req.add_header(key, value)
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


def get_json(url: str, params: dict[str, Any], headers: dict[str, str], retries: int = 3) -> dict[str, Any]:
    query = urllib.parse.urlencode({k: v for k, v in params.items() if v is not None})
    full_url = f"{url}?{query}" if query else url
    last_error: Exception | None = None
    for attempt in range(1, retries + 1):
        req = urllib.request.Request(full_url, method="GET")
        for key, value in headers.items():
            req.add_header(key, value)
        try:
            with urllib.request.urlopen(req, timeout=30) as resp:
                return json.loads(resp.read().decode("utf-8"))
        except urllib.error.HTTPError as exc:
            detail = exc.read().decode("utf-8", errors="replace")
            raise RuntimeError(f"HTTP {exc.code} for {full_url}: {detail}") from exc
        except (urllib.error.URLError, ssl.SSLError, TimeoutError, ConnectionError) as exc:
            last_error = exc
            if attempt < retries:
                time.sleep(attempt)
                continue
            break
    raise RuntimeError(f"Network error for {full_url}: {last_error}")


def login(namespace_name: str, user_name: str, password: str, target: str, browser_id: str) -> str:
    payload = {
        "namespaceName": namespace_name,
        "userName": user_name,
        "password": password,
        "remember": True,
        "target": target,
        "browserId": browser_id,
    }
    resp = post_json(LOGIN_URL, payload, {})
    if resp.get("errCode") != "e0000":
        raise RuntimeError(f"Login failed: {resp}")
    token = resp.get("body", {}).get("token")
    if not token:
        raise RuntimeError("Login token missing")
    return token


def normalize_code(value: Any) -> str:
    if value is None:
        return ""
    s = str(value).strip()
    if s.endswith(".0"):
        s = s[:-2]
    return s.zfill(6) if s.isdigit() else s


def resolve_snapshot_date(raw_df: pd.DataFrame, given_date: str | None) -> str:
    if given_date:
        return datetime.strptime(given_date, "%Y-%m-%d").strftime("%Y-%m-%d")
    max_date = pd.to_datetime(raw_df["统计日期"], errors="coerce").max()
    if pd.isna(max_date):
        raise ValueError("Cannot infer snapshot date from workbook")
    return pd.Timestamp(max_date).strftime("%Y-%m-%d")


def load_funds_from_workbook(
    workbook: Path,
    snapshot_date: str,
    max_funds: int | None,
    fund_pool: str,
) -> pd.DataFrame:
    raw_df = pd.read_excel(workbook, sheet_name="Raw_Data")
    if "基金代码" not in raw_df.columns:
        raise ValueError("Raw_Data sheet missing 基金代码")
    raw_df["基金代码"] = raw_df["基金代码"].map(normalize_code)
    raw_df["统计日期"] = pd.to_datetime(raw_df["统计日期"], errors="coerce")

    if fund_pool == "snapshot":
        day_df = raw_df[raw_df["统计日期"].dt.strftime("%Y-%m-%d") == snapshot_date].copy()
        if day_df.empty:
            raise ValueError(f"No data on snapshot date {snapshot_date}")
        day_df = day_df.sort_values(["榜单类型", "基金范围", "榜单名次"], ascending=[True, True, True])
        keep_cols = [c for c in ["基金代码", "基金简称", "榜单类型", "基金范围", "榜单名次"] if c in day_df.columns]
        uniq = day_df[keep_cols].drop_duplicates(subset=["基金代码"], keep="first").reset_index(drop=True)
        uniq["抓取日期"] = snapshot_date
    else:
        # all_history: use each fund's latest appearance date in Raw_Data.
        # This increases coverage versus single-day snapshot mode.
        hist = raw_df.copy()
        hist = hist.sort_values(["基金代码", "统计日期", "榜单类型", "基金范围", "榜单名次"], ascending=[True, False, True, True, True])
        keep_cols = [c for c in ["基金代码", "基金简称", "榜单类型", "基金范围", "榜单名次", "统计日期"] if c in hist.columns]
        uniq = hist[keep_cols].drop_duplicates(subset=["基金代码"], keep="first").reset_index(drop=True)
        uniq["抓取日期"] = pd.to_datetime(uniq["统计日期"], errors="coerce").dt.strftime("%Y-%m-%d")
        uniq = uniq.drop(columns=[c for c in ["统计日期"] if c in uniq.columns])
        uniq = uniq.sort_values(["抓取日期", "榜单类型", "基金范围", "榜单名次"], ascending=[False, True, True, True]).reset_index(drop=True)

    if max_funds is not None:
        uniq = uniq.head(max_funds)
    return uniq


def crawl_related_articles(
    token: str, fund_code: str, page_size: int, max_pages: int
) -> list[dict[str, Any]]:
    headers = {"token": token}
    page_no = 1
    rows: list[dict[str, Any]] = []
    while page_no <= max_pages:
        payload = {"fundCode": fund_code, "pageNo": page_no, "pageSize": page_size}
        resp = post_json(RELATED_ARTICLE_URL, payload, headers)
        if resp.get("code") != "0000":
            break
        data = resp.get("data") or {}
        page_list = data.get("list") or []
        for item in page_list:
            out = dict(item)
            out["fundCode"] = fund_code
            out["pageNo"] = page_no
            rows.append(out)
        if not data.get("hasNextPage"):
            break
        page_no += 1
    return rows


def crawl_one_fund(
    token: str,
    fund_code: str,
    snapshot_date: str,
    channel: str,
    page_size: int,
    max_pages: int,
) -> tuple[dict[str, Any], list[dict[str, Any]], list[dict[str, Any]], dict[str, Any]]:
    headers = {"token": token}
    detail_resp = post_json(FUND_DETAIL_URL, {"fundCode": fund_code}, headers)
    follow_resp = get_json(
        USER_FOLLOW_URL,
        {"fundCode": fund_code, "date": snapshot_date, "channel": channel},
        headers,
    )
    analyze_resp = post_json(FUND_ANALYZE_URL, {"fundCode": fund_code}, headers)
    articles = crawl_related_articles(token, fund_code, page_size=page_size, max_pages=max_pages)

    detail_data = detail_resp.get("data") or {}
    follow_data = follow_resp.get("data") or {}
    analyze_data = analyze_resp.get("data") or {}
    fund_info = detail_data.get("fundInfoVO") or {}

    summary = {
        "snapshotDate": snapshot_date,
        "fundCode": fund_code,
        "fundName": detail_data.get("fundName") or follow_data.get("fundName"),
        "fundTypeName": detail_data.get("fundTypeName") or follow_data.get("fundTypeName"),
        "productId": detail_data.get("productId") or follow_data.get("productId"),
        "riskLevel": follow_data.get("riskLevel"),
        "fundTypeFirst": follow_data.get("fundTypeFirst") or fund_info.get("fundTypeFirst"),
        "fundTypeSecond": follow_data.get("fundTypeSecond") or fund_info.get("fundTypeSecond"),
        "investment": fund_info.get("investment"),
        "investmentDesc": fund_info.get("investmentDesc"),
        "assetSize": follow_data.get("assetSize") if follow_data else detail_data.get("assetSize"),
        "weekVisit": follow_data.get("weekVisit"),
        "monthVisit": follow_data.get("monthVisit"),
        "weekSearch": follow_data.get("weekSearch"),
        "monthSearch": follow_data.get("monthSearch"),
        "optional": follow_data.get("optional"),
        "hold": follow_data.get("hold"),
        "searchToExposureRatio": follow_data.get("searchToExposureRatio"),
        "exposureToFollowRatio": follow_data.get("exposureToFollowRatio"),
        "exposureToConversionRatio": follow_data.get("exposureToConversionRatio"),
        "followToConversionRatio": follow_data.get("followToConversionRatio"),
        "netExposureToFollowRatio": follow_data.get("netExposureToFollowRatio"),
        "dayInc": follow_data.get("dayInc"),
        "maxDrawdown1y": follow_data.get("maxDrawdown1y"),
        "hasFundExpertNum7d": follow_data.get("hasFundExpertNum7d"),
        "articleMentionNum7d": follow_data.get("articleMentionNum7d"),
        "totalExpertFundMoney7d": follow_data.get("totalExpertFundMoney7d"),
        "relatedArticleCount": len(articles),
        "periodText": analyze_data.get("periodText"),
        "showTrackName": analyze_data.get("showTrackName"),
        "showTrackYield": analyze_data.get("showTrackYield"),
        "trackingErrorTitle": analyze_data.get("trackingErrorTitle"),
        "trackingErrorSubTitle": analyze_data.get("trackingErrorSubTitle"),
        "trackingErrorValue": analyze_data.get("trackingErrorValue"),
        "trackingErrorLowThreshold": analyze_data.get("trackingErrorLowThreshold"),
        "trackingErrorHighThreshold": analyze_data.get("trackingErrorHighThreshold"),
        "fundAnalyzeIndicatorCount": len(analyze_data.get("zfbIndicatorList") or []),
        "crawlTime": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }

    indicators: list[dict[str, Any]] = []
    for idx, item in enumerate(analyze_data.get("zfbIndicatorList") or [], start=1):
        indicators.append(
            {
                "snapshotDate": snapshot_date,
                "fundCode": fund_code,
                "fundName": summary.get("fundName"),
                "indicatorOrder": idx,
                "indicatorTitle": item.get("title"),
                "indicatorSubTitle": item.get("subTitle"),
                "indicatorLevel": item.get("level"),
                "hasThumbUp": item.get("hasThumbUp"),
            }
        )

    raw_responses = {
        "fundDetail": detail_resp,
        "userFollowData": follow_resp,
        "fundAnalyze": analyze_resp,
    }
    return summary, articles, indicators, raw_responses


def main() -> int:
    parser = argparse.ArgumentParser(description="Crawl fund detail-page data for all funds in workbook.")
    parser.add_argument("--namespace-name", required=True)
    parser.add_argument("--user-name", required=True)
    parser.add_argument("--password", required=True)
    parser.add_argument("--workbook", required=True, help="Input workbook path (must contain Raw_Data)")
    parser.add_argument("--output", required=True, help="Output xlsx path")
    parser.add_argument("--raw-json-output", help="Optional raw JSON output path")
    parser.add_argument("--date", help="Snapshot date YYYY-MM-DD; default=max(统计日期) in Raw_Data")
    parser.add_argument("--target", default=DEFAULT_TARGET)
    parser.add_argument("--browser-id", default=DEFAULT_BROWSER_ID)
    parser.add_argument("--channel", default="ALIPAY")
    parser.add_argument("--article-page-size", type=int, default=20)
    parser.add_argument("--article-max-pages", type=int, default=5)
    parser.add_argument("--max-funds", type=int)
    parser.add_argument("--workers", type=int, default=10)
    parser.add_argument(
        "--fund-pool",
        choices=["snapshot", "all_history"],
        default="snapshot",
        help="snapshot=仅抓某一天上榜基金; all_history=抓Raw_Data全历史基金池(按每只基金最近上榜日期取数)",
    )
    args = parser.parse_args()

    workbook = Path(args.workbook)
    if not workbook.exists():
        raise SystemExit(f"Workbook not found: {workbook}")

    raw_df = pd.read_excel(workbook, sheet_name="Raw_Data")
    snapshot_date = resolve_snapshot_date(raw_df, args.date)
    funds_df = load_funds_from_workbook(
        workbook,
        snapshot_date=snapshot_date,
        max_funds=args.max_funds,
        fund_pool=args.fund_pool,
    )

    token = login(args.namespace_name, args.user_name, args.password, args.target, args.browser_id)
    summaries: list[dict[str, Any]] = []
    articles: list[dict[str, Any]] = []
    indicators: list[dict[str, Any]] = []
    raw_json_rows: list[dict[str, Any]] = []

    total = len(funds_df)
    rows = [r for _, r in funds_df.iterrows()]
    done = 0

    def run_one(row: pd.Series) -> tuple[pd.Series, dict[str, Any], list[dict[str, Any]], list[dict[str, Any]], dict[str, Any]]:
        fund_code = normalize_code(row.get("基金代码"))
        if not fund_code:
            raise ValueError("empty fund code")
        crawl_date = str(row.get("抓取日期") or snapshot_date)
        summary, rel_articles, rel_indicators, raw_resp = crawl_one_fund(
            token=token,
            fund_code=fund_code,
            snapshot_date=crawl_date,
            channel=args.channel,
            page_size=args.article_page_size,
            max_pages=args.article_max_pages,
        )
        return row, summary, rel_articles, rel_indicators, raw_resp

    with concurrent.futures.ThreadPoolExecutor(max_workers=max(1, args.workers)) as pool:
        fut_map = {pool.submit(run_one, r): r for r in rows}
        for fut in concurrent.futures.as_completed(fut_map):
            src_row = fut_map[fut]
            fund_code = normalize_code(src_row.get("基金代码"))
            done += 1
            try:
                row, summary, rel_articles, rel_indicators, raw_resp = fut.result()
                summary["rankBoard"] = row.get("榜单类型")
                summary["fundScope"] = row.get("基金范围")
                summary["rankOnBoard"] = row.get("榜单名次")
                summaries.append(summary)
                articles.extend(rel_articles)
                indicators.extend(rel_indicators)
                raw_json_rows.append(
                    {
                        "snapshotDate": summary.get("snapshotDate"),
                        "fundCode": fund_code,
                        "fundName": summary.get("fundName"),
                        "responses": raw_resp,
                    }
                )
                print(f"[{done}/{total}] ok {fund_code} {summary.get('fundName') or ''}")
            except Exception as exc:
                print(f"[{done}/{total}] fail {fund_code} {exc}")

    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)

    summary_df = pd.DataFrame(summaries)
    article_df = pd.DataFrame(articles)
    indicator_df = pd.DataFrame(indicators)

    if not article_df.empty:
        fund_name_map = {}
        if not summary_df.empty and {"fundCode", "fundName"}.issubset(summary_df.columns):
            for _, sr in summary_df[["fundCode", "fundName"]].drop_duplicates(subset=["fundCode"]).iterrows():
                fund_name_map[normalize_code(sr.get("fundCode"))] = str(sr.get("fundName") or "")
        article_df["fundCode"] = article_df["fundCode"].map(normalize_code)
        article_df["fundName"] = article_df["fundCode"].map(lambda x: fund_name_map.get(x, ""))
        article_df["publishTime"] = pd.to_datetime(article_df["publishTime"], errors="coerce").dt.strftime("%Y-%m-%d %H:%M:%S")
        article_df = article_df.sort_values(["fundCode", "publishTime"], ascending=[True, False])
    if not summary_df.empty:
        summary_df = summary_df.sort_values(["rankBoard", "fundScope", "rankOnBoard"], ascending=[True, True, True])
    if not indicator_df.empty:
        indicator_df = indicator_df.sort_values(["fundCode", "indicatorOrder"])

    summary_col_map = {
        "snapshotDate": "统计日期",
        "fundCode": "基金代码",
        "fundName": "基金名称",
        "rankBoard": "榜单类型",
        "fundScope": "基金范围",
        "rankOnBoard": "榜单名次",
        "productId": "产品ID",
        "riskLevel": "风险等级",
        "fundTypeName": "基金类别",
        "fundTypeFirst": "基金一级类型",
        "fundTypeSecond": "基金二级类型",
        "investment": "投资方向标签",
        "investmentDesc": "投资方向说明",
        "assetSize": "基金规模(元)",
        "weekVisit": "本周浏览人数",
        "monthVisit": "本月浏览人数",
        "weekSearch": "本周搜索人数",
        "monthSearch": "本月搜索人数",
        "optional": "自选人数",
        "hold": "持有人数",
        "searchToExposureRatio": "搜曝比",
        "exposureToFollowRatio": "曝关比",
        "exposureToConversionRatio": "曝转比",
        "followToConversionRatio": "关转比",
        "netExposureToFollowRatio": "净曝关比",
        "dayInc": "日涨跌幅(%)",
        "maxDrawdown1y": "近1年最大回撤(%)",
        "hasFundExpertNum7d": "近7天提及专家数",
        "articleMentionNum7d": "近7天提及文章数",
        "totalExpertFundMoney7d": "近7天提及资金量",
        "relatedArticleCount": "动态笔记条数",
        "periodText": "基金分析观察周期",
        "showTrackName": "跟踪指数名称",
        "showTrackYield": "跟踪指数收益率(%)",
        "trackingErrorTitle": "跟踪评价标题",
        "trackingErrorSubTitle": "跟踪评价说明",
        "trackingErrorValue": "跟踪误差(%)",
        "trackingErrorLowThreshold": "第一梯队阈值(%)",
        "trackingErrorHighThreshold": "第二梯队阈值(%)",
        "fundAnalyzeIndicatorCount": "基金分析标签数",
        "crawlTime": "抓取时间",
    }
    article_col_map = {
        "fundCode": "基金代码",
        "fundName": "基金名称",
        "id": "帖子ID",
        "nick": "发帖人",
        "title": "帖子标题",
        "publishTime": "发布时间",
        "readCount": "浏览量",
        "praiseNumber": "点赞数",
        "commentNumber": "评论数",
        "interaction": "有效互动率(%)",
        "pageNo": "抓取页码",
    }
    indicator_col_map = {
        "snapshotDate": "统计日期",
        "fundCode": "基金代码",
        "fundName": "基金名称",
        "indicatorOrder": "指标序号",
        "indicatorTitle": "分析标签标题",
        "indicatorSubTitle": "分析标签说明",
        "indicatorLevel": "评级等级",
        "hasThumbUp": "点赞标记",
    }

    if not summary_df.empty:
        summary_df = summary_df.rename(columns=summary_col_map)
        summary_order = [
            "统计日期",
            "榜单类型",
            "基金范围",
            "榜单名次",
            "基金名称",
            "基金代码",
            "基金类别",
            "基金一级类型",
            "基金二级类型",
            "风险等级",
            "投资方向标签",
            "投资方向说明",
            "基金规模(元)",
            "本周浏览人数",
            "本月浏览人数",
            "本周搜索人数",
            "本月搜索人数",
            "自选人数",
            "持有人数",
            "搜曝比",
            "曝关比",
            "曝转比",
            "关转比",
            "净曝关比",
            "日涨跌幅(%)",
            "近1年最大回撤(%)",
            "近7天提及专家数",
            "近7天提及文章数",
            "近7天提及资金量",
            "动态笔记条数",
            "基金分析观察周期",
            "跟踪指数名称",
            "跟踪指数收益率(%)",
            "跟踪评价标题",
            "跟踪评价说明",
            "跟踪误差(%)",
            "第一梯队阈值(%)",
            "第二梯队阈值(%)",
            "基金分析标签数",
            "产品ID",
            "抓取时间",
        ]
        summary_df = summary_df[[c for c in summary_order if c in summary_df.columns]]
    if not article_df.empty:
        article_df = article_df.rename(columns=article_col_map)
        article_order = [
            "基金代码",
            "基金名称",
            "帖子ID",
            "发帖人",
            "帖子标题",
            "发布时间",
            "浏览量",
            "点赞数",
            "评论数",
            "有效互动率(%)",
            "抓取页码",
        ]
        article_df = article_df[[c for c in article_order if c in article_df.columns]]
    if not indicator_df.empty:
        indicator_df = indicator_df.rename(columns=indicator_col_map)
        indicator_order = [
            "统计日期",
            "基金代码",
            "基金名称",
            "指标序号",
            "分析标签标题",
            "分析标签说明",
            "评级等级",
            "点赞标记",
        ]
        indicator_df = indicator_df[[c for c in indicator_order if c in indicator_df.columns]]

    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        summary_df.to_excel(writer, sheet_name="详情核心指标", index=False)
        article_df.to_excel(writer, sheet_name="动态和笔记", index=False)
        indicator_df.to_excel(writer, sheet_name="基金分析标签", index=False)

    if args.raw_json_output:
        raw_json_path = Path(args.raw_json_output)
        raw_json_path.parent.mkdir(parents=True, exist_ok=True)
        raw_json_path.write_text(json.dumps(raw_json_rows, ensure_ascii=False), encoding="utf-8")

    print(
        f"done snapshotDate={snapshot_date} fundPool={args.fund_pool} "
        f"funds={len(summary_df)} articles={len(article_df)} indicators={len(indicator_df)}"
    )
    print(f"xlsx={output}")
    if args.raw_json_output:
        print(f"raw_json={args.raw_json_output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

#!/usr/bin/env python3
"""Build a streamlined operations dashboard from split add/sub Excel."""

from __future__ import annotations

import json
import argparse
import statistics
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd


INPUT_XLSX = Path("/Users/yaoruanxingchen/c/exports/addsub/加仓减仓分表版20260312_20260409.xlsx")
OUTPUT_HTML = Path("/Users/yaoruanxingchen/c/exports/addsub/看板_核心版_20260312_20260409.html")
DETAIL_RAW_JSON = Path("/Users/yaoruanxingchen/c/exports/addsub/基金详情抓取_20260413_raw.json")


def norm_code(v: Any) -> str:
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return ""
    s = str(v).strip()
    if s.endswith(".0"):
        s = s[:-2]
    if s.isdigit():
        return s.zfill(6)
    return s


def clean_fund_name(v: Any) -> str:
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return ""
    s = str(v).strip()
    if s.endswith("-道乐数据"):
        s = s[: -len("-道乐数据")].rstrip()
    return s


def to_num(v: Any) -> float | None:
    n = pd.to_numeric(v, errors="coerce")
    if pd.isna(n):
        return None
    return float(n)


def norm_return_pct(v: Any) -> float | None:
    n = to_num(v)
    if n is None:
        return None
    if abs(n) > 100:
        return n / 10000.0
    return n


def norm_day_return_pct(v: Any) -> float | None:
    n = to_num(v)
    if n is None:
        return None
    if abs(n) > 1000:
        n = n / 10000.0
    elif abs(n) > 20:
        n = n / 100.0
    return n


def repair_cross_scope_metric_outliers(df: pd.DataFrame) -> pd.DataFrame:
    """For same date/board/fund, harmonize return metrics across scopes and repair outliers."""
    out = df.copy()
    required = {"统计日期", "榜单", "基金代码", "基金范围"}
    if not required.issubset(out.columns):
        return out

    metric_cfg = [
        ("日涨跌幅(%)", norm_day_return_pct, lambda ref: max(1.5, 6 * abs(ref) + 0.8)),
        ("近1月涨跌幅(%)", norm_return_pct, lambda ref: max(5.0, 4 * abs(ref) + 3.0)),
        ("近1年涨跌幅(%)", norm_return_pct, lambda ref: max(12.0, 3 * abs(ref) + 8.0)),
    ]
    for col, normalizer, _ in metric_cfg:
        if col in out.columns:
            out[col] = out[col].map(normalizer)

    for _, idx in out.groupby(["统计日期", "榜单", "基金代码"]).groups.items():
        block = out.loc[idx]
        non_all = block[block["基金范围"] != "全部基金"]
        all_rows = block[block["基金范围"] == "全部基金"]
        if non_all.empty or all_rows.empty:
            continue
        for col, _, tol_fn in metric_cfg:
            if col not in out.columns:
                continue
            vals = pd.to_numeric(non_all[col], errors="coerce").dropna().tolist()
            if not vals:
                continue
            base = float(statistics.median(vals))
            tol = tol_fn(base)
            for i in all_rows.index:
                cur = pd.to_numeric(pd.Series([out.at[i, col]]), errors="coerce").iloc[0]
                if pd.isna(cur) or abs(float(cur) - base) > tol:
                    out.at[i, col] = base

    return out


def load_invest_direction_map(detail_raw_json: Path) -> dict[str, str]:
    if not detail_raw_json.exists():
        return {}
    try:
        arr = json.loads(detail_raw_json.read_text(encoding="utf-8"))
    except Exception:
        return {}
    out: dict[str, str] = {}
    for item in arr if isinstance(arr, list) else []:
        fund_code = norm_code(item.get("fundCode"))
        responses = item.get("responses") or {}
        follow_data = (responses.get("userFollowData") or {}).get("data") or {}
        invest_direction = (
            str(item.get("investment") or "").strip()
            or str(item.get("investDirection") or "").strip()
            or str(follow_data.get("gfFundTypeFirst") or "").strip()
            or str(follow_data.get("fundTypeFirst") or "").strip()
            or str(follow_data.get("fundTypeName") or "").strip()
        )
        if fund_code:
            out[fund_code] = invest_direction
    return out


def load_rows(xlsx_path: Path, detail_raw_json: Path) -> tuple[list[dict[str, Any]], dict[str, str]]:
    add_df = pd.read_excel(xlsx_path, sheet_name="加仓榜")
    sub_df = pd.read_excel(xlsx_path, sheet_name="减仓榜")
    add_df["榜单"] = "加仓"
    sub_df["榜单"] = "减仓"
    df = pd.concat([add_df, sub_df], ignore_index=True)
    df = repair_cross_scope_metric_outliers(df)
    invest_map = load_invest_direction_map(detail_raw_json)

    rows: list[dict[str, Any]] = []
    for _, r in df.iterrows():
        fund_code = norm_code(r.get("基金代码"))
        fund_category = str(r.get("基金类型") or "").strip()
        fund_scope = str(r.get("基金范围") or "").strip()
        invest_direction = invest_map.get(fund_code, "").strip() or fund_category or fund_scope or "未知"
        rows.append(
            {
                "date": pd.to_datetime(r.get("统计日期"), errors="coerce").strftime("%Y-%m-%d")
                if not pd.isna(pd.to_datetime(r.get("统计日期"), errors="coerce"))
                else "",
                "board": str(r.get("榜单") or ""),
                "type": fund_scope,  # UI displays as 类型 per new naming rule
                "fund_name": clean_fund_name(r.get("基金简称")),
                "fund_code": fund_code,
                "invest_direction": invest_direction,
                "fund_category": fund_category,
                "rank": to_num(r.get("榜单名次")),
                "day_ret": norm_day_return_pct(r.get("日涨跌幅(%)")),
                "month_ret": norm_return_pct(r.get("近1月涨跌幅(%)")),
            }
        )
    latest_date = ""
    if rows:
        latest_date = max((x["date"] for x in rows if x["date"]), default="")
    update_times = pd.to_datetime(df.get("数据更新时间"), errors="coerce")
    source_update_latest = ""
    if update_times.notna().any():
        source_update_latest = update_times.max().strftime("%Y-%m-%d %H:%M:%S")
    meta = {
        "latest_date": latest_date,
        "crawl_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "source_update_latest": source_update_latest,
    }
    return rows, meta


def build_html(rows: list[dict[str, Any]], meta: dict[str, str]) -> str:
    payload = json.dumps(rows, ensure_ascii=False)
    meta_json = json.dumps(meta, ensure_ascii=False)
    return f"""<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>核心看板（加仓/减仓/双边/当日排序）</title>
  <style>
    :root {{
      --bg:#f5f7fb; --text:#1f2937; --card:#ffffff; --border:#cbd5e1; --softborder:#e2e8f0; --muted:#64748b; --accent:#111827; --thead:#f8fafc;
    }}
    body {{ margin: 0; font-family: "PingFang SC","Microsoft YaHei",sans-serif; background: var(--bg); color: var(--text); }}
    .wrap {{ max-width: 1520px; margin: 0 auto; padding: 16px; }}
    .card {{ background:var(--card); border-radius:10px; padding:12px; margin-bottom:12px; box-shadow: 0 1px 6px rgba(0,0,0,.08); }}
    .tabs button {{ margin-right:8px; border:1px solid var(--border); background:var(--card); border-radius:8px; padding:6px 12px; cursor:pointer; color:var(--text); }}
    .tabs .on {{ background:var(--accent); color:#fff; border-color:var(--accent); }}
    .filters {{ display:grid; grid-template-columns: 180px 180px 180px 180px 280px; gap:8px; }}
    select,input {{ width:100%; padding:8px; border:1px solid var(--border); border-radius:8px; background:var(--card); color:var(--text); }}
    .tiny-note {{ font-size:12px; color:var(--muted); margin-top:4px; line-height:1.2; }}
    .status-note {{ font-size:13px; color:#b45309; margin-top:8px; }}
    table {{ width:100%; border-collapse:collapse; font-size:13px; }}
    th,td {{ padding:8px; border-bottom:1px solid var(--softborder); text-align:left; }}
    th {{ background:var(--thead); position:sticky; top:0; }}
    th.sortable {{ cursor:pointer; user-select:none; }}
    .sort-mark {{ margin-left:4px; color:var(--muted); font-size:12px; }}
    .area {{ max-height:68vh; overflow:auto; }}
    .meta-row {{ display:grid; grid-template-columns: 1fr 1fr 1fr auto; gap:8px; margin-top:10px; }}
    .meta-box {{ background:var(--thead); border:1px solid var(--softborder); border-radius:8px; padding:8px; font-size:13px; }}
    .btn {{ border:1px solid var(--accent); background:var(--accent); color:#fff; border-radius:8px; padding:8px 12px; cursor:pointer; }}
    .jump-links {{ display:flex; flex-wrap:wrap; gap:8px; margin:8px 0 10px; }}
    .jump-links a {{ text-decoration:none; border:1px solid var(--border); background:var(--card); color:var(--text); border-radius:8px; padding:6px 10px; font-size:13px; }}
    .jump-links a.on {{ background:var(--accent); color:#fff; border-color:var(--accent); }}
    .page-desc {{ font-size:13px; color:#475569; margin:2px 0 8px; }}
    .style-row {{ display:grid; grid-template-columns: 220px 1fr; gap:8px; margin-top:8px; }}
  </style>
</head>
<body>
  <div class="wrap">
    <div class="card">
      <h2 style="margin:0 0 10px;">核心看板（加仓/减仓/双边/当日排序）</h2>
      <div class="jump-links">
        <a class="on" href="/">核心看板</a>
        <a href="/fund-detail-cockpit">基金详情驾驶舱</a>
        <a href="/ops-metrics">动态指标看板</a>
        <a href="/competitor-weakness">竞品弱点看板</a>
        <a href="/metrics-doc">指标文档</a>
        <a href="/quickstart">新手导航</a>
      </div>
      <div class="page-desc">这张表用来看“今天和最近一段时间”哪些基金最热、加仓还是减仓更强，适合做日常盯盘。</div>
      <div class="tabs">
        <button id="tab_add" class="on">加仓表</button>
        <button id="tab_sub">减仓表</button>
        <button id="tab_both">双边表</button>
        <button id="tab_daily">当日排序表</button>
      </div>
      <div class="filters" style="margin-top:10px;">
        <div>
          <select id="date"></select>
          <select id="daily_board" style="display:none;">
            <option value="加仓">加仓榜</option>
            <option value="减仓">减仓榜</option>
          </select>
          <div class="tiny-note" id="date_note">这是滚动排序，看当天请去“当日排序表”。</div>
        </div>
        <select id="type"></select>
        <select id="fund_type"></select>
      </div>
      <div class="filters" style="margin-top:8px; grid-template-columns: 1fr;">
        <input id="q" placeholder="搜索基金名称/代码" />
      </div>
      <div class="style-row">
        <select id="theme_sel">
          <option value="org">机构简报风</option>
          <option value="terminal">交易终端风</option>
          <option value="research">券商研究风</option>
          <option value="minimal">极简运营风</option>
          <option value="news">数据新闻风</option>
          <option value="industrial">工业仪表风</option>
          <option value="guofeng">国风金融风</option>
          <option value="tech">科技蓝图风</option>
          <option value="warm">暖色决策风</option>
          <option value="brand">品牌定制风</option>
        </select>
        <div class="tiny-note">风格切换面板：只改视觉样式，不影响数据与计算。</div>
      </div>
      <div class="status-note" id="status_note"></div>
      <div class="tiny-note" id="scope_note"></div>
      <div class="meta-row">
        <div class="meta-box">最新数据日期：<span id="meta_latest"></span></div>
        <div class="meta-box">本次爬取时间：<span id="meta_crawl"></span></div>
        <div class="meta-box">源数据更新时间：<span id="meta_src_upd"></span></div>
        <button class="btn" id="btn_update">再次爬取检测更新</button>
      </div>
    </div>

    <div class="card area">
      <table>
        <thead id="thead"></thead>
        <tbody id="tb"></tbody>
      </table>
    </div>
  </div>

  <script>
    const DATA = {payload};
    const META = {meta_json};
    let mode = "add"; // add | sub | both | daily
    const tabAdd = document.getElementById("tab_add");
    const tabSub = document.getElementById("tab_sub");
    const tabBoth = document.getElementById("tab_both");
    const tabDaily = document.getElementById("tab_daily");
    const dateSel = document.getElementById("date");
    const dailyBoardSel = document.getElementById("daily_board");
    const dateNote = document.getElementById("date_note");
    const typeSel = document.getElementById("type");
    const fundTypeSel = document.getElementById("fund_type");
    const qInput = document.getElementById("q");
    const themeSel = document.getElementById("theme_sel");
    const statusNote = document.getElementById("status_note");
    const scopeNote = document.getElementById("scope_note");
    const thead = document.getElementById("thead");
    const tbody = document.getElementById("tb");
    const btnUpdate = document.getElementById("btn_update");
    let sortState = {{ key: "m7", order: "desc" }};
    const THEMES = {{
      org:       {{bg:'#f5f7fb',text:'#1f2937',card:'#ffffff',border:'#cbd5e1',softborder:'#e2e8f0',muted:'#64748b',accent:'#111827',thead:'#f8fafc'}},
      terminal:  {{bg:'#0b1220',text:'#dbeafe',card:'#111827',border:'#1f2937',softborder:'#1f2937',muted:'#93c5fd',accent:'#22c55e',thead:'#0f172a'}},
      research:  {{bg:'#f7f4ee',text:'#1f2937',card:'#fffdf8',border:'#e5dccb',softborder:'#efe7d9',muted:'#6b7280',accent:'#1d4ed8',thead:'#faf7f0'}},
      minimal:   {{bg:'#fafafa',text:'#111827',card:'#ffffff',border:'#e5e7eb',softborder:'#e5e7eb',muted:'#6b7280',accent:'#111827',thead:'#f9fafb'}},
      news:      {{bg:'#fffaf5',text:'#111827',card:'#ffffff',border:'#f1e4d5',softborder:'#f1e4d5',muted:'#7c6f64',accent:'#b45309',thead:'#fff7ed'}},
      industrial:{{bg:'#101827',text:'#e5e7eb',card:'#111827',border:'#374151',softborder:'#374151',muted:'#9ca3af',accent:'#0ea5e9',thead:'#0f172a'}},
      guofeng:   {{bg:'#faf6ef',text:'#2c1f16',card:'#fffaf2',border:'#e8d9bf',softborder:'#e8d9bf',muted:'#6b4f3f',accent:'#8b5e34',thead:'#f6efe3'}},
      tech:      {{bg:'#06131f',text:'#d1fae5',card:'#0b2233',border:'#155e75',softborder:'#155e75',muted:'#67e8f9',accent:'#06b6d4',thead:'#082f49'}},
      warm:      {{bg:'#fff7ed',text:'#431407',card:'#fffbf5',border:'#fed7aa',softborder:'#fed7aa',muted:'#9a3412',accent:'#ea580c',thead:'#ffedd5'}},
      brand:     {{bg:'#f3f7ff',text:'#1e3a8a',card:'#ffffff',border:'#bfdbfe',softborder:'#bfdbfe',muted:'#3b82f6',accent:'#2563eb',thead:'#eff6ff'}},
    }};
    function applyTheme(name) {{
      const t = THEMES[name] || THEMES.org;
      const root = document.documentElement;
      root.style.setProperty('--bg', t.bg);
      root.style.setProperty('--text', t.text);
      root.style.setProperty('--card', t.card);
      root.style.setProperty('--border', t.border);
      root.style.setProperty('--softborder', t.softborder);
      root.style.setProperty('--muted', t.muted);
      root.style.setProperty('--accent', t.accent);
      root.style.setProperty('--thead', t.thead);
      try {{ localStorage.setItem('dashboard_theme', name); }} catch(e) {{}}
    }}

    const uniq = arr => [...new Set(arr)];
    const dates = uniq(DATA.map(r => r.date)).filter(Boolean).sort((a,b)=>a>b?-1:1);
    dateSel.innerHTML = dates.map(d => `<option value="${{d}}">${{d}}</option>`).join("");
    const types = uniq(DATA.map(r => r.type)).filter(Boolean).sort();
    typeSel.innerHTML = types.map(t => `<option value="${{t}}">${{t}}</option>`).join("");
    if (types.includes("全部基金")) typeSel.value = "全部基金";
    const fundTypes = uniq(DATA.map(r => r.fund_category)).filter(Boolean).sort();
    fundTypeSel.innerHTML = '<option value="">全部基金类型</option>' + fundTypes.map(t => `<option value="${{t}}">${{t}}</option>`).join("");
    document.getElementById("meta_latest").textContent = META.latest_date || "-";
    document.getElementById("meta_crawl").textContent = META.crawl_at || "-";
    document.getElementById("meta_src_upd").textContent = META.source_update_latest || "-";

    function todayStrLocal() {{
      const d = new Date();
      const y = d.getFullYear();
      const m = String(d.getMonth() + 1).padStart(2, "0");
      const day = String(d.getDate()).padStart(2, "0");
      return `${{y}}-${{m}}-${{day}}`;
    }}
    function refreshStatusNote() {{
      const latest = META.latest_date || "";
      const today = todayStrLocal();
      if (latest && latest < today) {{
        if (mode === "daily") {{
          statusNote.textContent = `当日未更新爬取，每天四点半前爬（当前更新至${{latest}}）`;
        }} else {{
          statusNote.textContent = `数据爬取更新至前一日（${{latest}}）`;
        }}
      }} else {{
        statusNote.textContent = "";
      }}
    }}

    function setTab(next) {{
      mode = next;
      [tabAdd, tabSub, tabBoth, tabDaily].forEach(btn => btn.classList.remove("on"));
      if (mode === "add") tabAdd.classList.add("on");
      if (mode === "sub") tabSub.classList.add("on");
      if (mode === "both") tabBoth.classList.add("on");
      if (mode === "daily") tabDaily.classList.add("on");
      // All tabs: single date selector only.
      dailyBoardSel.style.display = mode === "daily" ? "" : "none";
      typeSel.style.display = "";
      fundTypeSel.style.display = "";
      qInput.style.display = mode === "daily" ? "none" : "";
      dateNote.textContent = mode === "daily" ? "当日排序" : "这是滚动排序，看当天请去“当日排序表”。";
      refreshStatusNote();
      render();
    }}
    tabAdd.addEventListener("click", () => setTab("add"));
    tabSub.addEventListener("click", () => setTab("sub"));
    tabBoth.addEventListener("click", () => setTab("both"));
    tabDaily.addEventListener("click", () => setTab("daily"));
    themeSel.addEventListener("input", () => applyTheme(themeSel.value));
    [dateSel, dailyBoardSel, typeSel, fundTypeSel, qInput].forEach(el => el.addEventListener("input", render));
    function bindSortableHeaders() {{
      document.querySelectorAll("th.sortable").forEach(th => {{
        th.addEventListener("click", () => {{
          const k = th.dataset.key;
          if (sortState.key === k) sortState.order = sortState.order === "desc" ? "asc" : "desc";
          else {{ sortState.key = k; sortState.order = "desc"; }}
          render();
        }});
      }});
    }}
    btnUpdate.addEventListener("click", async () => {{
      btnUpdate.disabled = true;
      btnUpdate.textContent = "再次爬取检测更新中...";
      try {{
        const resp = await fetch("/api/update", {{ method: "POST" }});
        const payload = await resp.json();
        if (!resp.ok || !payload.ok) throw new Error(payload.error || "更新失败");
        alert("更新完成，页面将刷新。");
        window.location.reload();
      }} catch (err) {{
        alert("更新失败：" + err.message + "。如果是静态托管，请在服务端执行更新脚本。");
      }} finally {{
        btnUpdate.disabled = false;
        btnUpdate.textContent = "再次爬取检测更新";
      }}
    }});

    function n(v) {{ const x = Number(v); return Number.isFinite(x) ? x : 0; }}
    function compoundReturns(dayRets) {{
      let acc = 1;
      for (const r of dayRets) acc *= (1 + n(r)/100);
      return (acc - 1) * 100;
    }}
    function applyCommonFilter(r, exactDate, selectedType, selectedFundType, q) {{
      if (exactDate && r.date !== exactDate) return false;
      if (selectedType && r.type !== selectedType) return false;
      if (selectedFundType && r.fund_category !== selectedFundType) return false;
      if (q && !(String(r.fund_name||"").includes(q) || String(r.fund_code||"").includes(q))) return false;
      return true;
    }}

    function buildRows() {{
      const exactDate = dateSel.value;
      const selectedType = typeSel.value;
      const selectedFundType = fundTypeSel.value;
      const q = qInput.value.trim();

      // Daily ranking tab: show only selected date ranking (not rolling windows).
      if (mode === "daily") {{
        const board = dailyBoardSel.value || "加仓";
        const dailyScope = selectedType || "全部基金";
        const rows = DATA
          .filter(r => r.board === board && applyCommonFilter(r, exactDate, dailyScope, selectedFundType, ""))
          .map(r => ({{
            fund_name: r.fund_name,
            fund_code: r.fund_code,
            invest_direction: r.invest_direction || "",
            type: r.type,
            fund_category: r.fund_category || "",
            m7: null,
            m14: null,
            m30: null,
            r30: r.month_ret,
            r14: null,
            r7: null,
            r1: r.day_ret,
            rank: r.rank,
            board: r.board,
          }}));
        rows.sort((a,b)=> (a.board > b.board ? 1 : -1) || n(a.rank)-n(b.rank));
        return rows;
      }}

      const histDates = dates.filter(d => !exactDate || d <= exactDate).sort();
      const histDateSet = new Set(histDates);
      const filtered = DATA.filter(r => {{
        if (!r.date || !histDateSet.has(r.date)) return false;
        if (!applyCommonFilter(r, "", selectedType, selectedFundType, q)) return false;
        if (mode === "add" && r.board !== "加仓") return false;
        if (mode === "sub" && r.board !== "减仓") return false;
        return true;
      }});

      const mp = new Map();
      const endIdx = histDates.length - 1;
      const win30 = new Set(endIdx >= 0 ? histDates.slice(Math.max(0, endIdx - 29), endIdx + 1) : []);
      const win14 = new Set(endIdx >= 0 ? histDates.slice(Math.max(0, endIdx - 13), endIdx + 1) : []);
      const win7 = new Set(endIdx >= 0 ? histDates.slice(Math.max(0, endIdx - 6), endIdx + 1) : []);

      for (const r of filtered) {{
        const key = r.fund_code || r.fund_name;
        if (!key) continue;
        if (!mp.has(key)) {{
          mp.set(key, {{
            fund_name: r.fund_name || "",
            fund_code: r.fund_code || "",
            invest_direction: r.invest_direction || "",
            type: r.type || "",
            d30: new Set(), d14: new Set(), d7: new Set(),
            latestDate: "", latestMonth: null, latestDay: null,
            exactMonth: null, exactDay: null,
            hasExact: false,
            dayByDate: {{}}, boardSeen: new Set(), inRangeCount: 0,
          }});
        }}
        const o = mp.get(key);
        const dateKey = mode === "both" ? `${{r.date}}|${{r.board}}` : r.date;
        if (win30.has(r.date)) o.d30.add(dateKey);
        if (win14.has(r.date)) o.d14.add(dateKey);
        if (win7.has(r.date)) o.d7.add(dateKey);
        o.inRangeCount += 1;
        o.boardSeen.add(r.board);
        if (!o.dayByDate[r.date]) o.dayByDate[r.date] = [];
        o.dayByDate[r.date].push(r.day_ret);
        if (!o.latestDate || r.date > o.latestDate) {{
          o.latestDate = r.date; o.latestMonth = r.month_ret; o.latestDay = r.day_ret;
        }}
        if (exactDate && r.date === exactDate) {{
          o.hasExact = true;
          if (o.exactMonth == null && r.month_ret != null) o.exactMonth = r.month_ret;
          if (o.exactDay == null && r.day_ret != null) o.exactDay = r.day_ret;
        }}
      }}

      const out = [];
      for (const [,o] of mp) {{
        if (exactDate && !o.hasExact) continue;
        if (mode === "both" && o.boardSeen.size < 2) continue;
        const dates14 = [...win14].sort();
        const dates7 = [...win7].sort();
        const day14 = dates14.flatMap(d => (o.dayByDate[d] || []).slice(0,1));
        const day7 = dates7.flatMap(d => (o.dayByDate[d] || []).slice(0,1));
        out.push({{
          fund_name: o.fund_name, fund_code: o.fund_code, invest_direction: o.invest_direction || "", type: o.type,
          m30: o.d30.size, m14: o.d14.size, m7: o.d7.size,
          r30: o.exactMonth != null ? o.exactMonth : null, r14: day14.length ? compoundReturns(day14) : null,
          r7: day7.length ? compoundReturns(day7) : null, r1: o.exactDay != null ? o.exactDay : null,
        }});
      }}
      return out;
    }}

    function render() {{
      const rows = buildRows();
      const qtxt = (qInput.value || "").trim();
      scopeNote.textContent = mode === "daily"
        ? `当前口径：当日排序；日期=${{dateSel.value || META.latest_date}}；榜单=${{dailyBoardSel.value || "加仓榜"}}；范围=${{typeSel.value || "全部基金"}}；基金类型=${{fundTypeSel.value || "全部"}}`
        : `当前口径：滚动统计；日期=${{dateSel.value || META.latest_date}}；范围=${{typeSel.value || "全部基金"}}；基金类型=${{fundTypeSel.value || "全部"}}；检索=${{qtxt || "无"}}`;
      if (mode === "daily") {{
        thead.innerHTML = `<tr>
          <th>基金名称</th>
          <th>基金代码</th>
          <th>投资方向</th>
          <th>基金类型</th>
          <th>当日排序</th>
        </tr>`;
      }} else {{
        thead.innerHTML = `<tr>
          <th>基金名称</th>
          <th>基金代码</th>
          <th>投资方向</th>
          <th>类型</th>
          <th class="sortable" data-key="m7">近7日上榜次数<span class="sort-mark"></span></th>
          <th class="sortable" data-key="m14">近2周上榜次数<span class="sort-mark"></span></th>
          <th class="sortable" data-key="m30">近1月上榜次数<span class="sort-mark"></span></th>
          <th class="sortable" data-key="r7">近1周涨跌幅(%)<span class="sort-mark"></span></th>
          <th class="sortable" data-key="r14">近2周涨跌幅(%)<span class="sort-mark"></span></th>
          <th class="sortable" data-key="r30">近1月涨跌幅(%)<span class="sort-mark"></span></th>
          <th class="sortable" data-key="r1">日涨跌幅(%)<span class="sort-mark"></span></th>
        </tr>`;
        bindSortableHeaders();
      }}
      if (mode !== "daily") {{
        const k = sortState.key;
        const ord = sortState.order === "asc" ? 1 : -1;
        rows.sort((a,b) => (n(a[k]) - n(b[k])) * ord || (a.fund_code > b.fund_code ? 1 : -1));
      }}
      document.querySelectorAll("th.sortable").forEach(th => {{
        const mark = th.querySelector(".sort-mark");
        if (!mark) return;
        if (mode === "daily") {{ mark.textContent = ""; return; }}
        mark.textContent = th.dataset.key === sortState.key ? (sortState.order === "desc" ? "▼" : "▲") : "";
      }});
      if (mode === "daily") {{
        tbody.innerHTML = rows.map(r => `
          <tr>
            <td>${{r.fund_name||""}}</td>
            <td>${{r.fund_code||""}}</td>
            <td>${{r.invest_direction||""}}</td>
            <td>${{r.fund_category||""}}</td>
            <td>${{r.rank==null?"":Number(r.rank)}}</td>
          </tr>
        `).join("");
      }} else {{
        tbody.innerHTML = rows.map(r => `
          <tr>
            <td>${{r.fund_name||""}}</td>
            <td>${{r.fund_code||""}}</td>
            <td>${{r.invest_direction||""}}</td>
            <td>${{r.type||""}}</td>
            <td>${{r.m7==null?"-":r.m7}}</td>
            <td>${{r.m14==null?"-":r.m14}}</td>
            <td>${{r.m30==null?"-":r.m30}}</td>
            <td>${{r.r7==null?"":r.r7.toFixed(2)}}</td>
            <td>${{r.r14==null?"":r.r14.toFixed(2)}}</td>
            <td>${{r.r30==null?"":r.r30.toFixed(2)}}</td>
            <td>${{r.r1==null?"":r.r1.toFixed(2)}}</td>
          </tr>
        `).join("");
      }}
    }}
    try {{
      const savedTheme = localStorage.getItem('dashboard_theme') || localStorage.getItem('core_theme') || 'org';
      if (THEMES[savedTheme]) themeSel.value = savedTheme;
      applyTheme(themeSel.value);
    }} catch(e) {{ applyTheme('org'); }}
    setTab("add");
  </script>
</body>
</html>
"""


def main() -> int:
    parser = argparse.ArgumentParser(description="Build core dashboard html from split workbook.")
    parser.add_argument("--input", default=str(INPUT_XLSX))
    parser.add_argument("--output", default=str(OUTPUT_HTML))
    parser.add_argument("--detail-raw-json", default=str(DETAIL_RAW_JSON))
    args = parser.parse_args()

    input_path = Path(args.input)
    output_path = Path(args.output)
    if not input_path.exists():
        raise SystemExit(f"Input file not found: {input_path}")
    rows, meta = load_rows(input_path, Path(args.detail_raw_json))
    output_path.write_text(build_html(rows, meta), encoding="utf-8")
    print(output_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

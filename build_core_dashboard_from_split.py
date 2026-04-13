#!/usr/bin/env python3
"""Build a streamlined operations dashboard from split add/sub Excel."""

from __future__ import annotations

import json
import argparse
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


def to_num(v: Any) -> float | None:
    n = pd.to_numeric(v, errors="coerce")
    if pd.isna(n):
        return None
    return float(n)


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
            str(follow_data.get("fundTypeFirst") or "").strip()
            or str(follow_data.get("gfFundTypeFirst") or "").strip()
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
    invest_map = load_invest_direction_map(detail_raw_json)

    rows: list[dict[str, Any]] = []
    for _, r in df.iterrows():
        rows.append(
            {
                "date": pd.to_datetime(r.get("统计日期"), errors="coerce").strftime("%Y-%m-%d")
                if not pd.isna(pd.to_datetime(r.get("统计日期"), errors="coerce"))
                else "",
                "board": str(r.get("榜单") or ""),
                "type": str(r.get("基金范围") or ""),  # UI displays as 类型 per new naming rule
                "fund_name": str(r.get("基金简称") or ""),
                "fund_code": norm_code(r.get("基金代码")),
                "invest_direction": invest_map.get(norm_code(r.get("基金代码")), ""),
                "fund_category": str(r.get("基金类型") or ""),
                "rank": to_num(r.get("榜单名次")),
                "day_ret": to_num(r.get("日涨跌幅(%)")),
                "month_ret": to_num(r.get("近1月涨跌幅(%)")),
            }
        )
    latest_date = ""
    if rows:
        latest_date = max((x["date"] for x in rows if x["date"]), default="")
    meta = {
        "latest_date": latest_date,
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
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
    body {{ margin: 0; font-family: "PingFang SC","Microsoft YaHei",sans-serif; background: #f5f7fb; color: #1f2937; }}
    .wrap {{ max-width: 1520px; margin: 0 auto; padding: 16px; }}
    .card {{ background:#fff; border-radius:10px; padding:12px; margin-bottom:12px; box-shadow: 0 1px 6px rgba(0,0,0,.08); }}
    .tabs button {{ margin-right:8px; border:1px solid #cbd5e1; background:#fff; border-radius:8px; padding:6px 12px; cursor:pointer; }}
    .tabs .on {{ background:#111827; color:#fff; border-color:#111827; }}
    .filters {{ display:grid; grid-template-columns: 180px 180px 180px 180px 280px; gap:8px; }}
    select,input {{ width:100%; padding:8px; border:1px solid #cbd5e1; border-radius:8px; }}
    .tiny-note {{ font-size:12px; color:#64748b; margin-top:4px; line-height:1.2; }}
    .status-note {{ font-size:13px; color:#b45309; margin-top:8px; }}
    table {{ width:100%; border-collapse:collapse; font-size:13px; }}
    th,td {{ padding:8px; border-bottom:1px solid #e2e8f0; text-align:left; }}
    th {{ background:#f8fafc; position:sticky; top:0; }}
    th.sortable {{ cursor:pointer; user-select:none; }}
    .sort-mark {{ margin-left:4px; color:#64748b; font-size:12px; }}
    .area {{ max-height:68vh; overflow:auto; }}
    .meta-row {{ display:grid; grid-template-columns: 1fr 1fr auto; gap:8px; margin-top:10px; }}
    .meta-box {{ background:#f8fafc; border:1px solid #e2e8f0; border-radius:8px; padding:8px; font-size:13px; }}
    .btn {{ border:1px solid #111827; background:#111827; color:#fff; border-radius:8px; padding:8px 12px; cursor:pointer; }}
    .jump-links {{ display:flex; flex-wrap:wrap; gap:8px; margin:8px 0 10px; }}
    .jump-links a {{ text-decoration:none; border:1px solid #cbd5e1; background:#fff; color:#111827; border-radius:8px; padding:6px 10px; font-size:13px; }}
    .jump-links a.on {{ background:#111827; color:#fff; border-color:#111827; }}
    .page-desc {{ font-size:13px; color:#475569; margin:2px 0 8px; }}
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
      <div class="status-note" id="status_note"></div>
      <div class="meta-row">
        <div class="meta-box">最新数据日期：<span id="meta_latest"></span></div>
        <div class="meta-box">看板生成时间：<span id="meta_gen"></span></div>
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
    const statusNote = document.getElementById("status_note");
    const thead = document.getElementById("thead");
    const tbody = document.getElementById("tb");
    const btnUpdate = document.getElementById("btn_update");
    let sortState = {{ key: "m7", order: "desc" }};

    const uniq = arr => [...new Set(arr)];
    const dates = uniq(DATA.map(r => r.date)).filter(Boolean).sort((a,b)=>a>b?-1:1);
    dateSel.innerHTML = dates.map(d => `<option value="${{d}}">${{d}}</option>`).join("");
    const types = uniq(DATA.map(r => r.type)).filter(Boolean).sort();
    typeSel.innerHTML = '<option value="">全部类型</option>' + types.map(t => `<option value="${{t}}">${{t}}</option>`).join("");
    const fundTypes = uniq(DATA.map(r => r.fund_category)).filter(Boolean).sort();
    fundTypeSel.innerHTML = '<option value="">全部基金类型</option>' + fundTypes.map(t => `<option value="${{t}}">${{t}}</option>`).join("");
    document.getElementById("meta_latest").textContent = META.latest_date || "-";
    document.getElementById("meta_gen").textContent = META.generated_at || "-";

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
      typeSel.style.display = mode === "daily" ? "none" : "";
      fundTypeSel.style.display = mode === "daily" ? "none" : "";
      qInput.style.display = mode === "daily" ? "none" : "";
      dateNote.textContent = mode === "daily" ? "当日排序" : "这是滚动排序，看当天请去“当日排序表”。";
      refreshStatusNote();
      render();
    }}
    tabAdd.addEventListener("click", () => setTab("add"));
    tabSub.addEventListener("click", () => setTab("sub"));
    tabBoth.addEventListener("click", () => setTab("both"));
    tabDaily.addEventListener("click", () => setTab("daily"));
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
        const rows = DATA
          .filter(r => r.board === board && applyCommonFilter(r, exactDate, "", "", ""))
          .map(r => ({{
            fund_name: r.fund_name,
            fund_code: r.fund_code,
            invest_direction: r.invest_direction || "",
            type: r.type,
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
      }}

      const out = [];
      for (const [,o] of mp) {{
        if (mode === "both" && o.boardSeen.size < 2) continue;
        const dates14 = [...win14].sort();
        const dates7 = [...win7].sort();
        const day14 = dates14.flatMap(d => (o.dayByDate[d] || []).slice(0,1));
        const day7 = dates7.flatMap(d => (o.dayByDate[d] || []).slice(0,1));
        out.push({{
          fund_name: o.fund_name, fund_code: o.fund_code, invest_direction: o.invest_direction || "", type: o.type,
          m30: o.d30.size, m14: o.d14.size, m7: o.d7.size,
          r30: o.latestMonth, r14: day14.length ? compoundReturns(day14) : null,
          r7: day7.length ? compoundReturns(day7) : null, r1: o.latestDay,
        }});
      }}
      return out;
    }}

    function render() {{
      const rows = buildRows();
      if (mode === "daily") {{
        thead.innerHTML = `<tr>
          <th>基金名称</th>
          <th>基金代码</th>
          <th>投资方向</th>
          <th>类型</th>
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
            <td>${{r.type||""}}</td>
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

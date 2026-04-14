#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd


def n(v: Any) -> float:
    x = pd.to_numeric(v, errors="coerce")
    if pd.isna(x):
        return 0.0
    return float(x)


def normalize_percent(v: Any) -> float:
    x = n(v)
    if x <= 0:
        return 0.0
    return x * 100.0 if x <= 1.0 else x


def t(v: Any) -> str:
    if v is None:
        return ""
    if isinstance(v, float) and pd.isna(v):
        return ""
    s = str(v).strip()
    return "" if s.lower() == "nan" else s


def norm_code(v: Any) -> str:
    if v is None:
        return ""
    s = str(v).strip()
    if s.endswith(".0"):
        s = s[:-2]
    return s.zfill(6) if s.isdigit() else s


def load_data(xlsx: Path) -> tuple[list[dict[str, Any]], list[dict[str, Any]], dict[str, Any]]:
    core = pd.read_excel(xlsx, sheet_name="详情核心指标")
    notes = pd.read_excel(xlsx, sheet_name="动态和笔记")

    core["基金代码"] = core["基金代码"].map(norm_code)
    notes["基金代码"] = notes["基金代码"].map(norm_code)

    notes["发布时间"] = pd.to_datetime(notes["发布时间"], errors="coerce")
    notes["日"] = notes["发布时间"].dt.strftime("%Y-%m-%d")
    notes["互动总量"] = notes["点赞数"].map(n) + notes["评论数"].map(n)
    notes["有效互动率_百分比"] = notes["有效互动率(%)"].map(normalize_percent)
    latest_note_time = notes["发布时间"].dropna().max() if "发布时间" in notes.columns else pd.NaT
    notes_7d = notes.copy()
    if pd.notna(latest_note_time):
        notes_7d = notes[notes["发布时间"] >= latest_note_time - pd.Timedelta(days=7)]

    note_agg = (
        notes.groupby("基金代码", dropna=False)
        .agg(
            笔记浏览总量=("浏览量", "sum"),
            笔记点赞总量=("点赞数", "sum"),
            笔记评论总量=("评论数", "sum"),
            笔记互动总量=("互动总量", "sum"),
            笔记条数_实抓=("帖子ID", "count"),
            笔记平均互动率=("有效互动率_百分比", "mean"),
        )
        .reset_index()
    )
    note_agg_7d = (
        notes_7d.groupby("基金代码", dropna=False)
        .agg(
            实抓近7天帖子数=("帖子ID", "count"),
            实抓近7天互动总量=("互动总量", "sum"),
        )
        .reset_index()
    )
    day_trend = (
        notes.groupby("日", dropna=False)
        .agg(发帖数=("帖子ID", "count"), 浏览量=("浏览量", "sum"), 互动量=("互动总量", "sum"))
        .reset_index()
        .sort_values("日")
    )

    merged = core.merge(note_agg, on="基金代码", how="left").merge(note_agg_7d, on="基金代码", how="left")
    for c in [
        "笔记浏览总量",
        "笔记点赞总量",
        "笔记评论总量",
        "笔记互动总量",
        "笔记条数_实抓",
        "笔记平均互动率",
        "实抓近7天帖子数",
        "实抓近7天互动总量",
    ]:
        if c in merged.columns:
            merged[c] = merged[c].fillna(0)

    rows = []
    for _, r in merged.iterrows():
        rows.append(
            {
                "基金代码": norm_code(r.get("基金代码")),
                "基金名称": t(r.get("基金名称")),
                "基金公司名称": t(r.get("基金公司名称")),
                "投资方向标签": t(r.get("投资方向标签")) or "未分类",
                "投资方向说明": t(r.get("投资方向说明")),
                "榜单类型": t(r.get("榜单类型")),
                "基金范围": t(r.get("基金范围")),
                "本周浏览人数": n(r.get("本周浏览人数")),
                "本月浏览人数": n(r.get("本月浏览人数")),
                "自选人数": n(r.get("自选人数")),
                "持有人数": n(r.get("持有人数")),
                "搜曝比": n(r.get("搜曝比")),
                "曝关比": n(r.get("曝关比")),
                "曝转比": n(r.get("曝转比")),
                "关转比": n(r.get("关转比")),
                "净曝关比": n(r.get("净曝关比")),
                "近7天提及文章数": n(r.get("近7天提及文章数")),  # 平台聚合口径
                "实抓近7天帖子数": n(r.get("实抓近7天帖子数")),  # 本次抓取样本口径
                "动态笔记条数": n(r.get("动态笔记条数")),
                "笔记浏览总量": n(r.get("笔记浏览总量")),
                "笔记点赞总量": n(r.get("笔记点赞总量")),
                "笔记评论总量": n(r.get("笔记评论总量")),
                "笔记互动总量": n(r.get("笔记互动总量")),
                "笔记平均互动率": n(r.get("笔记平均互动率")),
                "基金分析观察周期": t(r.get("基金分析观察周期")),
                "跟踪指数名称": t(r.get("跟踪指数名称")),
                "跟踪指数收益率(%)": n(r.get("跟踪指数收益率(%)")),
                "跟踪评价标题": t(r.get("跟踪评价标题")),
                "跟踪评价说明": t(r.get("跟踪评价说明")),
                "跟踪误差(%)": n(r.get("跟踪误差(%)")),
                "第一梯队阈值(%)": n(r.get("第一梯队阈值(%)")),
                "第二梯队阈值(%)": n(r.get("第二梯队阈值(%)")),
            }
        )

    meta = {
        "snapshot": str(core["统计日期"].iloc[0]) if len(core) else "",
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "fund_count": len(rows),
        "direction_list": sorted({r["投资方向标签"] for r in rows if r["投资方向标签"]}),
        "trend": day_trend.to_dict(orient="records"),
        "note_latest_time": latest_note_time.strftime("%Y-%m-%d %H:%M:%S") if pd.notna(latest_note_time) else "",
    }
    return rows, notes.to_dict(orient="records"), meta


def build_html(rows: list[dict[str, Any]], notes_rows: list[dict[str, Any]], meta: dict[str, Any]) -> str:
    payload = json.dumps(rows, ensure_ascii=False)
    notes_payload = json.dumps(notes_rows, ensure_ascii=False, default=str)
    meta_payload = json.dumps(meta, ensure_ascii=False)
    return f"""<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>基金详情运营驾驶舱</title>
  <style>
    :root {{ --bg:#f4f7fb; --text:#1f2937; --card:#fff; --line:#e5e7eb; --muted:#64748b; --accent:#111827; --thead:#f8fafc; --soft:#cbd5e1; --tagbg:#eff6ff; --tagtext:#1d4ed8; }}
    body {{ margin:0; font-family:"PingFang SC","Microsoft YaHei",sans-serif; background:var(--bg); color:var(--text); }}
    .wrap {{ max-width:1700px; margin:0 auto; padding:16px; }}
    .card {{ background:var(--card); border:1px solid var(--line); border-radius:12px; padding:12px; margin-bottom:12px; }}
    .head {{ display:flex; justify-content:space-between; align-items:flex-end; gap:10px; flex-wrap:wrap; }}
    .jump-links {{ display:flex; flex-wrap:wrap; gap:8px; margin:8px 0 10px; }}
    .jump-links a {{ text-decoration:none; border:1px solid var(--soft); background:var(--card); color:var(--text); border-radius:8px; padding:6px 10px; font-size:13px; }}
    .jump-links a.on {{ background:var(--accent); color:#fff; border-color:var(--accent); }}
    .title {{ font-size:26px; font-weight:800; }}
    .sub {{ color:var(--muted); font-size:13px; margin-top:4px; }}
    .filters {{ display:grid; grid-template-columns: 220px 240px 300px; gap:8px; margin-top:10px; }}
    select,input {{ width:100%; border:1px solid var(--soft); border-radius:8px; padding:8px; font-size:13px; background:var(--card); color:var(--text); }}
    .style-row {{ display:grid; grid-template-columns:220px 1fr; gap:8px; margin-top:8px; }}
    .kpis {{ display:grid; grid-template-columns:repeat(5,1fr); gap:8px; margin-top:10px; }}
    .kpi {{ background:var(--thead); border:1px solid var(--line); border-radius:10px; padding:10px; }}
    .kpi .k {{ color:var(--muted); font-size:12px; }}
    .kpi .v {{ font-size:24px; font-weight:700; margin-top:4px; }}
    .grid2 {{ display:grid; grid-template-columns: 1.2fr 0.8fr; gap:10px; }}
    .grid2b {{ display:grid; grid-template-columns: 1fr 1fr; gap:10px; }}
    .chart {{ width:100%; height:420px; border:1px solid var(--line); border-radius:10px; background:var(--card); }}
    .trend {{ width:100%; height:260px; border:1px solid var(--line); border-radius:10px; background:var(--card); }}
    .box-title {{ font-weight:700; margin-bottom:8px; }}
    .list li {{ margin:6px 0; font-size:13px; }}
    table {{ width:100%; border-collapse:collapse; font-size:13px; }}
    th,td {{ border-bottom:1px solid var(--line); text-align:left; padding:8px; }}
    th {{ position:sticky; top:0; background:var(--thead); }}
    .area {{ max-height:460px; overflow:auto; border:1px solid var(--line); border-radius:10px; }}
    .muted {{ color:var(--muted); }}
    .tag {{ display:inline-block; border:1px solid var(--soft); background:var(--tagbg); color:var(--tagtext); border-radius:999px; padding:2px 8px; font-size:12px; }}
    .legend {{ display:flex; flex-wrap:wrap; gap:8px; margin-top:8px; }}
    .legend-item {{ display:inline-flex; align-items:center; gap:6px; font-size:12px; color:var(--muted); border:1px solid var(--line); border-radius:999px; padding:3px 8px; background:var(--card); }}
    .legend-dot {{ width:10px; height:10px; border-radius:50%; display:inline-block; }}
    @media (max-width: 1200px) {{ .grid2,.grid2b {{ grid-template-columns:1fr; }} .filters {{ grid-template-columns:1fr; }} .kpis {{ grid-template-columns:repeat(2,1fr); }} }}
  </style>
</head>
<body>
<div class="wrap">
  <div class="card">
    <div class="head">
      <div>
        <div class="title">基金详情运营驾驶舱</div>
        <div class="sub">快照日期：<span id="snapshot"></span> ｜ 生成时间：<span id="gen"></span></div>
      </div>
      <div class="muted">目标：先看转化效率，再看内容热度，再用基金分析近1年做质量复核</div>
    </div>
    <div class="jump-links">
      <a href="/">核心看板</a>
      <a class="on" href="/fund-detail-cockpit">基金详情驾驶舱</a>
      <a href="/ops-metrics">动态指标看板</a>
      <a href="/competitor-weakness">竞品弱点看板</a>
      <a href="/metrics-doc">指标文档</a>
      <a href="/quickstart">新手导航</a>
    </div>
    <div class="filters">
      <select id="direction"></select>
      <select id="board">
        <option value="">全部榜单</option>
        <option value="加仓榜">加仓榜</option>
        <option value="减仓榜">减仓榜</option>
      </select>
      <input id="q" placeholder="搜索基金名称/代码/公司" />
    </div>
    <div class="sub" id="scope_note" style="margin-top:6px;"></div>
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
      <div class="sub">风格切换面板：只改视觉样式，不影响数据与计算。</div>
    </div>
    <div class="kpis">
      <div class="kpi"><div class="k">样本基金数</div><div class="v" id="k_count">-</div></div>
      <div class="kpi"><div class="k">近7天提及总数（平台）</div><div class="v" id="k_mention">-</div></div>
      <div class="kpi"><div class="k">近7天提及总数（实抓）</div><div class="v" id="k_mention_cap">-</div></div>
      <div class="kpi"><div class="k">动态总互动量（赞+评）</div><div class="v" id="k_interact">-</div></div>
      <div class="kpi"><div class="k">高关注基金数（自选P75）</div><div class="v" id="k_focus">-</div></div>
    </div>
  </div>

  <div class="grid2">
    <div class="card">
      <div class="box-title">主视图：浏览热度 vs 关转比（气泡=自选人数，颜色=投资方向）</div>
      <svg id="scatter" class="chart"></svg>
      <div id="dir_legend" class="legend"></div>
    </div>
    <div class="card">
      <div class="box-title">运营Top榜</div>
      <div class="grid2b">
        <div>
          <div class="tag">按关转比 Top10</div>
          <ol id="top_ctr" class="list"></ol>
        </div>
        <div>
          <div class="tag">按曝关比 Top10</div>
          <ol id="top_exp" class="list"></ol>
        </div>
      </div>
      <div style="margin-top:8px;">
        <div class="tag">按近7天提及文章数（平台）Top10</div>
        <ol id="top_note" class="list"></ol>
        <div class="tag" style="margin-top:8px;">按近7天提及文章数（实抓）Top10</div>
        <ol id="top_note_cap" class="list"></ol>
      </div>
    </div>
  </div>

  <div class="grid2">
    <div class="card">
      <div class="box-title">明细A：内容热度与口碑（基金级）</div>
      <div class="area">
        <table>
          <thead>
            <tr>
              <th>基金名称</th><th>基金代码</th><th>投资方向</th><th>本周浏览</th><th>自选</th><th>持有</th><th>曝关比</th><th>关转比</th><th>提及文章(7天,平台)</th><th>提及文章(7天,实抓)</th><th>笔记互动总量</th><th>笔记平均互动率(%)</th>
            </tr>
          </thead>
          <tbody id="tb_a"></tbody>
        </table>
      </div>
    </div>
    <div class="card">
      <div class="box-title">明细A补充：按天内容趋势（发帖数/互动量）</div>
      <svg id="trend" class="trend"></svg>
    </div>
  </div>

  <div class="card">
    <div class="box-title">明细B：基金分析近1年（跟踪质量分层）</div>
    <div class="sub">分层口径：跟踪误差 ≤ 第一梯队阈值 = 第一梯队；介于第一/第二阈值 = 第二梯队；> 第二阈值 = 偏弱。</div>
    <div style="display:grid;grid-template-columns:300px 1fr;gap:10px;margin-top:10px;">
      <div>
        <ul id="tier_stats" class="list"></ul>
      </div>
      <div class="area" style="max-height:340px;">
        <table>
          <thead>
            <tr>
              <th>基金名称</th><th>基金代码</th><th>投资方向</th><th>跟踪指数</th><th>指数收益(%)</th><th>跟踪误差(%)</th><th>第一阈值</th><th>第二阈值</th><th>评价</th><th>梯队</th>
            </tr>
          </thead>
          <tbody id="tb_b"></tbody>
        </table>
      </div>
    </div>
  </div>
</div>

<script>
const DATA = {payload};
const NOTES = {notes_payload};
const META = {meta_payload};
const fmt = x => Number.isFinite(Number(x)) ? Number(x).toFixed(2) : "";
const n = x => Number.isFinite(Number(x)) ? Number(x) : 0;
const byId = id => document.getElementById(id);
const themeSel = byId("theme_sel");
const THEMES = {{
  org:       {{bg:'#f5f7fb',text:'#1f2937',card:'#ffffff',line:'#e5e7eb',muted:'#64748b',accent:'#111827',thead:'#f8fafc',soft:'#cbd5e1',tagbg:'#eff6ff',tagtext:'#1d4ed8'}},
  terminal:  {{bg:'#0b1220',text:'#dbeafe',card:'#111827',line:'#1f2937',muted:'#93c5fd',accent:'#22c55e',thead:'#0f172a',soft:'#1f2937',tagbg:'#052e16',tagtext:'#86efac'}},
  research:  {{bg:'#f7f4ee',text:'#1f2937',card:'#fffdf8',line:'#e5dccb',muted:'#6b7280',accent:'#1d4ed8',thead:'#faf7f0',soft:'#d6ccb8',tagbg:'#e0ecff',tagtext:'#1e40af'}},
  minimal:   {{bg:'#fafafa',text:'#111827',card:'#ffffff',line:'#e5e7eb',muted:'#6b7280',accent:'#111827',thead:'#f9fafb',soft:'#d1d5db',tagbg:'#f3f4f6',tagtext:'#111827'}},
  news:      {{bg:'#fffaf5',text:'#111827',card:'#ffffff',line:'#f1e4d5',muted:'#7c6f64',accent:'#b45309',thead:'#fff7ed',soft:'#e9d5b5',tagbg:'#ffedd5',tagtext:'#9a3412'}},
  industrial:{{bg:'#101827',text:'#e5e7eb',card:'#111827',line:'#374151',muted:'#9ca3af',accent:'#0ea5e9',thead:'#0f172a',soft:'#4b5563',tagbg:'#0c4a6e',tagtext:'#bae6fd'}},
  guofeng:   {{bg:'#faf6ef',text:'#2c1f16',card:'#fffaf2',line:'#e8d9bf',muted:'#6b4f3f',accent:'#8b5e34',thead:'#f6efe3',soft:'#d8c2a0',tagbg:'#fef3c7',tagtext:'#7c2d12'}},
  tech:      {{bg:'#06131f',text:'#d1fae5',card:'#0b2233',line:'#155e75',muted:'#67e8f9',accent:'#06b6d4',thead:'#082f49',soft:'#0e7490',tagbg:'#083344',tagtext:'#67e8f9'}},
  warm:      {{bg:'#fff7ed',text:'#431407',card:'#fffbf5',line:'#fed7aa',muted:'#9a3412',accent:'#ea580c',thead:'#ffedd5',soft:'#fdba74',tagbg:'#ffedd5',tagtext:'#9a3412'}},
  brand:     {{bg:'#f3f7ff',text:'#1e3a8a',card:'#ffffff',line:'#bfdbfe',muted:'#3b82f6',accent:'#2563eb',thead:'#eff6ff',soft:'#93c5fd',tagbg:'#dbeafe',tagtext:'#1e3a8a'}},
}};
function applyTheme(name) {{
  const t = THEMES[name] || THEMES.org;
  const root = document.documentElement;
  root.style.setProperty('--bg', t.bg);
  root.style.setProperty('--text', t.text);
  root.style.setProperty('--card', t.card);
  root.style.setProperty('--line', t.line);
  root.style.setProperty('--muted', t.muted);
  root.style.setProperty('--accent', t.accent);
  root.style.setProperty('--thead', t.thead);
  root.style.setProperty('--soft', t.soft);
  root.style.setProperty('--tagbg', t.tagbg);
  root.style.setProperty('--tagtext', t.tagtext);
  try {{ localStorage.setItem('dashboard_theme', name); }} catch(e) {{}}
}}

byId("snapshot").textContent = META.snapshot || "-";
byId("gen").textContent = META.generated_at || "-";

const dirSel = byId("direction");
const dirs = ["全部投资方向", ...(META.direction_list || [])];
dirSel.innerHTML = dirs.map(x => `<option value="${{x === "全部投资方向" ? "" : x}}">${{x}}</option>`).join("");

function filtered() {{
  const d = dirSel.value;
  const b = byId("board").value;
  const q = byId("q").value.trim();
  return DATA.filter(r => {{
    if (d && r["投资方向标签"] !== d) return false;
    if (b && r["榜单类型"] !== b) return false;
    if (q && !(String(r["基金名称"]||"").includes(q) || String(r["基金代码"]||"").includes(q) || String(r["基金公司名称"]||"").includes(q))) return false;
    return true;
  }});
}}

function percentile(arr, p) {{
  const a = [...arr].sort((x,y)=>x-y);
  if (!a.length) return 0;
  const i = Math.floor((a.length-1)*p);
  return a[i];
}}

function renderKPI(rows) {{
  byId("k_count").textContent = rows.length;
  byId("k_mention").textContent = Math.round(rows.reduce((s,r)=>s+n(r["近7天提及文章数"]),0));
  byId("k_mention_cap").textContent = Math.round(rows.reduce((s,r)=>s+n(r["实抓近7天帖子数"]),0));
  byId("k_interact").textContent = Math.round(rows.reduce((s,r)=>s+n(r["笔记互动总量"]),0));
  const p75 = percentile(rows.map(r=>n(r["自选人数"])), 0.75);
  byId("k_focus").textContent = rows.filter(r=>n(r["自选人数"])>=p75 && p75>0).length;
}}

function colorByDirection(name) {{
  const palette = ["#2563eb","#16a34a","#d97706","#db2777","#0d9488","#7c3aed","#dc2626","#0891b2","#4f46e5","#ca8a04"];
  const keys = META.direction_list || [];
  const idx = Math.max(0, keys.indexOf(name));
  return palette[idx % palette.length];
}}

function renderLegend(rows) {{
  const host = byId("dir_legend");
  const uniq = new Set(rows.map(r => (r["投资方向标签"] || "未分类")));
  const ordered = (META.direction_list || []).filter(x => uniq.has(x));
  for (const x of uniq) {{
    if (!ordered.includes(x)) ordered.push(x);
  }}
  host.innerHTML = ordered.map(x => {{
    const col = colorByDirection(x);
    return `<span class="legend-item"><span class="legend-dot" style="background:${{col}};border:1px solid ${{col}};"></span>${{x}}</span>`;
  }}).join("");
}}

function renderScatter(rows) {{
  const svg = byId("scatter");
  const W = svg.clientWidth || 800, H = svg.clientHeight || 420;
  const pad = 46;
  const xs = rows.map(r=>n(r["本周浏览人数"])); const ys = rows.map(r=>n(r["关转比"])); const ss = rows.map(r=>n(r["自选人数"]));
  const xmin = 0, xmax = Math.max(1, ...xs); const ymin = 0, ymax = Math.max(0.5, ...ys);
  const smin = Math.max(1, Math.min(...ss, 1)); const smax = Math.max(smin, ...ss);
  const sx = v => pad + (v-xmin)/(xmax-xmin||1)*(W-pad*2);
  const sy = v => H-pad - (v-ymin)/(ymax-ymin||1)*(H-pad*2);
  const sr = v => 5 + (v-smin)/(smax-smin||1)*15;

  let html = `<rect x="0" y="0" width="${{W}}" height="${{H}}" fill="#fff"/>`;
  html += `<line x1="${{pad}}" y1="${{H-pad}}" x2="${{W-pad}}" y2="${{H-pad}}" stroke="#cbd5e1"/>`;
  html += `<line x1="${{pad}}" y1="${{pad}}" x2="${{pad}}" y2="${{H-pad}}" stroke="#cbd5e1"/>`;
  html += `<text x="${{W/2}}" y="${{H-10}}" text-anchor="middle" fill="#64748b" font-size="12">本周浏览人数</text>`;
  html += `<text x="14" y="${{H/2}}" transform="rotate(-90 14 ${{H/2}})" text-anchor="middle" fill="#64748b" font-size="12">关转比</text>`;
  for (const r of rows.slice(0, 300)) {{
    const cx = sx(n(r["本周浏览人数"])), cy = sy(n(r["关转比"])), rr = sr(n(r["自选人数"]));
    const col = colorByDirection(r["投资方向标签"]);
    const tip = `${{r["基金名称"]}} (${{r["基金代码"]}})\\n投资方向:${{r["投资方向标签"]}}\\n浏览:${{Math.round(n(r["本周浏览人数"]))}} 关转比:${{fmt(n(r["关转比"]))}} 自选:${{Math.round(n(r["自选人数"]))}}`;
    html += `<circle cx="${{cx}}" cy="${{cy}}" r="${{rr}}" fill="${{col}}" fill-opacity="0.38" stroke="${{col}}"><title>${{tip}}</title></circle>`;
  }}
  svg.innerHTML = html;
}}

function topList(rows, key, el, fmtFn) {{
  const arr = [...rows].sort((a,b)=>n(b[key])-n(a[key])).slice(0,10);
  byId(el).innerHTML = arr.map(r=>`<li>${{r["基金名称"]}}（${{r["基金代码"]}}）${{fmtFn(r)}}</li>`).join("");
}}

function renderTableA(rows) {{
  const arr = [...rows].sort((a,b)=>n(b["关转比"])-n(a["关转比"]));
  byId("tb_a").innerHTML = arr.slice(0, 300).map(r => `<tr>
    <td>${{r["基金名称"]||""}}</td><td>${{r["基金代码"]||""}}</td><td>${{r["投资方向标签"]||""}}</td>
    <td>${{Math.round(n(r["本周浏览人数"]))}}</td><td>${{Math.round(n(r["自选人数"]))}}</td><td>${{Math.round(n(r["持有人数"]))}}</td>
    <td>${{fmt(n(r["曝关比"]))}}</td><td>${{fmt(n(r["关转比"]))}}</td>
    <td>${{Math.round(n(r["近7天提及文章数"]))}}</td><td>${{Math.round(n(r["实抓近7天帖子数"]))}}</td><td>${{Math.round(n(r["笔记互动总量"]))}}</td><td>${{fmt(n(r["笔记平均互动率"]))}}</td>
  </tr>`).join("");
}}

function renderTrend() {{
  const svg = byId("trend");
  const W = svg.clientWidth || 700, H = svg.clientHeight || 260, pad=34;
  const ts = (META.trend || []).filter(x=>x["日"]);
  if (!ts.length) {{ svg.innerHTML = ""; return; }}
  const xs = ts.map((_,i)=>i);
  const y1 = ts.map(x=>n(x["发帖数"])), y2 = ts.map(x=>n(x["互动量"]));
  const ymax = Math.max(1, ...y1, ...y2);
  const sx = i => pad + i/(Math.max(1,xs.length-1))*(W-pad*2);
  const sy = v => H-pad - v/ymax*(H-pad*2);
  const p1 = xs.map((i,idx)=>`${{sx(i)}},${{sy(y1[idx])}}`).join(" ");
  const p2 = xs.map((i,idx)=>`${{sx(i)}},${{sy(y2[idx])}}`).join(" ");
  let html = `<rect x="0" y="0" width="${{W}}" height="${{H}}" fill="#fff"/>`;
  html += `<polyline points="${{p1}}" fill="none" stroke="#2563eb" stroke-width="2"/><polyline points="${{p2}}" fill="none" stroke="#ef4444" stroke-width="2"/>`;
  html += `<text x="12" y="20" fill="#2563eb" font-size="12">发帖数</text><text x="70" y="20" fill="#ef4444" font-size="12">互动量</text>`;
  svg.innerHTML = html;
}}

function tier(row) {{
  const e = n(row["跟踪误差(%)"]), l = n(row["第一梯队阈值(%)"]), h = n(row["第二梯队阈值(%)"]);
  if (!e) return "未披露";
  if (l && e <= l) return "第一梯队";
  if (h && e <= h) return "第二梯队";
  if (h && e > h) return "偏弱";
  return "未分层";
}}

function renderTableB(rows) {{
  const arr = rows.filter(r=>r["跟踪指数名称"] || n(r["跟踪误差(%)"])>0).map(r=>({{...r, _tier:tier(r)}}));
  byId("tb_b").innerHTML = arr.slice(0, 300).map(r=>`<tr>
    <td>${{r["基金名称"]||""}}</td><td>${{r["基金代码"]||""}}</td><td>${{r["投资方向标签"]||""}}</td><td>${{r["跟踪指数名称"]||""}}</td>
    <td>${{fmt(n(r["跟踪指数收益率(%)"]))}}</td><td>${{fmt(n(r["跟踪误差(%)"]))}}</td>
    <td>${{fmt(n(r["第一梯队阈值(%)"]))}}</td><td>${{fmt(n(r["第二梯队阈值(%)"]))}}</td>
    <td>${{r["跟踪评价标题"]||""}} ${{r["跟踪评价说明"]||""}}</td><td>${{r["_tier"]}}</td>
  </tr>`).join("");
  const c = {{"第一梯队":0,"第二梯队":0,"偏弱":0,"未披露":0,"未分层":0}};
  for (const r of arr) c[r._tier] = (c[r._tier]||0)+1;
  byId("tier_stats").innerHTML = Object.entries(c).map(([k,v])=>`<li>${{k}}：${{v}} 只</li>`).join("");
}}

function render() {{
  const rows = filtered();
  byId("scope_note").textContent = `当前口径：快照=${{META.snapshot || '-'}}；榜单=${{byId("board").value || "全部"}}；投资方向=${{dirSel.value || "全部"}}；检索=${{(byId("q").value||'').trim() || "无"}}；动态样本截止=${{META.note_latest_time || "-"}}`;
  renderKPI(rows);
  renderScatter(rows);
  renderLegend(rows);
  topList(rows, "关转比", "top_ctr", r => `｜关转比 ${{fmt(n(r["关转比"]))}}`);
  topList(rows, "曝关比", "top_exp", r => `｜曝关比 ${{fmt(n(r["曝关比"]))}}`);
  topList(rows, "近7天提及文章数", "top_note", r => `｜近7天提及(平台) ${{Math.round(n(r["近7天提及文章数"]))}}`);
  topList(rows, "实抓近7天帖子数", "top_note_cap", r => `｜近7天提及(实抓) ${{Math.round(n(r["实抓近7天帖子数"]))}}`);
  renderTableA(rows);
  renderTrend();
  renderTableB(rows);
}}

[dirSel, byId("board"), byId("q")].forEach(el => el.addEventListener("input", render));
themeSel.addEventListener("input", () => applyTheme(themeSel.value));
try {{
  const savedTheme = localStorage.getItem('dashboard_theme') || localStorage.getItem('core_theme') || 'org';
  if (THEMES[savedTheme]) themeSel.value = savedTheme;
  applyTheme(themeSel.value);
}} catch(e) {{ applyTheme('org'); }}
render();
</script>
</body>
</html>"""


def main() -> int:
    parser = argparse.ArgumentParser(description="Build fund detail operations dashboard.")
    parser.add_argument("--input", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()

    in_path = Path(args.input)
    out_path = Path(args.output)
    if not in_path.exists():
        raise SystemExit(f"Input not found: {in_path}")

    rows, notes, meta = load_data(in_path)
    out_path.write_text(build_html(rows, notes, meta), encoding="utf-8")
    print(out_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

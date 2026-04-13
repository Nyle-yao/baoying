#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

import pandas as pd

DETAIL_RAW_JSON = Path("/Users/yaoruanxingchen/c/exports/addsub/基金详情抓取_20260413_raw.json")


def to_num(v: Any) -> float | None:
    try:
        if v is None:
            return None
        x = float(v)
        if pd.isna(x):
            return None
        return x
    except Exception:
        return None


def norm_return_pct(v: Any) -> float | None:
    x = to_num(v)
    if x is None:
        return None
    if abs(x) > 100:
        return x / 10000.0
    return x


def norm_code(v: Any) -> str:
    if v is None:
        return ""
    s = str(v).strip()
    if s.endswith(".0"):
        s = s[:-2]
    return s.zfill(6) if s.isdigit() else s


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


def load_rows(workbook: Path, detail_raw_json: Path) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    invest_map = load_invest_direction_map(detail_raw_json)
    rows: list[dict[str, Any]] = []
    for sheet, board in [("加仓榜", "加仓"), ("减仓榜", "减仓")]:
        df = pd.read_excel(workbook, sheet_name=sheet)
        for _, r in df.iterrows():
            dt = pd.to_datetime(r.get("统计日期"), errors="coerce")
            if pd.isna(dt):
                continue
            rows.append(
                {
                    "date": dt.strftime("%Y-%m-%d"),
                    "board": board,
                    "type": str(r.get("基金范围") or ""),
                    "fund_name": str(r.get("基金简称") or ""),
                    "fund_code": norm_code(r.get("基金代码")),
                    "invest_direction": invest_map.get(norm_code(r.get("基金代码")), ""),
                    "fund_category": str(r.get("基金类型") or ""),
                    "rank": to_num(r.get("榜单名次")),
                    "day_ret": norm_return_pct(r.get("日涨跌幅(%)")),
                    "month_ret": norm_return_pct(r.get("近1月涨跌幅(%)")),
                }
            )
    dates = sorted({r["date"] for r in rows})
    meta = {"latest_date": dates[-1] if dates else ""}
    return rows, meta


def build_html(rows: list[dict[str, Any]], meta: dict[str, Any]) -> str:
    payload = json.dumps(rows, ensure_ascii=False)
    meta_json = json.dumps(meta, ensure_ascii=False)
    return f"""<!doctype html>
<html lang=\"zh-CN\">
<head>
  <meta charset=\"utf-8\" />
  <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\" />
  <title>运营指标执行看板</title>
  <style>
    :root {{ --bg:#f6f8fb; --text:#1f2937; --card:#fff; --line:#e5e7eb; --muted:#64748b; --accent:#111827; --thead:#f8fafc; --soft:#cbd5e1; --tagbg:#eef2ff; --tagtext:#3730a3; }}
    body {{ margin:0; font-family:"PingFang SC","Microsoft YaHei",sans-serif; background:var(--bg); color:var(--text); }}
    .wrap {{ max-width:1680px; margin:0 auto; padding:16px; }}
    .card {{ background:var(--card); border:1px solid var(--line); border-radius:12px; padding:12px; margin-bottom:12px; }}
    .tabs button {{ margin-right:8px; border:1px solid var(--soft); background:var(--card); border-radius:8px; padding:6px 12px; cursor:pointer; color:var(--text); }}
    .tabs .on {{ background:var(--accent); color:#fff; border-color:var(--accent); }}
    .filters {{ display:grid; grid-template-columns:180px 180px 180px 280px; gap:8px; margin-top:8px; }}
    select,input {{ width:100%; padding:8px; border:1px solid var(--soft); border-radius:8px; background:var(--card); color:var(--text); }}
    .tiny {{ font-size:12px; color:var(--muted); margin-top:6px; }}
    .style-row {{ display:grid; grid-template-columns:220px 1fr; gap:8px; margin-top:8px; }}
    .kpis {{ display:grid; grid-template-columns:repeat(4,1fr); gap:8px; margin-top:10px; }}
    .kpi {{ background:var(--thead); border:1px solid var(--line); border-radius:8px; padding:8px; }}
    .kpi .v {{ font-size:22px; font-weight:700; }}
    .area {{ max-height:72vh; overflow:auto; border:1px solid var(--line); border-radius:10px; }}
    .area-sm {{ max-height:48vh; overflow:auto; border:1px solid var(--line); border-radius:10px; margin-top:12px; }}
    table {{ width:100%; border-collapse:collapse; font-size:13px; }}
    th,td {{ border-bottom:1px solid var(--line); text-align:left; padding:8px; vertical-align:top; }}
    th {{ background:var(--thead); position:sticky; top:0; }}
    .tag {{ display:inline-block; border-radius:999px; background:var(--tagbg); color:var(--tagtext); padding:2px 8px; font-size:12px; }}
    .muted {{ color:var(--muted); }}
    .jump-links {{ display:flex; flex-wrap:wrap; gap:8px; margin:8px 0 10px; }}
    .jump-links a {{ text-decoration:none; border:1px solid var(--soft); background:var(--card); color:var(--text); border-radius:8px; padding:6px 10px; font-size:13px; }}
    .jump-links a.on {{ background:var(--accent); color:#fff; border-color:var(--accent); }}
    .page-desc {{ font-size:13px; color:#475569; margin:2px 0 8px; }}
  </style>
</head>
<body>
<div class=\"wrap\">
  <div class=\"card\">
    <h2 style=\"margin:0\">运营指标执行看板</h2>
    <div class=\"jump-links\">
      <a href=\"/\">核心看板</a>
      <a href=\"/fund-detail-cockpit\">基金详情驾驶舱</a>
      <a class=\"on\" href=\"/ops-metrics\">动态指标看板</a>
      <a href=\"/competitor-weakness\">竞品弱点看板</a>
      <a href=\"/metrics-doc\">指标文档</a>
    </div>
    <div class=\"page-desc\">这张表用来看各项运营指标的“当前数值”，你换日期或筛选条件，数值会自动更新。</div>
    <div class=\"tabs\" style=\"margin-top:8px\">
      <button id=\"tab_add\">加仓视角</button>
      <button id=\"tab_sub\">减仓视角</button>
      <button id=\"tab_both\" class=\"on\">双边视角</button>
    </div>
    <div class=\"filters\">
      <select id=\"date\"></select>
      <select id=\"type\"></select>
      <select id=\"fund_type\"></select>
      <input id=\"q\" placeholder=\"搜索指标/用途/动作\" />
    </div>
    <div class=\"style-row\">
      <select id=\"theme_sel\">
        <option value=\"org\">机构简报风</option>
        <option value=\"terminal\">交易终端风</option>
        <option value=\"research\">券商研究风</option>
        <option value=\"minimal\">极简运营风</option>
        <option value=\"news\">数据新闻风</option>
        <option value=\"industrial\">工业仪表风</option>
        <option value=\"guofeng\">国风金融风</option>
        <option value=\"tech\">科技蓝图风</option>
        <option value=\"warm\">暖色决策风</option>
        <option value=\"brand\">品牌定制风</option>
      </select>
      <div class=\"tiny\">风格切换面板：只改视觉样式，不影响数据与计算。</div>
    </div>
    <div class=\"tiny\" id=\"note\"></div>
    <div class=\"kpis\">
      <div class=\"kpi\"><div class=\"muted\">样本基金数</div><div id=\"k_n\" class=\"v\">-</div></div>
      <div class=\"kpi\"><div class=\"muted\">净偏好值(加-减)</div><div id=\"k_net\" class=\"v\">-</div></div>
      <div class=\"kpi\"><div class=\"muted\">短期热度(7日均值)</div><div id=\"k_heat\" class=\"v\">-</div></div>
      <div class=\"kpi\"><div class=\"muted\">分歧预警数</div><div id=\"k_div\" class=\"v\">-</div></div>
    </div>
  </div>

  <div class=\"card area\">
    <table>
      <thead>
        <tr>
          <th style=\"min-width:120px\">分类</th>
          <th style=\"min-width:180px\">指标名</th>
          <th style=\"min-width:120px\">当前值</th>
          <th style=\"min-width:340px\">计算方式</th>
          <th style=\"min-width:260px\">目的</th>
          <th style=\"min-width:320px\">运营动作建议</th>
        </tr>
      </thead>
      <tbody id=\"tb\"></tbody>
    </table>
  </div>

  <div class=\"card area-sm\">
    <table>
      <thead>
        <tr>
          <th>基金名称</th>
          <th>基金代码</th>
          <th>投资方向</th>
          <th>类型</th>
          <th>近7日上榜次数</th>
          <th>近2周上榜次数</th>
          <th>近1月上榜次数</th>
        </tr>
      </thead>
      <tbody id=\"tb_fund\"></tbody>
    </table>
  </div>
</div>

<script>
const DATA = {payload};
const META = {meta_json};
let mode = 'both';
const n = v => {{ const x=Number(v); return Number.isFinite(x)?x:0; }};
const avg = arr => arr.length ? (arr.reduce((s,x)=>s+n(x),0)/arr.length) : 0;
const compound = rs => {{ let acc=1; for (const r of rs) acc *= (1+n(r)/100); return (acc-1)*100; }};
const mm = (v, lo, hi) => (hi<=lo?0:(v-lo)/(hi-lo));

const dates = [...new Set(DATA.map(r=>r.date).filter(Boolean))].sort((a,b)=>a>b?-1:1);
const dateSel = document.getElementById('date');
const typeSel = document.getElementById('type');
const fundTypeSel = document.getElementById('fund_type');
const qInput = document.getElementById('q');
const themeSel = document.getElementById('theme_sel');
const tb = document.getElementById('tb');
const tbFund = document.getElementById('tb_fund');
const note = document.getElementById('note');
const THEMES = {{
  org:       {{bg:'#f5f7fb',text:'#1f2937',card:'#ffffff',line:'#e5e7eb',muted:'#64748b',accent:'#111827',thead:'#f8fafc',soft:'#cbd5e1',tagbg:'#eef2ff',tagtext:'#3730a3'}},
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

dateSel.innerHTML = dates.map(d => `<option value="${{d}}">${{d}}</option>`).join('');
const types = [...new Set(DATA.map(r=>r.type).filter(Boolean))].sort();
typeSel.innerHTML = '<option value="">全部类型</option>' + types.map(t=>`<option>${{t}}</option>`).join('');
const fts = [...new Set(DATA.map(r=>r.fund_category).filter(Boolean))].sort();
fundTypeSel.innerHTML = '<option value="">全部基金类型</option>' + fts.map(t=>`<option>${{t}}</option>`).join('');

function setMode(m) {{
  mode = m;
  ['add','sub','both'].forEach(k=>document.getElementById('tab_'+k).classList.remove('on'));
  document.getElementById('tab_'+m).classList.add('on');
  render();
}}

document.getElementById('tab_add').onclick = () => setMode('add');
document.getElementById('tab_sub').onclick = () => setMode('sub');
document.getElementById('tab_both').onclick = () => setMode('both');

function buildAgg(modeX) {{
  const endDate = dateSel.value || META.latest_date;
  const st = typeSel.value;
  const sft = fundTypeSel.value;
  const hist = dates.filter(d=>d<=endDate).sort();
  const dset = new Set(hist);
  const idx = hist.length-1;
  const w30 = new Set(idx>=0?hist.slice(Math.max(0,idx-29),idx+1):[]);
  const w14 = new Set(idx>=0?hist.slice(Math.max(0,idx-13),idx+1):[]);
  const w7 = new Set(idx>=0?hist.slice(Math.max(0,idx-6),idx+1):[]);

  const raw = DATA.filter(r => {{
    if (!dset.has(r.date)) return false;
    if (st && r.type!==st) return false;
    if (sft && r.fund_category!==sft) return false;
    if (modeX==='add' && r.board!=='加仓') return false;
    if (modeX==='sub' && r.board!=='减仓') return false;
    return true;
  }});

  const mp = new Map();
  for (const r of raw) {{
    const k = r.fund_code || r.fund_name;
    if (!k) continue;
    if (!mp.has(k)) mp.set(k, {{name:r.fund_name||'', code:r.fund_code||'', invest_direction:r.invest_direction||'', type:r.type||'', d7:new Set(), d14:new Set(), d30:new Set(), dayByDate:{{}}, boardSeen:new Set(), latestMonth:null, latestDay:null, latestDate:''}});
    const o = mp.get(k);
    const dk = modeX==='both' ? `${{r.date}}|${{r.board}}` : r.date;
    if (w7.has(r.date)) o.d7.add(dk);
    if (w14.has(r.date)) o.d14.add(dk);
    if (w30.has(r.date)) o.d30.add(dk);
    o.boardSeen.add(r.board);
    if (!o.dayByDate[r.date]) o.dayByDate[r.date] = [];
    o.dayByDate[r.date].push(r.day_ret);
    if (!o.latestDate || r.date > o.latestDate) {{ o.latestDate = r.date; o.latestMonth = r.month_ret; o.latestDay = r.day_ret; }}
  }}

  const out = [];
  for (const [,o] of mp) {{
    if (modeX==='both' && o.boardSeen.size<2) continue;
    const d14 = [...w14].sort().flatMap(d => (o.dayByDate[d]||[]).slice(0,1));
    const d7 = [...w7].sort().flatMap(d => (o.dayByDate[d]||[]).slice(0,1));
    out.push({{name:o.name, code:o.code, invest_direction:o.invest_direction||'', type:o.type||'', m7:o.d7.size, m14:o.d14.size, m30:o.d30.size, r7:d7.length?compound(d7):null, r14:d14.length?compound(d14):null, r30:o.latestMonth, r1:o.latestDay}});
  }}
  return out;
}}

function metricRows() {{
  const rows = buildAgg(mode);
  const addRows = buildAgg('add');
  const subRows = buildAgg('sub');

  const a7 = rows.map(r=>r.m7), a14=rows.map(r=>r.m14), a30=rows.map(r=>r.m30);
  const lo7=Math.min(...a7,0), hi7=Math.max(...a7,0), lo14=Math.min(...a14,0), hi14=Math.max(...a14,0), lo30=Math.min(...a30,0), hi30=Math.max(...a30,0);
  const scoreAvg = avg(rows.map(r => 100*(0.5*mm(r.m7,lo7,hi7)+0.3*mm(r.m14,lo14,hi14)+0.2*mm(r.m30,lo30,hi30))));

  const divWarn = buildAgg('both').filter(r=>r.m14>=8).length;
  const net = addRows.length - subRows.length;
  const heat = avg(rows.map(r=>r.m7));
  const trend = avg(rows.map(r=>(r.m14+r.m30)/2));
  const r7 = avg(rows.map(r=>r.r7));
  const r14 = avg(rows.map(r=>r.r14));
  const r30 = avg(rows.map(r=>r.r30));
  const r1 = avg(rows.map(r=>r.r1));

  return [
    {{c:'风向核心', n:'近7日上榜次数（均值）', v:heat.toFixed(2), f:'截至所选日期，过去7天上榜次数', p:'识别短期热点', a:'短期内容选题与快投放'}},
    {{c:'风向核心', n:'近2周上榜次数（均值）', v:avg(rows.map(r=>r.m14)).toFixed(2), f:'截至所选日期，过去14天上榜次数', p:'判断热度延续', a:'是否进入持续投放池'}},
    {{c:'风向核心', n:'近1月上榜次数（均值）', v:avg(rows.map(r=>r.m30)).toFixed(2), f:'截至所选日期，过去30天上榜次数', p:'判断中期稳定关注', a:'重点产品池优先级'}},
    {{c:'风向核心', n:'风向分（均值）', v:scoreAvg.toFixed(1), f:'100*(0.5*7日标准分+0.3*14日标准分+0.2*30日标准分)', p:'综合热度排序', a:'减少人工多列判断'}},
    {{c:'风向核心', n:'净偏好值（加-减）', v:String(net), f:'加仓样本数-减仓样本数', p:'判断偏多/偏空情绪', a:'决定当天策略偏进攻或防守'}},
    {{c:'风向核心', n:'短期热度（7日均值）', v:heat.toFixed(2), f:'当前筛选基金近7日上榜次数均值', p:'看近期活跃程度', a:'决定短期资源强度'}},
    {{c:'风向核心', n:'趋势延续（14/30均值）', v:trend.toFixed(2), f:'当前筛选基金((14日+30日)/2)均值', p:'看趋势稳定性', a:'决定是否栏目化运营'}},
    {{c:'风向核心', n:'分歧预警数', v:String(divWarn), f:'双边视角下满足阈值基金数（例14日>=8）', p:'识别高争议标的', a:'提前风控与舆情准备'}},

    {{c:'收益验证', n:'近1周涨跌幅（均值%）', v:r7.toFixed(2), f:'近7日日涨跌幅复利合成', p:'验证短期热度是否有收益支撑', a:'热高收益弱时降推荐强度'}},
    {{c:'收益验证', n:'近2周涨跌幅（均值%）', v:r14.toFixed(2), f:'近14日日涨跌幅复利合成', p:'验证中短期趋势质量', a:'筛选可持续讲故事产品'}},
    {{c:'收益验证', n:'近1月涨跌幅（均值%）', v:r30.toFixed(2), f:'接口monthInc最新值（或30日复利统一）', p:'验证中期收益匹配', a:'结合热度决定主推池'}},
    {{c:'收益验证', n:'日涨跌幅（均值%）', v:r1.toFixed(2), f:'截至日当日涨跌幅字段', p:'解释当天排名变化', a:'用于日报快讯标题'}},

    {{c:'信号与预警', n:'短期热点标签占比', v:(100*rows.filter(r=>r.m7>=5 && r.m14<8).length/Math.max(rows.length,1)).toFixed(1)+'%', f:'近7日高且14/30未同步高', p:'识别新热点', a:'快速跟进内容'}},
    {{c:'信号与预警', n:'趋势延续标签占比', v:(100*rows.filter(r=>r.m14>=8 && r.m30>=12).length/Math.max(rows.length,1)).toFixed(1)+'%', f:'14日与30日都高', p:'识别稳定热度', a:'持续投放与专题化'}},
    {{c:'信号与预警', n:'分歧预警标签占比', v:(100*buildAgg('both').filter(r=>r.m14>=8).length/Math.max(buildAgg('both').length,1)).toFixed(1)+'%', f:'双边活跃高', p:'识别高争议', a:'强化风险提示'}},
    {{c:'信号与预警', n:'退潮观察标签占比', v:(100*rows.filter(r=>r.m7+2<r.m14).length/Math.max(rows.length,1)).toFixed(1)+'%', f:'7日显著弱于14日', p:'识别退潮', a:'降权减少资源占用'}},

    {{c:'口径与时效', n:'样本基金数', v:String(rows.length), f:'当前筛选+当前视角基金数量', p:'评估统计稳定性', a:'样本过低时谨慎决策'}},
    {{c:'口径与时效', n:'统计口径提示', v:`截至${{dateSel.value||META.latest_date}}`, f:'单日期选择 + 滚动7/14/30日', p:'防止口径误解', a:'汇报时明确更新截至日'}},
  ];
}}

function render() {{
  const list = metricRows();
  document.getElementById('k_n').textContent = list.find(x=>x.n==='样本基金数').v;
  document.getElementById('k_net').textContent = list.find(x=>x.n==='净偏好值（加-减）').v;
  document.getElementById('k_heat').textContent = list.find(x=>x.n==='短期热度（7日均值）').v;
  document.getElementById('k_div').textContent = list.find(x=>x.n==='分歧预警数').v;
  note.textContent = `截至${{dateSel.value||META.latest_date}}，滚动统计7/14/30日（视角：${{mode==='add'?'加仓':mode==='sub'?'减仓':'双边'}}）`;

  const q = qInput.value.trim();
  const rows = list.filter(x => !q || [x.c,x.n,x.f,x.p,x.a].join(' ').includes(q));
  tb.innerHTML = rows.map(x => `<tr>
    <td><span class="tag">${{x.c}}</span></td>
    <td><strong>${{x.n}}</strong></td>
    <td>${{x.v}}</td>
    <td>${{x.f}}</td>
    <td>${{x.p}}</td>
    <td class="muted">${{x.a}}</td>
  </tr>`).join('');

  const funds = buildAgg(mode).sort((a,b)=> (n(b.m7)-n(a.m7)) || (n(b.m14)-n(a.m14)) || (n(b.m30)-n(a.m30)) || (a.code>b.code?1:-1));
  tbFund.innerHTML = funds.slice(0, 120).map(r => `<tr>
    <td>${{r.name||''}}</td>
    <td>${{r.code||''}}</td>
    <td>${{r.invest_direction||''}}</td>
    <td>${{r.type||''}}</td>
    <td>${{r.m7}}</td>
    <td>${{r.m14}}</td>
    <td>${{r.m30}}</td>
  </tr>`).join('');
}}

[dateSel, typeSel, fundTypeSel, qInput].forEach(el => el.addEventListener('input', render));
themeSel.addEventListener('input', () => applyTheme(themeSel.value));
try {{
  const savedTheme = localStorage.getItem('dashboard_theme') || localStorage.getItem('core_theme') || 'org';
  if (THEMES[savedTheme]) themeSel.value = savedTheme;
  applyTheme(themeSel.value);
}} catch(e) {{ applyTheme('org'); }}
setMode('both');
</script>
</body>
</html>
"""


def main() -> int:
    parser = argparse.ArgumentParser(description="Build live ops metrics dashboard")
    parser.add_argument("--input", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--detail-raw-json", default=str(DETAIL_RAW_JSON))
    args = parser.parse_args()
    rows, meta = load_rows(Path(args.input), Path(args.detail_raw_json))
    Path(args.output).write_text(build_html(rows, meta), encoding="utf-8")
    print(args.output)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

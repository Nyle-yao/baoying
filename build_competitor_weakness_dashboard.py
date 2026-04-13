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


def read_sheet(path: Path, sheet: str, board: str, invest_map: dict[str, str]) -> list[dict[str, Any]]:
    df = pd.read_excel(path, sheet_name=sheet)
    out: list[dict[str, Any]] = []
    for _, r in df.iterrows():
        dt = pd.to_datetime(r.get("统计日期"), errors="coerce")
        if pd.isna(dt):
            continue
        out.append(
            {
                "date": dt.strftime("%Y-%m-%d"),
                "board": board,
                "type": str(r.get("基金范围") or ""),
                "fund_name": str(r.get("基金简称") or ""),
                "fund_code": norm_code(r.get("基金代码")),
                "invest_direction": invest_map.get(norm_code(r.get("基金代码")), ""),
                "fund_category": str(r.get("基金类型") or ""),
                "company_name": str(r.get("基金公司名称") or ""),
                "rank": to_num(r.get("榜单名次")),
                "day_ret": to_num(r.get("日涨跌幅(%)")),
                "month_ret": to_num(r.get("近1月涨跌幅(%)")),
                "year_ret": to_num(r.get("近1年涨跌幅(%)")),
                "on_rank_7d": to_num(r.get("近7日上榜天数")),
                "consecutive_day": to_num(r.get("连续上榜天数")),
                "rank_change": to_num(r.get("名次变动")),
                "update_time": str(r.get("数据更新时间") or ""),
            }
        )
    return out


def load_rows(path: Path, detail_raw_json: Path) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    invest_map = load_invest_direction_map(detail_raw_json)
    rows = read_sheet(path, "加仓榜", "加仓", invest_map) + read_sheet(path, "减仓榜", "减仓", invest_map)
    dates = sorted({r["date"] for r in rows})
    meta = {
        "latest_date": dates[-1] if dates else "",
        "start_date": dates[0] if dates else "",
        "end_date": dates[-1] if dates else "",
    }
    return rows, meta


def build_html(rows: list[dict[str, Any]], meta: dict[str, Any]) -> str:
    payload = json.dumps(rows, ensure_ascii=False)
    meta_json = json.dumps(meta, ensure_ascii=False)

    html = """<!doctype html>
<html lang=\"zh-CN\">
<head>
  <meta charset=\"utf-8\" />
  <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\" />
  <title>竞品弱点雷达附表</title>
  <style>
    body { margin:0; font-family:"PingFang SC","Microsoft YaHei",sans-serif; background:#f6f8fb; color:#1f2937; }
    .wrap { max-width:1650px; margin:0 auto; padding:16px; }
    .card { background:#fff; border-radius:10px; box-shadow:0 1px 6px rgba(0,0,0,.08); padding:12px; margin-bottom:12px; }
    .tabs button { margin-right:8px; border:1px solid #cbd5e1; background:#fff; border-radius:8px; padding:6px 12px; cursor:pointer; }
    .tabs .on { background:#111827; color:#fff; border-color:#111827; }
    .filters { display:grid; grid-template-columns:180px 180px 180px 300px; gap:8px; margin-top:8px; }
    select,input { width:100%; padding:8px; border:1px solid #cbd5e1; border-radius:8px; }
    .tiny { font-size:12px; color:#64748b; margin-top:6px; }
    .kpis { display:grid; grid-template-columns:repeat(4,1fr); gap:8px; margin-top:10px; }
    .kpi { background:#f8fafc; border:1px solid #e2e8f0; border-radius:8px; padding:8px; }
    .kpi .v { font-size:22px; font-weight:700; margin-top:2px; }
    .area { max-height:70vh; overflow:auto; }
    table { width:100%; border-collapse:collapse; font-size:13px; }
    th,td { padding:8px; border-bottom:1px solid #e2e8f0; text-align:left; }
    th { background:#f8fafc; position:sticky; top:0; }
    th.sortable { cursor:pointer; user-select:none; }
    .tag { display:inline-block; border-radius:999px; padding:2px 8px; font-size:12px; }
    .t-red { background:#fee2e2; color:#991b1b; }
    .t-orange { background:#ffedd5; color:#9a3412; }
    .t-blue { background:#dbeafe; color:#1e40af; }
    .t-gray { background:#e5e7eb; color:#374151; }
    .jump-links { display:flex; flex-wrap:wrap; gap:8px; margin:8px 0 10px; }
    .jump-links a { text-decoration:none; border:1px solid #cbd5e1; background:#fff; color:#111827; border-radius:8px; padding:6px 10px; font-size:13px; }
    .jump-links a.on { background:#111827; color:#fff; border-color:#111827; }
    .page-desc { font-size:13px; color:#475569; margin:2px 0 8px; }
  </style>
</head>
<body>
<div class=\"wrap\">
  <div class=\"card\">
    <h2 style=\"margin:0 0 8px;\">竞品弱点雷达附表</h2>
    <div class=\"jump-links\">
      <a href=\"/\">核心看板</a>
      <a href=\"/fund-detail-cockpit\">基金详情驾驶舱</a>
      <a href=\"/ops-metrics\">动态指标看板</a>
      <a class=\"on\" href=\"/competitor-weakness\">竞品弱点看板</a>
      <a href=\"/metrics-doc\">指标文档</a>
    </div>
    <div class=\"page-desc\">这张表用来找“对手基金哪里在变弱”，方便你挑出最值得优先突破的目标。</div>
    <div class=\"tabs\">
      <button id=\"tab_add\">加仓视角</button>
      <button id=\"tab_sub\">减仓视角</button>
      <button id=\"tab_both\" class=\"on\">双边视角</button>
    </div>
    <div class=\"filters\">
      <select id=\"date\"></select>
      <select id=\"type\"></select>
      <select id=\"fund_type\"></select>
      <input id=\"q\" placeholder=\"搜索基金名称/代码/公司\" />
    </div>
    <div id=\"note\" class=\"tiny\"></div>
    <div class=\"kpis\">
      <div class=\"kpi\"><div>高优先狙击数</div><div id=\"k_red\" class=\"v\">-</div></div>
      <div class=\"kpi\"><div>平均弱点分</div><div id=\"k_avg\" class=\"v\">-</div></div>
      <div class=\"kpi\"><div>分歧高基金数</div><div id=\"k_div\" class=\"v\">-</div></div>
      <div class=\"kpi\"><div>样本基金数</div><div id=\"k_n\" class=\"v\">-</div></div>
    </div>
  </div>

  <div class=\"card area\">
    <table>
      <thead>
        <tr>
          <th>基金名称</th>
          <th>基金代码</th>
          <th>投资方向</th>
          <th>基金公司</th>
          <th>类型</th>
          <th class=\"sortable\" data-key=\"weak_score\">综合弱点分</th>
          <th class=\"sortable\" data-key=\"heat_void\">热度空转度</th>
          <th class=\"sortable\" data-key=\"divergence\">分歧度</th>
          <th class=\"sortable\" data-key=\"decay\">衰减度</th>
          <th class=\"sortable\" data-key=\"rank_fragile\">名次脆弱度</th>
          <th class=\"sortable\" data-key=\"break_rate\">连续性断层率</th>
          <th>标签</th>
          <th>建议动作</th>
          <th class=\"sortable\" data-key=\"m7\">近7日上榜</th>
          <th class=\"sortable\" data-key=\"m14\">近2周上榜</th>
          <th class=\"sortable\" data-key=\"m30\">近1月上榜</th>
          <th class=\"sortable\" data-key=\"r7\">近1周涨跌幅(%)</th>
          <th class=\"sortable\" data-key=\"r30\">近1月涨跌幅(%)</th>
        </tr>
      </thead>
      <tbody id=\"tb\"></tbody>
    </table>
  </div>
</div>

<script>
const DATA = __PAYLOAD__;
const META = __META__;
let mode = 'both';
let sortState = { key:'weak_score', order:'desc' };

const n = v => { const x = Number(v); return Number.isFinite(x) ? x : 0; };
const avg = arr => arr.length ? arr.reduce((s,x)=>s+n(x),0)/arr.length : 0;
const compound = rs => { let acc = 1; for (const r of rs) acc *= (1 + n(r)/100); return (acc-1)*100; };
const clip = (x, lo, hi) => Math.max(lo, Math.min(hi, x));
const std = arr => {
  const a = arr.map(n).filter(x => Number.isFinite(x));
  if (!a.length) return 0;
  const m = avg(a);
  const v = avg(a.map(x => (x-m)*(x-m)));
  return Math.sqrt(v);
};

const dateSel = document.getElementById('date');
const typeSel = document.getElementById('type');
const fundTypeSel = document.getElementById('fund_type');
const qInput = document.getElementById('q');
const tb = document.getElementById('tb');
const note = document.getElementById('note');

const dates = [...new Set(DATA.map(r=>r.date).filter(Boolean))].sort((a,b)=>a>b?-1:1);
dateSel.innerHTML = dates.map(d => `<option value="${d}">${d}</option>`).join('');

const types = [...new Set(DATA.map(r=>r.type).filter(Boolean))].sort();
typeSel.innerHTML = '<option value="">全部类型</option>' + types.map(t=>`<option>${t}</option>`).join('');

const ftypes = [...new Set(DATA.map(r=>r.fund_category).filter(Boolean))].sort();
fundTypeSel.innerHTML = '<option value="">全部基金类型</option>' + ftypes.map(t=>`<option>${t}</option>`).join('');

function setMode(m) {
  mode = m;
  ['add','sub','both'].forEach(k => document.getElementById('tab_'+k).classList.remove('on'));
  document.getElementById('tab_'+m).classList.add('on');
  render();
}

document.getElementById('tab_add').onclick = () => setMode('add');
document.getElementById('tab_sub').onclick = () => setMode('sub');
document.getElementById('tab_both').onclick = () => setMode('both');

function buildRows() {
  const endDate = dateSel.value || META.latest_date;
  const selectedType = typeSel.value;
  const selectedFundType = fundTypeSel.value;
  const q = qInput.value.trim();

  const histDates = dates.filter(d => d <= endDate).sort();
  const dateSet = new Set(histDates);
  const endIdx = histDates.length - 1;
  const w30 = new Set(endIdx>=0 ? histDates.slice(Math.max(0,endIdx-29), endIdx+1) : []);
  const w14 = new Set(endIdx>=0 ? histDates.slice(Math.max(0,endIdx-13), endIdx+1) : []);
  const w7  = new Set(endIdx>=0 ? histDates.slice(Math.max(0,endIdx-6), endIdx+1) : []);

  const raw = DATA.filter(r => {
    if (!dateSet.has(r.date)) return false;
    if (selectedType && r.type !== selectedType) return false;
    if (selectedFundType && r.fund_category !== selectedFundType) return false;
    if (q && !(String(r.fund_name||'').includes(q) || String(r.fund_code||'').includes(q) || String(r.company_name||'').includes(q))) return false;
    if (mode === 'add' && r.board !== '加仓') return false;
    if (mode === 'sub' && r.board !== '减仓') return false;
    return true;
  });

  const mp = new Map();
  for (const r of raw) {
    const key = r.fund_code || r.fund_name;
    if (!key) continue;
    if (!mp.has(key)) {
      mp.set(key, {
        fund_name: r.fund_name || '',
        fund_code: r.fund_code || '',
        invest_direction: r.invest_direction || '',
        company_name: r.company_name || '',
        type: r.type || '',
        add7: new Set(), sub7: new Set(),
        d7: new Set(), d14: new Set(), d30: new Set(),
        appear30: new Set(),
        rank7: [],
        dayByDate: {},
        latestDate: '', latestMonth: null,
      });
    }
    const o = mp.get(key);
    const dk = mode === 'both' ? `${r.date}|${r.board}` : r.date;
    if (w7.has(r.date)) o.d7.add(dk);
    if (w14.has(r.date)) o.d14.add(dk);
    if (w30.has(r.date)) {
      o.d30.add(dk);
      o.appear30.add(r.date);
    }
    if (w7.has(r.date) && r.rank != null) o.rank7.push(r.rank);
    if (w7.has(r.date) && r.board === '加仓') o.add7.add(r.date);
    if (w7.has(r.date) && r.board === '减仓') o.sub7.add(r.date);

    if (!o.dayByDate[r.date]) o.dayByDate[r.date] = [];
    o.dayByDate[r.date].push(r.day_ret);

    if (!o.latestDate || r.date > o.latestDate) {
      o.latestDate = r.date;
      o.latestMonth = r.month_ret;
    }
  }

  const out = [];
  for (const [,o] of mp) {
    const d14 = [...w14].sort().flatMap(d => (o.dayByDate[d] || []).slice(0,1));
    const d7  = [...w7].sort().flatMap(d => (o.dayByDate[d] || []).slice(0,1));
    const m7 = o.d7.size;
    const m14 = o.d14.size;
    const m30 = o.d30.size;

    // 1) 热度空转度：热度高但近期收益弱
    const hot = clip(m7 / 7, 0, 1);
    const r7 = d7.length ? compound(d7) : null;
    const r30 = o.latestMonth;
    const retWeak = (n(r7) <= 0 || n(r30) <= 0) ? 1 : (n(r7) <= 1 ? 0.6 : 0.2);
    const heat_void = clip(100 * hot * retWeak, 0, 100);

    // 2) 分歧度：加减仓同现强度
    const add7 = o.add7.size;
    const sub7 = o.sub7.size;
    const divergence = mode === 'both' ? clip((Math.min(add7, sub7) / Math.max(add7 + sub7, 1)) * 200, 0, 100) : 0;

    // 3) 衰减度：最近7日弱于前7日
    const prev7 = Math.max(m14 - m7, 0);
    const decay = clip((Math.max(prev7 - m7, 0) / 7) * 100, 0, 100);

    // 4) 名次脆弱度：近7日排名波动
    const rank_fragile = clip((std(o.rank7) / 15) * 100, 0, 100);

    // 5) 连续性断层率：30日内断层比率
    const ad = [...o.appear30].sort();
    let breaks = 0;
    for (let i=1;i<ad.length;i++) {
      const d0 = new Date(ad[i-1]);
      const d1 = new Date(ad[i]);
      const gap = Math.round((d1 - d0) / (24*3600*1000));
      if (gap > 1) breaks += 1;
    }
    const break_rate = ad.length > 1 ? clip((breaks / (ad.length - 1)) * 100, 0, 100) : 0;

    const weak_score = clip(0.25*heat_void + 0.20*divergence + 0.20*decay + 0.20*rank_fragile + 0.15*break_rate, 0, 100);

    let tag = '稳定观察';
    if (weak_score >= 70) tag = '高优先狙击';
    else if (weak_score >= 50) tag = '重点观察';
    else if (weak_score >= 30) tag = '跟踪观察';

    const pairs = [
      ['热度空转度', heat_void],
      ['分歧度', divergence],
      ['衰减度', decay],
      ['名次脆弱度', rank_fragile],
      ['连续性断层率', break_rate],
    ].sort((a,b)=>b[1]-a[1]);

    let action = '常规跟踪';
    if (pairs[0][0] === '热度空转度') action = '热度空转，推收益更稳替代基金';
    if (pairs[0][0] === '分歧度') action = '分歧高，做对比内容承接流量';
    if (pairs[0][0] === '衰减度') action = '热度退潮，抢关键词与入口位';
    if (pairs[0][0] === '名次脆弱度') action = '波动大，强化风险提示与替代建议';
    if (pairs[0][0] === '连续性断层率') action = '持续性弱，强调长期稳健叙事';

    out.push({
      fund_name: o.fund_name,
      fund_code: o.fund_code,
      company_name: o.company_name,
      type: o.type,
      m7, m14, m30,
      r7, r30,
      heat_void, divergence, decay, rank_fragile, break_rate,
      weak_score,
      tag,
      action,
    });
  }

  return out;
}

function render() {
  const rows = buildRows();
  const k = sortState.key;
  const ord = sortState.order === 'asc' ? 1 : -1;
  rows.sort((a,b) => (n(a[k]) - n(b[k])) * ord || (a.fund_code > b.fund_code ? 1 : -1));

  const red = rows.filter(r=>r.tag==='高优先狙击').length;
  const divN = rows.filter(r=>r.divergence>=60).length;
  document.getElementById('k_red').textContent = red;
  document.getElementById('k_avg').textContent = avg(rows.map(r=>r.weak_score)).toFixed(1);
  document.getElementById('k_div').textContent = divN;
  document.getElementById('k_n').textContent = rows.length;
  note.textContent = `截至${dateSel.value || META.latest_date}，滚动统计7/14/30日；该表用于识别对手基金弱点。`;

  const cls = (tag) => tag==='高优先狙击' ? 't-red' : (tag==='重点观察' ? 't-orange' : (tag==='跟踪观察' ? 't-blue' : 't-gray'));

  tb.innerHTML = rows.map(r => `<tr>
    <td>${r.fund_name || ''}</td>
    <td>${r.fund_code || ''}</td>
    <td>${r.invest_direction || ''}</td>
    <td>${r.company_name || ''}</td>
    <td>${r.type || ''}</td>
    <td>${r.weak_score.toFixed(1)}</td>
    <td>${r.heat_void.toFixed(1)}</td>
    <td>${r.divergence.toFixed(1)}</td>
    <td>${r.decay.toFixed(1)}</td>
    <td>${r.rank_fragile.toFixed(1)}</td>
    <td>${r.break_rate.toFixed(1)}</td>
    <td><span class="tag ${cls(r.tag)}">${r.tag}</span></td>
    <td>${r.action}</td>
    <td>${r.m7}</td>
    <td>${r.m14}</td>
    <td>${r.m30}</td>
    <td>${r.r7==null?'':r.r7.toFixed(2)}</td>
    <td>${r.r30==null?'':Number(r.r30).toFixed(2)}</td>
  </tr>`).join('');
}

[dateSel, typeSel, fundTypeSel, qInput].forEach(el => el.addEventListener('input', render));
document.querySelectorAll('th.sortable').forEach(th => {
  th.addEventListener('click', () => {
    const k = th.dataset.key;
    if (sortState.key === k) sortState.order = sortState.order === 'desc' ? 'asc' : 'desc';
    else { sortState.key = k; sortState.order = 'desc'; }
    render();
  });
});

setMode('both');
</script>
</body>
</html>
"""

    return html.replace("__PAYLOAD__", payload).replace("__META__", meta_json)


def main() -> int:
    parser = argparse.ArgumentParser(description="Build competitor weakness dashboard")
    parser.add_argument("--input", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--detail-raw-json", default=str(DETAIL_RAW_JSON))
    args = parser.parse_args()

    in_path = Path(args.input)
    out_path = Path(args.output)
    rows, meta = load_rows(in_path, Path(args.detail_raw_json))
    out_path.write_text(build_html(rows, meta), encoding="utf-8")
    print(out_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

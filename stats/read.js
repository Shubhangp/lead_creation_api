'use strict';
/**
 * Read side: stored `stats_daily` items → dashboard payloads.
 * No per-row data is ever loaded, so response time and memory do not depend
 * on how many rows the source tables hold.
 */

const core = require('./core');
const store = require('./store');
const { SPECS, BY_KEY, publicSpec } = require('./specs');

const DAY_RE = /^\d{4}-\d{2}-\d{2}$/;
const MAX_RANGE_DAYS = 800;
const WEEKDAYS = ['Sun', 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat'];

const round1 = (n) => Math.round(n * 10) / 10;

/** Group stored items by day: counters (live + backfill) and HLL registers. */
function groupByDay(items) {
  const days = new Map();
  for (const it of items) {
    const sk = it.sk || '';
    const day = sk.slice(0, 10);
    if (!DAY_RE.test(day)) continue;
    let d = days.get(day);
    if (!d) { d = { counters: {}, hll: {} }; days.set(day, d); }
    if (sk.includes('#u#')) {
      const name = sk.split('#u#')[1];
      const reg = core.toBuffer(it.R);
      if (reg) {
        if (!d.hll[name]) d.hll[name] = Buffer.from(reg);
        else core.hllMerge(d.hll[name], reg);
      }
    } else {
      core.addInto(d.counters, core.countersOf(it));
    }
  }
  return days;
}

function successOf(spec, categories) {
  return (spec.successKeys || []).reduce((s, k) => s + (categories[k] || 0), 0);
}

function evaluateAlerts(spec, s) {
  const out = [];
  for (const a of spec.alerts || []) {
    if (a.type === 'minTotal') {
      if (s.total < a.critical) out.push({ level: 'critical', msg: `Zero ${spec.displayName.toLowerCase()} received` });
      else if (s.total < a.warning) out.push({ level: 'warning', msg: `Very low count: ${s.total}` });
    } else if (a.type === 'ratio') {
      const n = s.categories[a.category] || 0;
      const r = n / (s.total || 1);
      if (r > a.critical) out.push({ level: 'critical', msg: `${a.label} >${a.critical * 100}%: ${n}/${s.total}` });
      else if (r > a.warning) out.push({ level: 'warning', msg: `${a.label} elevated: ${n}/${s.total}` });
    } else if (a.type === 'rcs') {
      const sent = s.categories.SENT || 0;
      const failed = s.categories.FAILED || 0;
      if (sent === 0) out.push({ level: 'critical', msg: 'No RCS messages sent' });
      else if (failed > sent * 0.5) out.push({ level: 'warning', msg: `High failure rate: ${failed} failed vs ${sent} sent` });
    }
  }
  return out;
}

function validDay(d) {
  return typeof d === 'string' && DAY_RE.test(d) && Number.isFinite(Date.parse(d + 'T00:00:00Z'));
}

// ─── Daily snapshot: every table, one day ────────────────────────────────────

async function snapshot(docClient, date) {
  if (!validDay(date)) throw Object.assign(new Error('date must be YYYY-MM-DD'), { status: 400 });
  const items = await store.getDayForTables(docClient, SPECS.map((s) => s.key), date);
  const byTbl = new Map();
  for (const it of items) {
    const acc = byTbl.get(it.tbl) || {};
    byTbl.set(it.tbl, core.addInto(acc, core.countersOf(it)));
  }
  const tables = SPECS.map((spec) => {
    const s = core.shape(byTbl.get(spec.key) || {});
    const success = successOf(spec, s.categories);
    return {
      key: spec.key,
      displayName: spec.displayName,
      total: s.total,
      categories: s.categories,
      bySource: s.bySource,
      success,
      successRate: s.total ? round1((success / s.total) * 100) : 0,
      metrics: s.metrics,
      alerts: evaluateAlerts(spec, s),
    };
  });
  return { date, tables, specs: SPECS.map(publicSpec), generatedAt: new Date().toISOString() };
}

// ─── Full analytics: one table, a date range ─────────────────────────────────

async function range(docClient, key, start, end) {
  const spec = BY_KEY[key];
  if (!spec) throw Object.assign(new Error(`Unknown lender: ${key}`), { status: 400 });
  if (!validDay(start) || !validDay(end)) throw Object.assign(new Error('start and end must be YYYY-MM-DD'), { status: 400 });
  if (end < start) throw Object.assign(new Error('end is before start'), { status: 400 });
  const allDays = core.daysBetween(start, end);
  if (allDays.length > MAX_RANGE_DAYS) throw Object.assign(new Error(`Range too long (max ${MAX_RANGE_DAYS} days)`), { status: 400 });

  const items = await store.queryRange(docClient, spec.key, start, end);
  const days = groupByDay(items);

  const totalCounters = {};
  const rangeHll = {};
  const daily = allDays.map((date) => {
    const d = days.get(date);
    const counters = d ? d.counters : {};
    core.addInto(totalCounters, counters);
    const s = core.shape(counters);
    const success = successOf(spec, s.categories);
    const uniques = {};
    if (d) {
      for (const [name, reg] of Object.entries(d.hll)) {
        uniques[name] = core.hllCount(reg);
        if (!rangeHll[name]) rangeHll[name] = Buffer.from(reg); else core.hllMerge(rangeHll[name], reg);
      }
    }
    return {
      date, total: s.total, categories: s.categories, sources: s.sources, success,
      successRate: s.total ? round1((success / s.total) * 100) : 0,
      metrics: s.metrics, uniques,
    };
  });

  const s = core.shape(totalCounters);
  const success = successOf(spec, s.categories);
  const activeDays = daily.filter((d) => d.total > 0);
  const peak = activeDays.reduce((p, d) => (!p || d.total > p.total ? d : p), null);

  const weekday = WEEKDAYS.map((name) => ({ name, total: 0, days: 0, avg: 0 }));
  for (const d of daily) {
    const w = weekday[new Date(d.date + 'T00:00:00Z').getUTCDay()];
    w.total += d.total; w.days += 1;
  }
  weekday.forEach((w) => { w.avg = w.days ? Math.round(w.total / w.days) : 0; });

  const uniques = {};
  for (const u of spec.uniques || []) {
    const est = rangeHll[u.key] ? core.hllCount(rangeHll[u.key]) : null;
    uniques[u.key] = {
      label: u.label,
      estimate: est === null ? null : Math.min(est, s.total),
      repeats: est === null ? null : Math.max(0, s.total - Math.min(est, s.total)),
    };
  }

  return {
    lender: spec.key,
    displayName: spec.displayName,
    spec: publicSpec(spec),
    range: { start, end, days: allDays.length, activeDays: activeDays.length },
    total: s.total,
    success,
    successRate: s.total ? round1((success / s.total) * 100) : 0,
    avgPerDay: allDays.length ? Math.round(s.total / allDays.length) : 0,
    peakDay: peak ? { date: peak.date, total: peak.total } : null,
    categories: s.categories,
    sources: s.sources,
    bySource: s.bySource,
    dims: s.dims,
    metrics: s.metrics,
    hourly: s.hourly,
    weekday,
    uniques,
    daily,
    alerts: evaluateAlerts(spec, s),
    generatedAt: new Date().toISOString(),
  };
}

// ─── All-time total (cached) ─────────────────────────────────────────────────

const allTimeCache = new Map();
const ALLTIME_TTL_MS = 5 * 60 * 1000;

async function allTime(docClient, key) {
  const spec = BY_KEY[key];
  if (!spec) throw Object.assign(new Error(`Unknown lender: ${key}`), { status: 400 });
  const hit = allTimeCache.get(key);
  if (hit && Date.now() - hit.at < ALLTIME_TTL_MS) return hit.value;
  const items = await store.queryRange(docClient, spec.key, '2000-01-01', '2999-12-31');
  const days = groupByDay(items);
  const counters = {};
  for (const d of days.values()) core.addInto(counters, d.counters);
  const s = core.shape(counters);
  const sorted = [...days.keys()].sort();
  const value = {
    lender: key, displayName: spec.displayName, total: s.total, sources: s.sources,
    categories: s.categories, success: successOf(spec, s.categories),
    firstDay: sorted[0] || null, lastDay: sorted[sorted.length - 1] || null,
  };
  allTimeCache.set(key, { at: Date.now(), value });
  return value;
}

function meta() {
  return { specs: SPECS.map(publicSpec) };
}

module.exports = { snapshot, range, allTime, meta, groupByDay, evaluateAlerts };

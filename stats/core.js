'use strict';
/**
 * Pure helpers shared by the stream Lambda, the backfill script and the API.
 * No AWS / app imports here — this file is bundled into the Lambda as-is.
 *
 * ── How a stats row is stored (table `stats_daily`) ─────────────────────────
 *   PK `tbl`  = spec key            e.g. "zype"
 *   SK `sk`   = "YYYY-MM-DD"         live counters, written by the stream Lambda (ADD)
 *             = "YYYY-MM-DD#bf"     backfill / rebuild counters (absolute PUT)
 *             = "YYYY-MM-DD#u#name" HyperLogLog registers for unique counts
 *   A day's numbers = live item + #bf item (the API adds them together).
 *
 * Counter attribute names (all Numbers):
 *   t                    total rows
 *   c|<cat>              rows per outcome category
 *   s|<src>              rows per source
 *   sc|<src>|<cat>       rows per source × category
 *   d|<dim>|<val>        rows per dimension value (e.g. d|jobType|Salaried)
 *   n|<metric>           sum of a numeric metric (e.g. n|offerAmount)
 *   k|<metric>           how many rows had that metric
 *   h|<HH>               rows per hour of day (IST)
 */

const crypto = require('crypto');

const MAX_VAL_LEN = 100;
const IST_OFFSET_MS = 330 * 60 * 1000;

// ─── value sanitising ────────────────────────────────────────────────────────

const UUID_RE = /\b[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\b/gi;
const LONG_NUM_RE = /\d{5,}/g;
const EMAIL_RE = /[^\s@]+@[^\s@]+\.[^\s@]+/g;

/** Make a value safe and low-cardinality enough to be an attribute-name segment. */
function clean(v) {
  if (v === null || v === undefined) return null;
  if (typeof v === 'object') return null;
  let s = String(v).trim();
  if (!s) return null;
  s = s.replace(/\|/g, '/').replace(/\s+/g, ' ');
  if (s.length > MAX_VAL_LEN) s = s.slice(0, MAX_VAL_LEN - 1) + '…';
  return s;
}

/** Free-text messages: also strip ids / long numbers / emails so they group. */
function cleanMessage(v) {
  if (v === null || v === undefined || typeof v === 'object') return null;
  const s = String(v).replace(UUID_RE, '<id>').replace(EMAIL_RE, '<email>').replace(LONG_NUM_RE, '#');
  return clean(s);
}

function num(v) {
  if (v === null || v === undefined || v === '') return null;
  const n = typeof v === 'number' ? v : parseFloat(String(v).replace(/[,₹\s]/g, ''));
  return Number.isFinite(n) ? n : null;
}

function dayOf(iso) {
  if (!iso || typeof iso !== 'string' || iso.length < 10) return null;
  const d = iso.slice(0, 10);
  return /^\d{4}-\d{2}-\d{2}$/.test(d) ? d : null;
}

function istHour(iso) {
  const t = Date.parse(iso);
  if (!Number.isFinite(t)) return null;
  return String(new Date(t + IST_OFFSET_MS).getUTCHours()).padStart(2, '0');
}

// ─── contribution of one row ─────────────────────────────────────────────────

/**
 * What one source row adds to the counters.
 * Returns null when the row is not counted (skipped / no date).
 * Shape: { day, attrs: { name: delta }, uniques: { name: value } }
 */
function contribution(spec, item) {
  if (!item || typeof item !== 'object') return null;
  if (spec.skip && spec.skip(item)) return null;
  const dateIso = spec.dateOf ? spec.dateOf(item) : item.createdAt;
  const day = dayOf(dateIso);
  if (!day) return null;

  const r = spec.classify(item) || {};
  const attrs = {};
  const add = (k, v = 1) => { attrs[k] = (attrs[k] || 0) + v; };

  const cat = clean(r.category) || 'other';
  const src = clean(spec.sourceOf ? spec.sourceOf(item) : item.source) || 'unknown';

  add('t');
  add(`c|${cat}`);
  add(`s|${src}`);
  add(`sc|${src}|${cat}`);

  const hh = istHour(dateIso);
  if (hh !== null) add(`h|${hh}`);

  for (const [dim, raw] of Object.entries(r.dims || {})) {
    const vals = Array.isArray(raw) ? raw : [raw];
    const seen = new Set();
    for (const v of vals) {
      const cv = clean(v);
      if (cv === null || seen.has(cv)) continue;
      seen.add(cv);
      add(`d|${dim}|${cv}`);
    }
  }

  for (const [m, raw] of Object.entries(r.metrics || {})) {
    const n = num(raw);
    if (n === null) continue;
    add(`n|${m}`, n);
    add(`k|${m}`);
  }

  const uniques = {};
  for (const u of spec.uniques || []) {
    const v = u.value(item);
    if (v !== null && v !== undefined && v !== '') uniques[u.key] = String(v);
  }

  return { day, attrs, uniques };
}

/**
 * Counter changes caused by one write (old image → new image).
 * Returns Map "day" → { attr: delta } with zero deltas removed.
 */
function diffContribution(spec, oldItem, newItem) {
  const out = new Map();
  const apply = (c, sign) => {
    if (!c) return;
    let m = out.get(c.day);
    if (!m) { m = {}; out.set(c.day, m); }
    for (const [k, v] of Object.entries(c.attrs)) m[k] = (m[k] || 0) + sign * v;
  };
  apply(oldItem ? contribution(spec, oldItem) : null, -1);
  apply(newItem ? contribution(spec, newItem) : null, +1);
  for (const [day, m] of out) {
    for (const k of Object.keys(m)) {
      // floating point sums (amounts) can leave 1e-12 crumbs
      if (Math.abs(m[k]) < 1e-9) delete m[k];
    }
    if (!Object.keys(m).length) out.delete(day);
  }
  return out;
}

function addInto(target, attrs) {
  for (const [k, v] of Object.entries(attrs)) target[k] = (target[k] || 0) + v;
  return target;
}

// ─── HyperLogLog (unique counts that merge across days) ──────────────────────

const HLL_P = 10;            // 1024 registers → ~3.3% standard error
const HLL_M = 1 << HLL_P;

function hllNew() { return Buffer.alloc(HLL_M); }

function hllAdd(reg, value) {
  const h = crypto.createHash('sha1').update(String(value)).digest();
  const hi = h.readUInt32BE(0);
  const lo = h.readUInt32BE(4);
  const idx = hi >>> (32 - HLL_P);
  // remaining 22 bits of hi + 32 bits of lo
  const restHi = (hi << HLL_P) >>> 0;
  let rank;
  if (restHi !== 0) rank = Math.clz32(restHi) + 1;
  else rank = (32 - HLL_P) + (lo !== 0 ? Math.clz32(lo) : 32) + 1;
  if (rank > reg[idx]) { reg[idx] = rank; return true; }
  return false;
}

/** Merge b into a (max per register). Returns true if a changed. */
function hllMerge(a, b) {
  let changed = false;
  for (let i = 0; i < HLL_M; i++) if (b[i] > a[i]) { a[i] = b[i]; changed = true; }
  return changed;
}

function hllCount(reg) {
  const m = HLL_M;
  const alpha = 0.7213 / (1 + 1.079 / m);
  let sum = 0, zeros = 0;
  for (let i = 0; i < m; i++) { sum += Math.pow(2, -reg[i]); if (reg[i] === 0) zeros++; }
  let e = alpha * m * m / sum;
  if (e <= 2.5 * m && zeros > 0) e = m * Math.log(m / zeros);
  return Math.round(e);
}

function toBuffer(v) {
  if (!v) return null;
  if (Buffer.isBuffer(v)) return v;
  if (v instanceof Uint8Array) return Buffer.from(v);
  if (typeof v === 'string') return Buffer.from(v, 'base64');
  return null;
}

// ─── DynamoDB stream image → plain JS ───────────────────────────────────────

function unmarshallValue(v) {
  if (v === null || v === undefined) return undefined;
  if ('S' in v) return v.S;
  if ('N' in v) return Number(v.N);
  if ('BOOL' in v) return v.BOOL;
  if ('NULL' in v) return null;
  if ('M' in v) return unmarshall(v.M);
  if ('L' in v) return v.L.map(unmarshallValue);
  if ('SS' in v) return v.SS;
  if ('NS' in v) return v.NS.map(Number);
  if ('B' in v) return v.B;
  if ('BS' in v) return v.BS;
  return undefined;
}

function unmarshall(image) {
  if (!image) return null;
  const out = {};
  for (const [k, v] of Object.entries(image)) out[k] = unmarshallValue(v);
  return out;
}

// ─── read side: stored items → API payload ──────────────────────────────────

const SYSTEM_ATTR = new Set(['tbl', 'sk', '_u', '_bfAt', 'R', 'v']);

/** Split a stored item into counters only. */
function countersOf(item) {
  const out = {};
  for (const [k, v] of Object.entries(item || {})) {
    if (SYSTEM_ATTR.has(k) || k.startsWith('_')) continue;
    if (typeof v === 'number') out[k] = v;
  }
  return out;
}

/**
 * Turn flat counters into a structured summary.
 * { total, categories, bySource:{src:{total,cat..}}, sources, dims:{dim:{val:n}},
 *   metrics:{m:{sum,count,avg}}, hourly:[24] }
 */
function shape(counters) {
  const s = { total: 0, categories: {}, sources: {}, bySource: {}, dims: {}, metrics: {}, hourly: new Array(24).fill(0) };
  const metricSum = {}, metricCnt = {};
  for (const [k, v] of Object.entries(counters)) {
    if (!v) continue;
    if (k === 't') { s.total += v; continue; }
    const i = k.indexOf('|');
    if (i < 0) continue;
    const p = k.slice(0, i);
    const rest = k.slice(i + 1);
    if (p === 'c') s.categories[rest] = (s.categories[rest] || 0) + v;
    else if (p === 's') {
      s.sources[rest] = (s.sources[rest] || 0) + v;
      (s.bySource[rest] ||= { total: 0 }).total += v;
    } else if (p === 'sc') {
      const j = rest.lastIndexOf('|');
      const src = rest.slice(0, j), cat = rest.slice(j + 1);
      const b = (s.bySource[src] ||= { total: 0 });
      b[cat] = (b[cat] || 0) + v;
    } else if (p === 'd') {
      const j = rest.indexOf('|');
      const dim = rest.slice(0, j), val = rest.slice(j + 1);
      (s.dims[dim] ||= {})[val] = ((s.dims[dim] || {})[val] || 0) + v;
    } else if (p === 'n') metricSum[rest] = (metricSum[rest] || 0) + v;
    else if (p === 'k') metricCnt[rest] = (metricCnt[rest] || 0) + v;
    else if (p === 'h') { const h = parseInt(rest, 10); if (h >= 0 && h < 24) s.hourly[h] += v; }
  }
  for (const m of new Set([...Object.keys(metricSum), ...Object.keys(metricCnt)])) {
    const sum = Math.round((metricSum[m] || 0) * 100) / 100;
    const count = metricCnt[m] || 0;
    s.metrics[m] = { sum, count, avg: count ? Math.round((sum / count) * 100) / 100 : 0 };
  }
  // drop negative/zero crumbs that can appear after deltas
  for (const d of Object.values(s.dims)) for (const k of Object.keys(d)) if (d[k] <= 0) delete d[k];
  for (const k of Object.keys(s.categories)) if (s.categories[k] <= 0) delete s.categories[k];
  return s;
}

/** Enumerate YYYY-MM-DD days from start to end inclusive. */
function daysBetween(start, end) {
  const out = [];
  const a = Date.parse(start + 'T00:00:00Z'), b = Date.parse(end + 'T00:00:00Z');
  if (!Number.isFinite(a) || !Number.isFinite(b) || b < a) return out;
  for (let t = a; t <= b; t += 86400000) out.push(new Date(t).toISOString().slice(0, 10));
  return out;
}

module.exports = {
  clean, cleanMessage, num, dayOf, istHour,
  contribution, diffContribution, addInto,
  HLL_M, hllNew, hllAdd, hllMerge, hllCount, toBuffer,
  unmarshall, countersOf, shape, daysBetween,
};

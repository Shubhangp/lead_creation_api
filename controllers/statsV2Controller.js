'use strict';
/**
 * /api/v1/unified-stats/v2/* — dashboard stats served from pre-computed daily
 * counters (`stats_daily`). Every request reads a handful of small items, so it
 * stays fast and light no matter how large the source tables grow.
 */

const { docClient } = require('../dynamodb');
const read = require('../stats/read');

function handle(fn) {
  return async (req, res) => {
    try {
      const data = await fn(req);
      return res.status(200).json({ success: true, data });
    } catch (err) {
      const status = err.status || 500;
      if (status >= 500) console.error('[stats v2]', err);
      return res.status(status).json({ success: false, message: err.message });
    }
  };
}

const todayUtc = () => new Date().toISOString().slice(0, 10);

module.exports = {
  // GET /v2/meta — table list + labels/categories/dimensions for the UI
  meta: handle(async () => read.meta()),

  // GET /v2/snapshot?date=YYYY-MM-DD — all tables for one day
  snapshot: handle(async (req) => read.snapshot(docClient, req.query.date || todayUtc())),

  // GET /v2/range?lender=zype&start=YYYY-MM-DD&end=YYYY-MM-DD — full analytics
  range: handle(async (req) => {
    const { lender, start, end } = req.query;
    if (!lender) throw Object.assign(new Error('lender is required'), { status: 400 });
    return read.range(docClient, String(lender).toLowerCase(), start, end || todayUtc());
  }),

  // GET /v2/alltime?lender=zype — all-time total + source split
  allTime: handle(async (req) => {
    if (!req.query.lender) throw Object.assign(new Error('lender is required'), { status: 400 });
    return read.allTime(docClient, String(req.query.lender).toLowerCase());
  }),
};

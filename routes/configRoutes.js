// routes/configRoutes.js
// Serves the central lender/source registry to the frontends at runtime.
// Adding a lender/source in config/registry.js reflects here with no redeploy.
const express = require('express');
const {
  LENDER_CATALOG,
  DEFAULT_LENDER_ORDER,
  RESPONSELOG_SOURCES,
  LEAD_SOURCES_DEFAULT,
  SOURCE_LISTS,
  LENDER_LISTS,
} = require('../config/registry');

const router = express.Router();

// Full registry in one shot.
router.get('/', (req, res) => {
  res.status(200).json({
    lenderLists: LENDER_LISTS,
    sourceLists: SOURCE_LISTS,
    responseLogSources: RESPONSELOG_SOURCES,
    leadSourcesDefault: LEAD_SOURCES_DEFAULT,
    defaultLenderOrder: DEFAULT_LENDER_ORDER,
    catalog: LENDER_CATALOG,
  });
});

// GET /config/lenders            -> { ...all named lists }
// GET /config/lenders?list=upload -> ["SML","FREO",...]
router.get('/lenders', (req, res) => {
  const { list } = req.query;
  if (list) {
    if (!LENDER_LISTS[list]) {
      return res.status(404).json({ error: `Unknown lender list: ${list}` });
    }
    return res.status(200).json(LENDER_LISTS[list]);
  }
  res.status(200).json(LENDER_LISTS);
});

// GET /config/sources            -> { ...all named lists }
// GET /config/sources?list=upload -> ["FREO","Ratecut",...]
router.get('/sources', (req, res) => {
  const { list } = req.query;
  if (list) {
    if (!SOURCE_LISTS[list]) {
      return res.status(404).json({ error: `Unknown source list: ${list}` });
    }
    return res.status(200).json(SOURCE_LISTS[list]);
  }
  res.status(200).json(SOURCE_LISTS);
});

// GET /config/catalog -> rich per-lender metadata
router.get('/catalog', (req, res) => {
  res.status(200).json(LENDER_CATALOG);
});

// ── Lender rate limits (edited on /admin/rate_limits) ────────────────────────
const {
  describeRateLimits, saveRateLimitSettings, loadRateLimitSettings,
} = require('../config/lenderRateLimits');
const { getLimiterStats } = require('../utils/lenderRateLimiter');

// Same key the admin dashboard asks for at login (NEXT_PUBLIC_ADMIN_KEY there).
const ADMIN_KEY = process.env.ADMIN_KEY || 'ratecut-admin';

// GET /config/rate-limits -> effective limits, live/bulk split, current queues
router.get('/rate-limits', async (req, res) => {
  try {
    await loadRateLimitSettings();
    res.status(200).json({ success: true, data: { ...describeRateLimits(), usage: getLimiterStats() } });
  } catch (err) {
    res.status(500).json({ success: false, message: err.message });
  }
});

// PUT /config/rate-limits  { liveSharePercent?: 60, limits: { OVLY: 250, ZYPE: null } }
// null / "" for a lender removes the dashboard value (env/default applies again).
router.put('/rate-limits', express.json(), async (req, res) => {
  if (req.headers['x-admin-key'] !== ADMIN_KEY) {
    return res.status(401).json({ success: false, message: 'Admin key required' });
  }
  try {
    const { limits, liveSharePercent, updatedBy } = req.body || {};
    await saveRateLimitSettings({ limits, liveSharePercent }, updatedBy || null);
    res.status(200).json({ success: true, data: { ...describeRateLimits(), usage: getLimiterStats() } });
  } catch (err) {
    res.status(err.status || 500).json({ success: false, message: err.message });
  }
});

module.exports = router;

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

module.exports = router;

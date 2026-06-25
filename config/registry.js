// ============================================================================
// registry.js — SINGLE SOURCE OF TRUTH for lenders & sources
// ----------------------------------------------------------------------------
// Add / rename / remove a lender or source HERE ONLY.
//
//   • Backend modules import the named lists below directly.
//   • Frontends (xlsx_upload, dashboard, loanform) read these same lists at
//     runtime via the /api/v1/config endpoints (see routes/configRoutes.js),
//     so adding a lender needs NO frontend redeploy.
//
// Rich per-lender metadata (logo, APR, website, ...) lives in lenderCatalog.js
// and is re-exported here so everything funnels through one module.
//
// NOTE: the per-screen lists below are intentionally DIFFERENT from each other
// (each screen historically showed its own subset). They are kept exact to
// preserve current behavior. To make a new lender appear on a screen, add its
// code to that screen's list.
// ============================================================================

const {
  LENDER_CATALOG,
  DEFAULT_LENDER_ORDER,
  buildLenderList,
} = require('./lenderCatalog');

// ---------------------------------------------------------------------------
// SOURCES
// ---------------------------------------------------------------------------

// Default sources for the DynamoDB response-log models (per-source GSI scan).
// Individual models may still override via their own *_SOURCES env var.
const RESPONSELOG_SOURCES = ['CashKuber', 'FREO', 'BatterySmart', 'Ratecut', 'VFC', 'Apr'];

// Default for models that read process.env.LEAD_SOURCES (creditLinks, leadModel).
const LEAD_SOURCES_DEFAULT = ['BatterySmart', 'VFC', 'FREO', 'CashKuber', 'Ratecut'];

// Named source lists consumed by the frontends. Kept exact to current values.
const SOURCE_LISTS = {
  upload:       ['FREO', 'Ratecut', 'Apr', 'CashKuber'],                          // xlsx_upload › lead_upload
  distribution: ['FREO', 'CashKuber', 'SML', 'Ratecut', 'MyMoneyMantra', 'Apr'],  // dashboard › lead_distribution
  responseLog:  RESPONSELOG_SOURCES,
};

// ---------------------------------------------------------------------------
// LENDERS
// ---------------------------------------------------------------------------

// Named lender lists. Each preserves the exact array previously hard-coded at
// its consumption site, so refactoring changes nothing functionally.
const LENDER_LISTS = {
  // backend: models/leadSuccessModel.js (validation + success-flag columns)
  leadSuccess: [
    'OVLY', 'FREO', 'LendingPlate', 'ZYPE', 'FINTIFI',
    'FATAKPAY', 'FATAKPAYPL', 'RAMFINCROP', 'MyMoneyMantra',
    'INDIALENDS', 'CRMPaisa', 'SML', 'MPOKKET', 'CreditSea',
    // MIS lenders (status updated via file upload, not immediate API response)
    'CASHVIA', 'DIGICREDIT', 'TAP4CREDIT', 'SPEEDOLOAN',
    'PAISABOXX', 'HEROFINCORP', 'PREFR',
  ],
  // frontend: xlsx_upload › lead_upload (push-to-lenders selection)
  upload: [
    'SML', 'FREO', 'OVLY', 'LendingPlate', 'ZYPE', 'FINTIFI',
    'FATAKPAY', 'FATAKPAYPL', 'RAMFINCROP', 'MyMoneyMantra',
    'MPOKKET', 'INDIALENDS', 'CRMPaisa', 'CreditPluse', 'CreditSea',
  ],
  // frontend: dashboard › lead_distribution (filter UI)
  distribution: [
    'SML', 'FREO', 'ZYPE', 'LendingPlate', 'FINTIFI',
    'FATAKPAY', 'FATAKPAYPL', 'OVLY', 'RAMFINCROP', 'MPOKKET',
    'INDIALENDS', 'CRMPaisa', 'MyMoneyMantra', 'CreditSea',
  ],
  // frontend: loanform › DistributionRulesManagement
  distributionRules: [
    'SML', 'FREO', 'OVLY', 'LendingPlate', 'ZYPE', 'FINTIFI',
    'FATAKPAY', 'RAMFINCROP', 'MyMoneyMantra', 'INDIALENDS', 'MPOKKET', 'CRMPaisa',
  ],
  // frontend: xlsx_upload › Home.js (status dashboard columns)
  home: [
    'ZYPE', 'LendingPlate', 'FATAKPAY', 'FATAKPAYPL', 'OVLY',
    'MPOKKET', 'INDIALENDS', 'CRMPaisa', 'CreditPluse', 'CreditSea',
  ],
};

module.exports = {
  // metadata (re-exported from lenderCatalog.js)
  LENDER_CATALOG,
  DEFAULT_LENDER_ORDER,
  buildLenderList,
  // sources
  RESPONSELOG_SOURCES,
  LEAD_SOURCES_DEFAULT,
  SOURCE_LISTS,
  // lenders
  LENDER_LISTS,
};

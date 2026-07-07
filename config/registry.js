const {
  LENDER_CATALOG,
  DEFAULT_LENDER_ORDER,
  buildLenderList,
} = require('./lenderCatalog');

// Default sources for the DynamoDB response-log models (per-source GSI scan).
// Individual models may still override via their own *_SOURCES env var.
const RESPONSELOG_SOURCES = ['CashKuber', 'FREO', 'BatterySmart', 'Ratecut', 'VFC', 'Apr', 'CreditHaat', 'BAL'];

// Default for models that read process.env.LEAD_SOURCES (creditLinks, leadModel).
const LEAD_SOURCES_DEFAULT = ['BatterySmart', 'VFC', 'FREO', 'CashKuber', 'Ratecut', 'CreditHaat', 'BAL', 'Apr'];

// Named source lists consumed by the frontends. Kept exact to current values.
const SOURCE_LISTS = {
  upload:       ['FREO', 'Ratecut', 'Apr', 'CashKuber', 'CreditHaat', 'BAL', 'BatterySmart', 'VFC' ],                          // xlsx_upload › lead_upload
  distribution: ['FREO', 'CashKuber', 'Ratecut', 'Apr', 'CreditHaat', 'BAL', 'BatterySmart', 'VFC', ],  // dashboard › lead_distribution
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
    'INDIALENDS', 'CRMPaisa', 'SML', 'MPOKKET', 'CreditSea', 'CreditHaat', 'CreditLinks',
    // MIS lenders (status updated via file upload, not immediate API response)
    'CASHVIA', 'DIGICREDIT', 'TAP4CREDIT', 'SPEEDOLOAN',
    'PAISABOXX', 'HEROFINCORP', 'PREFR',
  ],
  // frontend: xlsx_upload › lead_upload (push-to-lenders selection)
  upload: [
    'SML', 'FREO', 'OVLY', 'LendingPlate', 'ZYPE', 'FINTIFI',
    'FATAKPAY', 'FATAKPAYPL', 'RAMFINCROP', 'MyMoneyMantra',
    'MPOKKET', 'INDIALENDS', 'CRMPaisa', 'CreditPluse', 'CreditSea', 'CreditHaat'
  ],
  // frontend: dashboard › lead_distribution (filter UI)
  distribution: [
    'SML', 'FREO', 'ZYPE', 'LendingPlate', 'FINTIFI',
    'FATAKPAY', 'FATAKPAYPL', 'OVLY', 'RAMFINCROP', 'MPOKKET',
    'INDIALENDS', 'CRMPaisa', 'MyMoneyMantra', 'CreditSea', 'CreditHaat', 'CreditLinks',
  ],
  // frontend: loanform › DistributionRulesManagement
  distributionRules: [
    'SML', 'FREO', 'OVLY', 'LendingPlate', 'ZYPE', 'FINTIFI',
    'FATAKPAY', 'RAMFINCROP', 'MyMoneyMantra', 'INDIALENDS', 'MPOKKET', 'CRMPaisa', 'CreditHaat', 'CreditLinks',
  ],
  // frontend: xlsx_upload › Home.js (status dashboard columns)
  home: [
    'ZYPE', 'LendingPlate', 'FATAKPAY', 'FATAKPAYPL', 'OVLY',
    'MPOKKET', 'INDIALENDS', 'CRMPaisa', 'CreditPluse', 'CreditSea', 'CreditHaat', 'CreditLinks',
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

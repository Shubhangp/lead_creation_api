// config/lenderResultCategorizer.js
// ---------------------------------------------------------------------------
// Maps a single lender send-result (the response-log item returned by each
// services/lenderService.js `sendTo*` function) into the SAME status category
// that the stats dashboard shows for that lender.
//
// Why this exists:
//   The distribution flow stores a per-batch `statusCategories` breakdown so
//   the dashboard reads it straight from the history record (no recompute).
//   Each lender has its OWN category scheme:
//     CreditLinks → LEAD_CREATED / ALREADY_EXISTS / NOT_ELIGIBLE / FAILED
//     ZYPE        → ACCEPT / REJECTED / Failed
//     Mpokket     → 200 / 400 / 403
//     CreditSea   → Success / Fail / Duplicate / null
//     FatakPay    → eligible / notEligible / leadExists / loanExists
//     ...
//   A single fixed {ACCEPT,REJECTED,Failed,other} map (the old behaviour) threw
//   everything into "other" for non-Zype lenders. This module mirrors each
//   lender's own getStats() bucketing so the stored keys match the dashboard.
//
// Where the send-function result comes from:
//   Each sendTo*() returns the persisted response-log item, which carries
//   `responseStatus` and `responseBody`. Those are exactly the fields the
//   response-log models categorize on, so we reuse the model `_extractStatus`
//   helpers where they exist to stay in lock-step with the dashboard.
// ---------------------------------------------------------------------------

// Models that already expose a canonical `_extractStatus(item)` — reuse them so
// distribution categorization can never drift from the dashboard.
const CreditLinksResponseLog = require('../models/creditLinksResponseLog');
const CreditSeaResponseLog   = require('../models/creditSeaResponseLog');
const CreditPulseResponseLog = require('../models/creditPulseResponseLog');
const CreditHaatResponseLog  = require('../models/creditHaatResponseLog');
const LendingPlateResponseLog = require('../models/leadingPlateResponseLog');

// Safe JSON body accessor (responseBody may be an object or a JSON string).
function parseBody(item) {
  let body = item && item.responseBody;
  if (!body) return null;
  if (typeof body === 'string') {
    try { body = JSON.parse(body); } catch (_) { return null; }
  }
  return (body && typeof body === 'object') ? body : null;
}

// Collapse a raw Success/Fail-style status into the {Success,Fail,null,other}
// buckets used by CreditSea / CreditPulse / CreditHaat / LendingPlate dashboards.
function bucketSuccessFail(raw, { duplicate = false } = {}) {
  const s = String(raw == null ? '' : raw).trim();
  const low = s.toLowerCase();
  if (!s || low === 'null') return 'null';
  if (low === 'lead_created' || low === 'success' || low === '200' || low === '201') return 'Success';
  if (duplicate && (low === 'duplicate' || low === 'already_exists')) return 'Duplicate';
  if (low === 'fail' || low === 'failed' || low === 'failure' || low === '400') return 'Fail';
  if (low === 'error') return 'other';
  return 'other';
}

// ── Per-lender categorizers. Each returns the dashboard bucket string. ────────
const LENDER_CATEGORIZERS = {
  // ZYPE buckets on responseBody.status → ACCEPT / REJECTED / Failed / other
  ZYPE(item) {
    const body = parseBody(item);
    const bs = body && body.status;
    return ['ACCEPT', 'REJECTED', 'Failed'].includes(bs) ? bs : 'other';
  },

  // CreditLinks writes responseStatus = LEAD_CREATED / ALREADY_EXISTS / NOT_ELIGIBLE / FAILED
  CreditLinks(item) {
    const st = CreditLinksResponseLog._extractStatus(item);
    return ['LEAD_CREATED', 'ALREADY_EXISTS', 'NOT_ELIGIBLE', 'FAILED'].includes(st) ? st : 'other';
  },

  // CreditSea → Success / Fail / Duplicate / null / other
  CreditSea(item) {
    const st = CreditSeaResponseLog._extractStatus(item);
    if (st === 'DUPLICATE') return 'Duplicate';
    return bucketSuccessFail(st, { duplicate: true });
  },

  // CreditPulse → Success / Fail / null / other
  CreditPulse(item) {
    return bucketSuccessFail(CreditPulseResponseLog._extractStatus(item));
  },

  // CreditHaat → Success / Fail / null / other
  CreditHaat(item) {
    return bucketSuccessFail(CreditHaatResponseLog._extractStatus(item));
  },

  // LendingPlate → Success / Fail / null / other
  LendingPlate(item) {
    return bucketSuccessFail(LendingPlateResponseLog._extractStatus(item));
  },

  // Mpokket buckets on HTTP-ish responseStatus → 200 / 400 / 403 / other
  MPOKKET(item) {
    const rs = String(item && item.responseStatus || '');
    return ['200', '400', '403'].includes(rs) ? rs : 'other';
  },

  // RamFinCrop → 200 / 400 / other
  RAMFINCROP(item) {
    const rs = String(item && item.responseStatus || '');
    return ['200', '400'].includes(rs) ? rs : 'other';
  },

  // OVLY → 403 / success / duplicate / other
  OVLY(item) {
    const low = String(item && item.responseStatus || '').toLowerCase();
    if (low === '403') return '403';
    if (low === 'success' || low === '200') return 'success';
    if (low === 'duplicate') return 'duplicate';
    return 'other';
  },

  // FatakPay / FatakPayPL → message-driven eligibility buckets
  FATAKPAY: fatakPayCategorizer,
  FATAKPAYPL: fatakPayCategorizer,
};

function fatakPayCategorizer(item) {
  const body = parseBody(item);
  const msg = body && body.message;
  if (msg) {
    const low = String(msg).toLowerCase();
    if (low.includes('you are eligible')) return 'eligible';
    if (low.includes('not eligible')) return 'notEligible';
    if (low.includes('lead already exists')) return 'leadExists';
    if (low.includes('loan application already exists')) return 'loanExists';
  }
  return 'other';
}

// Generic fallback: category == the lender's raw responseStatus, else the
// responseBody.status, else 'other'. Covers lenders whose dashboard uses a
// dynamic responseStatus breakdown (CrmPaisa, FINTIFI, FREO, SML, IndiaLends,
// MyMoneyMantra, etc.). Keeps keys human-readable and lender-native.
function genericCategorizer(item) {
  const rs = item && item.responseStatus;
  if (rs !== undefined && rs !== null && String(rs).trim() !== '') return String(rs);
  const body = parseBody(item);
  const bs = body && (body.status || body.Status);
  if (bs) return String(bs);
  return 'other';
}

/**
 * Categorize one lender send-result into its dashboard status bucket.
 *
 * @param {string} lender  Lender key (e.g. 'CreditLinks', 'ZYPE').
 * @param {*} result       The value returned by the sendTo*() function
 *                         (a response-log item), or undefined if it early-returned.
 * @param {Error|null} error  A thrown error, if the send failed hard.
 * @returns {string} category key for statusCategories (dynamic per lender).
 */
function categorizeLenderResult(lender, result, error) {
  // Hard throw with no response persisted → genuine failure.
  if (error) return 'FAILED';
  // Some lenders early-return (skipped / ineligible-before-call) with no log.
  if (result === undefined || result === null) return 'other';

  try {
    const fn = LENDER_CATEGORIZERS[lender] || genericCategorizer;
    const category = fn(result);
    return (category === undefined || category === null || category === '') ? 'other' : category;
  } catch (_) {
    return 'other';
  }
}

module.exports = { categorizeLenderResult };

'use strict';
/**
 * One spec per table shown on /admin/stats_dashboard.
 *
 * Each spec says how a single row of that table is counted:
 *   classify(item) → { category, dims: { name: value | [values] }, metrics: { name: number } }
 *
 * The category rules below are copied from the old per-model getStats /
 * getStatsByDate code so the numbers mean the same thing as before.
 *
 * Pure JS, no imports besides ./core — bundled into the stream Lambda.
 *
 * To add a table: add a spec here, run `node scripts/stats/setup.js --only <key>`
 * and `node scripts/stats/backfill.js --only <key>`.
 */

const { cleanMessage } = require('./core');

// ─── shared helpers ──────────────────────────────────────────────────────────

function parseBody(item) {
  let b = item && item.responseBody;
  if (b === null || b === undefined) return null;
  if (typeof b === 'string') { try { b = JSON.parse(b); } catch (_) { return null; } }
  return b && typeof b === 'object' ? b : null;
}

/** DynamoDB-typed ({S:..}) or plain value. */
function plain(v) {
  if (v && typeof v === 'object') return v.S ?? v.N ?? v.BOOL ?? null;
  return v;
}

/** Best-effort human message from a lender response body. */
function bodyMessage(body) {
  if (!body) return null;
  const cand = [
    body.message, body.Message, body.msg, body.info && body.info.message,
    body.data && body.data.message, body.error && body.error.message,
    typeof body.error === 'string' ? body.error : null, body.errorMessage, body.reason,
  ];
  for (const c of cand) {
    const v = plain(c);
    if (v !== null && v !== undefined && typeof v !== 'object' && String(v).trim()) return String(v).trim();
  }
  return null;
}

const rs = (item) => (item.responseStatus === null || item.responseStatus === undefined ? 'unknown' : String(item.responseStatus));
const has = (v) => v !== null && v !== undefined && v !== '';

const tone = { good: 'good', bad: 'bad', warn: 'warn', neutral: 'neutral' };
const cat = (key, label, t = tone.neutral) => ({ key, label, tone: t });

// Dimensions every response-log table gets.
const LOG_DIMS = [
  { key: 'responseStatus', label: 'Response status' },
  { key: 'message', label: 'Response messages', kind: 'table' },
];

// ─── leads ───────────────────────────────────────────────────────────────────

function ageAt(item) {
  let age = typeof item.age === 'number' ? item.age : parseInt(item.age, 10);
  if (!Number.isFinite(age) || age <= 0) age = null;
  if (age === null && item.dateOfBirth) {
    const dob = new Date(item.dateOfBirth);
    const ref = new Date(item.createdAt || Date.now());
    if (!isNaN(dob)) {
      age = ref.getUTCFullYear() - dob.getUTCFullYear();
      const md = ref.getUTCMonth() - dob.getUTCMonth();
      if (md < 0 || (md === 0 && ref.getUTCDate() < dob.getUTCDate())) age--;
      if (age <= 0 || age > 120) age = null;
    }
  }
  return age;
}

function ageRange(age) {
  if (age === null) return 'Unknown';
  if (age < 18) return 'Below 18';
  if (age <= 25) return '18-25';
  if (age <= 35) return '26-35';
  if (age <= 45) return '36-45';
  if (age <= 55) return '46-55';
  if (age <= 58) return '56-58';
  return 'Above 58';
}

function salaryRange(s) {
  if (s === null) return 'Unknown';
  if (s < 20000) return 'Below 20k';
  if (s < 40000) return '20k-40k';
  if (s < 60000) return '40k-60k';
  if (s < 80000) return '60k-80k';
  if (s < 100000) return '80k-100k';
  return 'Above 100k';
}

function scoreRange(s) {
  if (s === null) return 'Unknown';
  if (s < 580) return 'Poor (300-579)';
  if (s < 670) return 'Fair (580-669)';
  if (s < 740) return 'Good (670-739)';
  if (s < 800) return 'Very Good (740-799)';
  return 'Excellent (800-900)';
}

const PIN_ZONES = {
  1: 'North (DL/HR/PB/HP/J&K)', 2: 'North (UP/UK)', 3: 'West (RJ/GJ)',
  4: 'West (MH/MP/CG/GA)', 5: 'South (AP/TS/KA)', 6: 'South (TN/KL)',
  7: 'East (WB/OD/NE)', 8: 'East (BR/JH)', 9: 'Army Postal',
};

function toNum(v) {
  if (!has(v)) return null;
  const n = parseFloat(String(v).replace(/[,₹\s]/g, ''));
  return Number.isFinite(n) ? n : null;
}

const leads = {
  key: 'leads',
  table: 'leads',
  displayName: 'Leads',
  group: 'leads',
  skip: (i) => typeof i.leadId !== 'string' || i.leadId.startsWith('COUNT#') || i.leadId.startsWith('DAYCOUNT#'),
  categories: [
    cat('converted', 'Converted (any lender)', tone.good),
    cat('notConverted', 'Not converted yet', tone.neutral),
  ],
  successKeys: ['converted'],
  successLabel: 'Conversion rate',
  dimensions: [
    { key: 'gender', label: 'Gender', kind: 'pie' },
    { key: 'ageRange', label: 'Age range', order: ['Below 18', '18-25', '26-35', '36-45', '46-55', '56-58', 'Above 58', 'Unknown'] },
    { key: 'jobType', label: 'Job type', kind: 'pie' },
    { key: 'salaryRange', label: 'Salary range', order: ['Below 20k', '20k-40k', '40k-60k', '60k-80k', '80k-100k', 'Above 100k', 'Unknown'] },
    { key: 'creditScore', label: 'Credit score', order: ['Poor (300-579)', 'Fair (580-669)', 'Good (670-739)', 'Very Good (740-799)', 'Excellent (800-900)', 'Unknown'] },
    { key: 'businessType', label: 'Business type' },
    { key: 'consent', label: 'Consent', kind: 'pie' },
    { key: 'convertedWith', label: 'Converted with lender' },
    { key: 'source2', label: 'Secondary source (source2)' },
    { key: 'campaign', label: 'Campaign' },
    { key: 'pinZone', label: 'Region (PIN zone)' },
    { key: 'pinPrefix', label: 'PIN prefix (first 2 digits)' },
    { key: 'emailDomain', label: 'Email domain' },
    { key: 'visited', label: 'Visited', kind: 'pie' },
    { key: 'filled', label: 'Field completeness (rows with field)', kind: 'completeness' },
  ],
  metrics: [
    { key: 'salary', label: 'Salary', format: 'inr' },
    { key: 'creditScore', label: 'Credit score', format: 'num' },
    { key: 'age', label: 'Age', format: 'num' },
  ],
  uniques: [
    { key: 'phone', label: 'Unique phones', value: (i) => i.phone },
    { key: 'pan', label: 'Unique PANs', value: (i) => i.panNumber },
  ],
  alerts: [
    { type: 'minTotal', critical: 1, warning: 10 },
  ],
  classify(i) {
    const g = i.gender;
    const gender = !g ? 'Unknown' : ['Male', 'Female', 'Other'].includes(g) ? g : 'Other';
    const age = ageAt(i);
    const salary = toNum(i.salary);
    const score = toNum(i.creditScore) ?? toNum(i.cibilScore);
    const lenders = Array.isArray(i.successfulLenders) ? i.successfulLenders.filter(Boolean) : [];
    const pin = has(i.pincode) ? String(i.pincode).replace(/\D/g, '') : '';
    const email = typeof i.email === 'string' && i.email.includes('@') ? i.email.split('@').pop().toLowerCase() : null;
    const filled = [];
    if (has(i.phone)) filled.push('phone');
    if (has(i.panNumber)) filled.push('PAN');
    if (has(i.email)) filled.push('email');
    if (has(i.dateOfBirth) || has(i.age)) filled.push('DOB / age');
    if (has(i.gender)) filled.push('gender');
    if (has(i.salary)) filled.push('salary');
    if (has(i.creditScore) || has(i.cibilScore)) filled.push('credit score');
    if (has(i.jobType)) filled.push('job type');
    if (has(i.pincode)) filled.push('pincode');
    if (has(i.address)) filled.push('address');

    return {
      category: lenders.length ? 'converted' : 'notConverted',
      dims: {
        gender,
        ageRange: ageRange(age),
        jobType: i.jobType || 'Unknown',
        salaryRange: salaryRange(salary),
        creditScore: scoreRange(score),
        businessType: i.businessType || null,
        consent: i.consent === true ? 'Given' : i.consent === false ? 'Not given' : 'Unknown',
        convertedWith: lenders,
        source2: i.source2 || null,
        campaign: i.campaign_identifier || null,
        pinZone: pin.length === 6 ? (PIN_ZONES[pin[0]] || 'Invalid') : 'Unknown',
        pinPrefix: pin.length === 6 ? pin.slice(0, 2) : null,
        emailDomain: email,
        visited: i.visited === true ? 'Yes' : 'No',
        filled,
      },
      metrics: { salary, creditScore: score, age },
    };
  },
};

// ─── FatakPay / FatakPay PL ──────────────────────────────────────────────────

const PERMANENT_BLOCKS = [
  'application is permanently block.', 'attribute error', 'internal server error',
  'loan application already exists for this pan number.', 'loan application already exists.',
  'user already exists in the system.', 'unknown error', 'you are eligible.',
  'you are not eligible due to age criteria', 'you are not eligible.', 'this pan is already in use.',
];

function fatakpayClassify(i, withStatusFallback) {
  const body = parseBody(i);
  const msg = body && typeof body.message === 'string' ? body.message.trim() : null;
  const low = msg ? msg.toLowerCase() : '';
  let category = 'other';
  if (msg) {
    if (low.includes('you are eligible')) category = 'eligible';
    else if (low.includes('not eligible')) category = 'notEligible';
    else if (low.includes('lead already exists')) category = 'leadExists';
    else if (low.includes('loan application already exists')) category = 'loanExists';
  }
  if (category === 'other' && withStatusFallback && body) {
    if (body.status === 'ACCEPT') category = 'accept';
    else if (body.status === 'REJECTED') category = 'rejected';
    else if (body.status === 'Failed') category = 'failed';
  }
  let detail = null;
  if (low.includes('you are eligible')) detail = 'Eligible';
  else if (low.includes('not eligible due to age')) detail = 'Not eligible (age)';
  else if (low.includes('not eligible')) detail = 'Not eligible';
  else if (low.includes('lead already exists')) detail = 'Lead exists';
  else if (low.includes('loan application already exists')) detail = 'Loan exists';

  const offer = body && body.offer !== undefined ? toNum(body.offer) : null;
  return {
    category,
    dims: {
      responseStatus: rs(i),
      message: cleanMessage(msg),
      eligibility: detail,
      permanentBlock: msg && PERMANENT_BLOCKS.includes(low) ? 'Permanent block' : 'Not blocked',
      bodyStatus: body ? (plain(body.status) || null) : null,
      offerBand: (category === 'eligible' || category === 'accept') ? offerBand(offer) : null,
    },
    metrics: { offerAmount: (category === 'eligible' || category === 'accept') && offer ? offer : null },
  };
}

function offerBand(v) {
  if (!v) return 'No offer';
  if (v < 10000) return 'Below 10k';
  if (v < 25000) return '10k-25k';
  if (v < 50000) return '25k-50k';
  if (v < 100000) return '50k-1L';
  return 'Above 1L';
}
const OFFER_BAND_ORDER = ['No offer', 'Below 10k', '10k-25k', '25k-50k', '50k-1L', 'Above 1L'];

const fatakpayDims = [
  ...LOG_DIMS,
  { key: 'eligibility', label: 'Eligibility outcome' },
  { key: 'permanentBlock', label: 'Permanent blocks', kind: 'pie' },
  { key: 'bodyStatus', label: 'Body status field' },
  { key: 'offerBand', label: 'Offer size (eligible rows)', order: OFFER_BAND_ORDER },
];

const fatakpay = {
  key: 'fatakpay', table: 'fatakpay_response_logs', displayName: 'FatakPay', group: 'lender',
  categories: [
    cat('eligible', 'Eligible', tone.good), cat('notEligible', 'Not eligible', tone.bad),
    cat('leadExists', 'Lead exists', tone.warn), cat('loanExists', 'Loan exists', tone.warn),
    cat('other', 'Other', tone.neutral),
  ],
  successKeys: ['eligible'],
  dimensions: fatakpayDims,
  metrics: [{ key: 'offerAmount', label: 'Offer amount', format: 'inr' }],
  uniques: [{ key: 'lead', label: 'Unique leads', value: (i) => i.leadId }],
  classify: (i) => fatakpayClassify(i, false),
};

const fatakpaypl = {
  ...fatakpay,
  key: 'fatakpaypl', table: 'fatakpay_pl__response_logs', displayName: 'FatakPay PL',
  categories: [
    cat('eligible', 'Eligible', tone.good), cat('accept', 'Accepted', tone.good),
    cat('notEligible', 'Not eligible', tone.bad), cat('rejected', 'Rejected', tone.bad),
    cat('failed', 'Failed', tone.bad), cat('leadExists', 'Lead exists', tone.warn),
    cat('loanExists', 'Loan exists', tone.warn), cat('other', 'Other', tone.neutral),
  ],
  successKeys: ['eligible', 'accept'],
  classify: (i) => fatakpayClassify(i, true),
};

// ─── Ovly ────────────────────────────────────────────────────────────────────

const ovly = {
  key: 'ovly', table: 'ovly_response_logs', displayName: 'Ovly', group: 'lender',
  categories: [
    cat('success', 'Success', tone.good), cat('duplicate', 'Duplicate', tone.warn),
    cat('403', '403 Forbidden', tone.bad), cat('other', 'Other', tone.neutral),
  ],
  successKeys: ['success'],
  dimensions: [
    ...LOG_DIMS,
    { key: 'stage', label: 'Post-push funnel (from status uploads)', kind: 'funnel',
      order: ['Applied', 'KYC done', 'Approved', 'E-mandate', 'Agreement signed', 'Disbursed'] },
    { key: 'rejectionReason', label: 'Rejection reasons', kind: 'table' },
  ],
  metrics: [
    { key: 'loanAmount', label: 'Loan amount', format: 'inr' },
    { key: 'unlockAmount', label: 'Unlock amount', format: 'inr' },
  ],
  uniques: [{ key: 'lead', label: 'Unique leads', value: (i) => i.leadId }],
  alerts: [{ type: 'ratio', category: '403', warning: 0.2, critical: 0.5, label: '403 errors' }],
  classify(i) {
    const s = String(i.responseStatus || 'unknown');
    const l = s.toLowerCase();
    const category = l === '403' ? '403' : (l === 'success' || l === '200') ? 'success' : l === 'duplicate' ? 'duplicate' : 'other';
    const stage = [];
    if (has(i.applied_date)) stage.push('Applied');
    if (has(i.kyc_completed_date)) stage.push('KYC done');
    if (has(i.approved_date)) stage.push('Approved');
    if (has(i.emandate_done_at)) stage.push('E-mandate');
    if (has(i.agreement_signed_date)) stage.push('Agreement signed');
    if (has(i.loan_disbursed_date)) stage.push('Disbursed');
    return {
      category,
      dims: { responseStatus: s, message: cleanMessage(bodyMessage(parseBody(i))), stage, rejectionReason: cleanMessage(i.rejection_reason) },
      metrics: { loanAmount: i.loan_amount, unlockAmount: i.unlock_amount },
    };
  },
};

// ─── Lending Plate ───────────────────────────────────────────────────────────

function lpStatusOf(i) {
  if (i.responseStatus) return String(i.responseStatus);
  const b = parseBody(i);
  if (!b) return 'null';
  const v = plain(b.Status) ?? plain(b.status);
  return v ? String(v) : 'null';
}

const lendingplate = {
  key: 'lendingplate', table: 'lending_plate_response_logs', displayName: 'Lending Plate', group: 'lender',
  categories: [
    cat('Success', 'Success', tone.good), cat('Fail', 'Fail', tone.bad),
    cat('null', 'No status', tone.warn), cat('other', 'Other / Error', tone.neutral),
  ],
  successKeys: ['Success'],
  dimensions: [
    ...LOG_DIMS,
    { key: 'stage', label: 'Post-push funnel (from status uploads)', kind: 'funnel', order: ['Sanctioned', 'Disbursed', 'Rejected'] },
    { key: 'lpStatus', label: 'LP status' },
    { key: 'lpIncomeType', label: 'LP income type' },
    { key: 'lpRejectReason', label: 'LP reject reasons', kind: 'table' },
    { key: 'api1Response', label: 'API 1 response' },
    { key: 'api2Response', label: 'API 2 response' },
    { key: 'afMediaSource', label: 'AppsFlyer media source' },
    { key: 'afPartner', label: 'AppsFlyer partner' },
  ],
  metrics: [
    { key: 'sanctionedAmount', label: 'Sanctioned amount', format: 'inr' },
    { key: 'disbursedAmount', label: 'Disbursed amount', format: 'inr' },
  ],
  uniques: [{ key: 'lead', label: 'Unique leads', value: (i) => i.leadId }],
  alerts: [{ type: 'ratio', category: 'Fail', warning: 0.3, critical: 0.6, label: 'Failure rate' }],
  classify(i) {
    const s = lpStatusOf(i);
    const category = s === 'Success' ? 'Success' : s === 'Fail' ? 'Fail' : (s === 'null' || !s) ? 'null' : 'other';
    const lp = i.lpStatus ? String(i.lpStatus) : null;
    const stage = [];
    if (lp === 'SANCTION' || lp === 'SANCTION-ACCEPTED' || lp === 'DISBURSED' || has(i.sanctionedAmount)) stage.push('Sanctioned');
    if (lp === 'DISBURSED' || has(i.disbursedAmount)) stage.push('Disbursed');
    if (lp === 'REJECT') stage.push('Rejected');
    return {
      category,
      dims: {
        responseStatus: s, message: cleanMessage(bodyMessage(parseBody(i))), stage,
        lpStatus: lp, lpIncomeType: i.lpIncomeType, lpRejectReason: cleanMessage(i.lpRejectReason),
        api1Response: cleanMessage(i.api1Response), api2Response: cleanMessage(i.api2Response),
        afMediaSource: i.afMediaSource, afPartner: i.afPartner,
      },
      metrics: { sanctionedAmount: i.sanctionedAmount, disbursedAmount: i.disbursedAmount },
    };
  },
};

// ─── simple HTTP-code lenders (Mpokket, RamFinCorp) ──────────────────────────

const mpokket = {
  key: 'mpokket', table: 'mpokket_response_logs', displayName: 'Mpokket', group: 'lender',
  categories: [
    cat('200', '200 OK', tone.good), cat('400', '400 Bad request', tone.bad),
    cat('403', '403 Forbidden', tone.bad), cat('other', 'Other', tone.neutral),
  ],
  successKeys: ['200'],
  dimensions: [
    ...LOG_DIMS,
    { key: 'status', label: 'Status field' },
    { key: 'step', label: 'Step' },
  ],
  metrics: [],
  uniques: [{ key: 'lead', label: 'Unique leads', value: (i) => i.leadId }],
  classify(i) {
    const s = rs(i);
    return {
      category: ['200', '400', '403'].includes(s) ? s : 'other',
      dims: { responseStatus: s, message: cleanMessage(bodyMessage(parseBody(i))), status: i.status, step: i.step },
    };
  },
};

const ramfincrop = {
  key: 'ramfincrop', table: 'ramfincrop_logs', displayName: 'RamFinCorp', group: 'lender',
  categories: [cat('200', '200 OK', tone.good), cat('400', '400 Bad request', tone.bad), cat('other', 'Other', tone.neutral)],
  successKeys: ['200'],
  dimensions: [...LOG_DIMS, { key: 'dedupe', label: 'Dedupe result', kind: 'pie' }],
  metrics: [],
  uniques: [{ key: 'lead', label: 'Unique leads', value: (i) => i.leadId }],
  classify(i) {
    const s = rs(i);
    const body = parseBody(i);
    return {
      category: ['200', '400'].includes(s) ? s : 'other',
      dims: {
        responseStatus: s,
        message: cleanMessage(body && body.message),
        dedupe: body ? (plain(body.dedupe) ?? 'unknown') : null,
      },
    };
  },
};

// ─── India Lends ─────────────────────────────────────────────────────────────

const indialends = {
  key: 'indialends', table: 'indialends_response_logs', displayName: 'India Lends', group: 'lender',
  categories: [
    cat('successful', 'Successful (not duplicate)', tone.good),
    cat('duplicate', 'Duplicate', tone.warn),
    cat('failed', 'Failed', tone.bad),
  ],
  successKeys: ['successful'],
  dimensions: [
    ...LOG_DIMS,
    { key: 'outcome', label: 'Response outcome' },
    { key: 'isDuplicate', label: 'Duplicate flag', kind: 'pie' },
    { key: 'isSuccess', label: 'Success flag', kind: 'pie' },
    { key: 'duplicateStatus', label: 'Duplicate status code' },
    { key: 'retryCount', label: 'Retries' },
  ],
  metrics: [],
  uniques: [{ key: 'lead', label: 'Unique leads', value: (i) => i.leadId }],
  classify(i) {
    const body = parseBody(i);
    const info = body && body.info && body.info.message;
    const dataMsg = body && body.data && body.data.message;
    const outcome = [];
    if (typeof info === 'string' && info.includes('Verification code sent')) outcome.push('Verification code sent');
    if (typeof dataMsg === 'string' && dataMsg.includes('Duplicate lead found')) outcome.push('Duplicate lead found');
    const category = i.isDuplicate === 'true' ? 'duplicate'
      : (i.isSuccess === 'true' && i.isDuplicate === 'false') ? 'successful' : 'failed';
    return {
      category,
      dims: {
        responseStatus: rs(i), message: cleanMessage(info || dataMsg || bodyMessage(body)), outcome,
        isDuplicate: i.isDuplicate || 'unknown', isSuccess: i.isSuccess || 'false',
        duplicateStatus: i.duplicateStatus, retryCount: has(i.retryCount) ? String(i.retryCount) : null,
      },
    };
  },
};

// ─── Zype ────────────────────────────────────────────────────────────────────

const zype = {
  key: 'zype', table: 'zype_response_logs', displayName: 'Zype', group: 'lender',
  categories: [
    cat('ACCEPT', 'Accepted', tone.good), cat('REJECTED', 'Rejected', tone.bad),
    cat('Failed', 'Failed', tone.bad), cat('other', 'Other', tone.neutral),
    cat('noResponse', 'No response body', tone.warn),
  ],
  successKeys: ['ACCEPT'],
  dimensions: [...LOG_DIMS, { key: 'offerBand', label: 'Offer size (accepted rows)', order: OFFER_BAND_ORDER }],
  metrics: [{ key: 'offerAmount', label: 'Offer amount', format: 'inr' }],
  uniques: [{ key: 'lead', label: 'Unique leads', value: (i) => i.leadId }],
  classify(i) {
    const body = parseBody(i);
    const bs = body ? body.status : null;
    const category = !body ? 'noResponse' : ['ACCEPT', 'REJECTED', 'Failed'].includes(bs) ? bs : 'other';
    const offer = category === 'ACCEPT' && body && body.offer ? toNum(body.offer) : null;
    return {
      category,
      dims: {
        responseStatus: rs(i), message: cleanMessage(bodyMessage(body)),
        offerBand: category === 'ACCEPT' ? offerBand(offer) : null,
      },
      metrics: { offerAmount: offer },
    };
  },
};

// ─── CreditSea ───────────────────────────────────────────────────────────────

function creditSeaStatus(i) {
  if (i.responseStatus) return String(i.responseStatus);
  const b = parseBody(i);
  if (!b) return 'null';
  if (b.leadId) return 'LEAD_CREATED';
  if (b.isPresent === true) return 'DUPLICATE';
  if (b.message) return String(b.message);
  return 'null';
}

const creditsea = {
  key: 'creditsea', table: 'credit_sea_response_logs', displayName: 'Credit Sea', group: 'lender',
  categories: [
    cat('Success', 'Lead created', tone.good), cat('Duplicate', 'Duplicate', tone.warn),
    cat('Fail', 'Failed', tone.bad), cat('null', 'No status', tone.warn), cat('other', 'Other', tone.neutral),
  ],
  successKeys: ['Success'],
  dimensions: LOG_DIMS,
  metrics: [],
  uniques: [{ key: 'lead', label: 'Unique leads', value: (i) => i.leadId }],
  classify(i) {
    const s = creditSeaStatus(i);
    const category = s === 'LEAD_CREATED' ? 'Success' : s === 'DUPLICATE' ? 'Duplicate'
      : (s === 'FAILED' || s === 'Error') ? 'Fail' : (!s || s === 'null') ? 'null' : 'other';
    return { category, dims: { responseStatus: cleanMessage(s), message: cleanMessage(bodyMessage(parseBody(i))) } };
  },
};

// ─── CreditHaat ──────────────────────────────────────────────────────────────

function creditHaatStatus(i) {
  if (i.responseStatus) return String(i.responseStatus);
  const b = parseBody(i);
  if (!b) return 'null';
  if (b.code === 200 || b.code === '200') return 'Success';
  if (b.code && b.code !== 200) return 'Fail';
  const v = plain(b.Status) ?? plain(b.status);
  return v ? String(v) : 'null';
}

const credithaat = {
  key: 'credithaat', table: 'credithaat_response_logs', displayName: 'CreditHaat', group: 'lender',
  categories: [
    cat('Success', 'Success', tone.good), cat('Fail', 'Fail', tone.bad),
    cat('null', 'No status', tone.warn), cat('other', 'Other / Error', tone.neutral),
  ],
  successKeys: ['Success'],
  dimensions: [
    ...LOG_DIMS,
    { key: 'stage', label: 'Post-push funnel (from status uploads)', kind: 'funnel', order: ['Sanctioned', 'Disbursed'] },
    { key: 'chStatus', label: 'CreditHaat status' },
    { key: 'chProduct', label: 'Product' },
    { key: 'chRejectReason', label: 'Reject reasons', kind: 'table' },
    { key: 'afMediaSource', label: 'AppsFlyer media source' },
    { key: 'afPartner', label: 'AppsFlyer partner' },
  ],
  metrics: [
    { key: 'sanctionedAmount', label: 'Sanctioned amount', format: 'inr' },
    { key: 'disbursedAmount', label: 'Disbursed amount', format: 'inr' },
    { key: 'maxLoanAmount', label: 'Max loan amount offered', format: 'inr' },
  ],
  uniques: [{ key: 'lead', label: 'Unique leads', value: (i) => i.leadId }],
  classify(i) {
    const s = creditHaatStatus(i);
    const category = s === 'Success' ? 'Success' : s === 'Fail' ? 'Fail' : (s === 'null' || !s) ? 'null' : 'other';
    const stage = [];
    if (has(i.sanctionedAmount) || has(i.sanctionedDate)) stage.push('Sanctioned');
    if (has(i.disbursedAmount) || has(i.disbursedDate)) stage.push('Disbursed');
    return {
      category,
      dims: {
        responseStatus: s, message: cleanMessage(bodyMessage(parseBody(i))), stage,
        chStatus: i.chStatus, chProduct: i.chProduct, chRejectReason: cleanMessage(i.chRejectReason),
        afMediaSource: i.afMediaSource, afPartner: i.afPartner,
      },
      metrics: { sanctionedAmount: i.sanctionedAmount, disbursedAmount: i.disbursedAmount, maxLoanAmount: i.chMaxLoanAmount },
    };
  },
};

// ─── CreditPulse ─────────────────────────────────────────────────────────────

const creditpulse = {
  key: 'creditpulse', table: 'credit_pulse_response_logs', displayName: 'CreditPulse', group: 'lender',
  categories: [
    cat('Success', 'Success', tone.good), cat('Fail', 'Failure', tone.bad),
    cat('null', 'No status', tone.warn), cat('other', 'Other / Error', tone.neutral),
  ],
  successKeys: ['Success'],
  dimensions: LOG_DIMS,
  metrics: [],
  uniques: [{ key: 'lead', label: 'Unique leads', value: (i) => i.leadId }],
  classify(i) {
    let s;
    if (i.responseStatus) s = String(i.responseStatus);
    else { const b = parseBody(i); s = b ? String(plain(b.status) ?? plain(b.Status) ?? 'null') : 'null'; }
    const category = s === 'success' ? 'Success' : (s === 'failure' || s === 'fail') ? 'Fail'
      : (!s || s === 'null') ? 'null' : 'other';
    return { category, dims: { responseStatus: s, message: cleanMessage(bodyMessage(parseBody(i))) } };
  },
};

// ─── CrmPaisa (categories are the raw response status) ───────────────────────

const crmpaisa = {
  key: 'crmpaisa', table: 'crmPaisa_response_logs', displayName: 'CrmPaisa', group: 'lender',
  categories: null, // dynamic: whatever responseStatus values appear
  successKeys: ['200', '201', 'success', 'Success'],
  dimensions: LOG_DIMS,
  metrics: [],
  uniques: [{ key: 'lead', label: 'Unique leads', value: (i) => i.leadId }],
  classify(i) {
    const s = rs(i);
    const body = parseBody(i);
    return { category: s, dims: { responseStatus: s, message: cleanMessage(body && body.message) } };
  },
};

// ─── CreditLinks / CreditLinks Gold ──────────────────────────────────────────

function creditLinksClassify(i) {
  let s;
  if (i.responseStatus) s = String(i.responseStatus);
  else {
    const b = parseBody(i);
    s = !b ? 'other' : b.leadId ? 'LEAD_CREATED' : String(b.success) === 'false' ? 'FAILED' : 'other';
  }
  const category = ['LEAD_CREATED', 'ALREADY_EXISTS', 'NOT_ELIGIBLE', 'FAILED'].includes(s) ? s : 'other';
  const offers = typeof i.offersCount === 'number' ? i.offersCount : null;
  return {
    category,
    dims: {
      responseStatus: s,
      message: cleanMessage(bodyMessage(parseBody(i))),
      offers: offers === null ? null : offers > 0 ? (offers >= 5 ? '5+ offers' : `${offers} offer${offers > 1 ? 's' : ''}`) : 'No offers',
    },
    metrics: { offers: offers && offers > 0 ? offers : null },
  };
}

const creditlinks = {
  key: 'creditlinks', table: 'credit_links_response_logs', displayName: 'CreditLinks', group: 'lender',
  categories: [
    cat('LEAD_CREATED', 'Lead created', tone.good), cat('ALREADY_EXISTS', 'Already exists', tone.good),
    cat('NOT_ELIGIBLE', 'Not eligible', tone.bad), cat('FAILED', 'Failed', tone.bad), cat('other', 'Other', tone.neutral),
  ],
  successKeys: ['LEAD_CREATED', 'ALREADY_EXISTS'],
  dimensions: [...LOG_DIMS, { key: 'offers', label: 'Offers returned', order: ['No offers', '1 offer', '2 offers', '3 offers', '4 offers', '5+ offers'] }],
  metrics: [{ key: 'offers', label: 'Offers per lead (with offers)', format: 'num' }],
  uniques: [{ key: 'lead', label: 'Unique leads', value: (i) => i.leadId }],
  classify: creditLinksClassify,
};

const creditlinksgold = {
  ...creditlinks,
  key: 'creditlinksgold', table: 'credit_links_gold_response_logs', displayName: 'CreditLinks Gold',
};

// ─── Kamakshi Money ──────────────────────────────────────────────────────────

const kamakshimoney = {
  key: 'kamakshimoney', table: 'kamakshi_money_response_logs', displayName: 'Kamakshi Money', group: 'lender',
  categories: [
    cat('ATTRIBUTABLE', 'Attributable', tone.good), cat('NOT_ATTRIBUTABLE', 'Not attributable', tone.bad),
    cat('FAILED', 'Failed', tone.bad), cat('other', 'Other', tone.neutral),
  ],
  successKeys: ['ATTRIBUTABLE'],
  dimensions: [...LOG_DIMS, { key: 'reason', label: 'Decision reason' }],
  metrics: [],
  uniques: [{ key: 'lead', label: 'Unique leads', value: (i) => i.leadId }],
  classify(i) {
    const body = parseBody(i);
    let s;
    if (i.responseStatus) s = String(i.responseStatus);
    else s = !body ? 'other' : body.can_attribute === true ? 'ATTRIBUTABLE' : body.can_attribute === false ? 'NOT_ATTRIBUTABLE' : 'FAILED';
    const category = ['ATTRIBUTABLE', 'NOT_ATTRIBUTABLE', 'FAILED'].includes(s) ? s : 'other';
    return {
      category,
      dims: { responseStatus: s, message: cleanMessage(bodyMessage(body)), reason: i.reason || (body && body.reason) || null },
    };
  },
};

// ─── RCS queue ───────────────────────────────────────────────────────────────

const rcs_queue = {
  key: 'rcs_queue', table: 'rcs_queue', displayName: 'RCS Queue', group: 'queue',
  dateOf: (i) => i.scheduledTime || i.createdAt,
  sourceOf: (i) => i.lenderName || 'unknown',
  sourceLabel: 'Lender',
  categories: [
    cat('SENT', 'Sent', tone.good), cat('PENDING', 'Pending', tone.neutral),
    cat('FAILED', 'Failed', tone.bad), cat('CANCELLED', 'Cancelled', tone.warn), cat('other', 'Other', tone.neutral),
  ],
  successKeys: ['SENT'],
  dimensions: [
    { key: 'rcsType', label: 'RCS type' },
    { key: 'attempts', label: 'Attempts', order: ['0', '1', '2', '3', '4', '5'] },
    { key: 'failureReason', label: 'Failure reasons', kind: 'table' },
    { key: 'priority', label: 'Priority' },
  ],
  metrics: [],
  uniques: [{ key: 'lead', label: 'Unique leads', value: (i) => i.leadId }],
  alerts: [{ type: 'rcs' }],
  classify(i) {
    const st = i.status || 'unknown';
    return {
      category: ['SENT', 'PENDING', 'FAILED', 'CANCELLED'].includes(st) ? st : 'other',
      dims: {
        rcsType: i.rcsType || 'unknown', attempts: String(i.attempts ?? '0'),
        failureReason: cleanMessage(i.failureReason), priority: has(i.priority) ? String(i.priority) : null,
      },
    };
  },
};

// ─── registry (dashboard order) ──────────────────────────────────────────────

const SPECS = [
  leads, fatakpay, fatakpaypl, ovly, lendingplate, mpokket, ramfincrop, indialends, zype,
  creditsea, credithaat, creditlinks, creditlinksgold, creditpulse, crmpaisa, kamakshimoney, rcs_queue,
];

const BY_KEY = Object.fromEntries(SPECS.map((s) => [s.key, s]));
const BY_TABLE = Object.fromEntries(SPECS.map((s) => [s.table, s]));

/** JSON-safe description of a spec for the dashboard. */
function publicSpec(s) {
  return {
    key: s.key, table: s.table, displayName: s.displayName, group: s.group,
    categories: s.categories, successKeys: s.successKeys, successLabel: s.successLabel || 'Success rate',
    sourceLabel: s.sourceLabel || 'Source',
    dimensions: s.dimensions, metrics: s.metrics,
    uniques: (s.uniques || []).map((u) => ({ key: u.key, label: u.label })),
  };
}

module.exports = { SPECS, BY_KEY, BY_TABLE, publicSpec, parseBody };

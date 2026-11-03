const SOURCE_ALIASES = {
  ratecut: 'Ratecut',

  ratecutpl: 'RatecutPL',

  ck: 'CashKuber',
  cashkuber: 'CashKuber',

  ck2: 'CK2',

  fr: 'FREO',
  freo: 'FREO',

  fr2: 'FREO2',
  freo2: 'FREO2',

  wc: 'WeCredit',
  WeCredit: 'WeCredit',
  wecredit: 'WeCredit',

  ap: 'Apr',
  apr: 'Apr',

  bls: 'Blostem',
  blostem: 'Blostem',

  bosc: 'BoostScore',
  bos: 'BoostScore',
  boc: 'BoostScore',
  boostscore: 'BoostScore',

  bs: 'BatterySmart',
  batterysmart: 'BatterySmart',

  vfc: 'VFC',

  HSB: 'HSB',
  hsb: 'HSB',

  ANU: 'ANU',
  anu: 'ANU'
};

const PARTNER_NUMBERS = {
  Ratecut: '01',
  RatecutPL: '02',
  CK2: '03',
  CashKuber: '04',
  FREO: '05',
  WeCredit: '06',
  FREO2: '07',
  Apr: '08',
  Blostem: '09',
  BoostScore: '10',
  ANU: '11',
  BatterySmart: '12',
  VFC: '13',
  HSB: '14',
};

const UNKNOWN_PARTNER_NUMBER = '00';

function resolveSource(input) {
  if (!input) return input;
  const key = String(input).toLowerCase();
  return SOURCE_ALIASES[key] || input;
}

// Partner number → canonical source ("05" → "FREO").
const PARTNER_BY_NUMBER = Object.fromEntries(
  Object.entries(PARTNER_NUMBERS).map(([source, num]) => [num, source])
);

// Partner reference IDs look like RC{2-digit partner}{12-24 base36}, e.g. RC04MVQPZRH486PQ7VHI.
const PARTNER_REF_RE = /^RC(\d{2})[0-9A-Z]{12,24}$/i;

// Sub-sources that belong to the same real partner for disbursal/status reporting.
const MERGED_SOURCES = {
  CK2: 'CashKuber',
  FREO2: 'FREO',
};
const merged = (src) => (src ? MERGED_SOURCES[src] || src : null);

/**
 * Canonical source from a UTM value (source / medium / campaign), or null.
 * Handles:
 *   - partner reference IDs: "RC04MVQPZRH486PQ7VHI" → CashKuber (via PARTNER_NUMBERS)
 *   - any alias in SOURCE_ALIASES, case-insensitive: "fr2" → FREO2, "bosc" → BoostScore
 *   - values with trailing junk: "fr%26p13%3d…", "ck2_campaign", "wc-123" → leading token
 * "applynow1" stays unmatched (its leading token is not an alias), so callers fall back.
 */
function sourceFromUtm(input) {
  return merged(_sourceFromUtm(input));
}

function _sourceFromUtm(input) {
  if (input === null || input === undefined) return null;
  let raw = String(input).trim();
  if (!raw) return null;
  try { raw = decodeURIComponent(raw); } catch { /* keep as-is */ }
  raw = raw.trim();

  const ref = raw.match(PARTNER_REF_RE);
  if (ref) return PARTNER_BY_NUMBER[ref[1]] || null;

  const low = raw.toLowerCase();
  if (SOURCE_ALIASES[low]) return SOURCE_ALIASES[low];

  const alnum = (low.match(/^[a-z0-9]+/) || [''])[0];   // "ck2_x" → "ck2"
  if (alnum && SOURCE_ALIASES[alnum]) return SOURCE_ALIASES[alnum];

  const alpha = (low.match(/^[a-z]+/) || [''])[0];      // "fr%26…" / "freo123" → "fr" / "freo"
  if (alpha && SOURCE_ALIASES[alpha]) return SOURCE_ALIASES[alpha];

  return null;
}

function getPartnerNumber(input) {
  if (!input) return UNKNOWN_PARTNER_NUMBER;
  const canonical = resolveSource(input);
  return PARTNER_NUMBERS[canonical] || PARTNER_NUMBERS[input] || UNKNOWN_PARTNER_NUMBER;
}

module.exports = {
  SOURCE_ALIASES,
  PARTNER_NUMBERS,
  UNKNOWN_PARTNER_NUMBER,
  resolveSource,
  getPartnerNumber,
  sourceFromUtm,
  MERGED_SOURCES,
};

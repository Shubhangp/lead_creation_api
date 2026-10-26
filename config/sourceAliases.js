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
};

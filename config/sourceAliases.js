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

  HSB: 'HSB'
};

function resolveSource(input) {
  if (!input) return input;
  const key = String(input).toLowerCase();
  return SOURCE_ALIASES[key] || input;
}

module.exports = { SOURCE_ALIASES, resolveSource };

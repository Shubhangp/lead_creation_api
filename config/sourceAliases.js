const SOURCE_ALIASES = {
  ratecut: 'Ratecut',

  ratecutpl: 'RatecutPL',

  ck: 'CashKuber',
  cashkuber: 'CashKuber',

  fr: 'FREO',
  freo: 'FREO',

  ap: 'Apr',
  apr: 'Apr',

  bls: 'Blostem',
  blostem: 'Blostem',

  bs: 'BatterySmart',
  batterysmart: 'BatterySmart',

  vfc: 'VFC',
};

function resolveSource(input) {
  if (!input) return input;
  const key = String(input).toLowerCase();
  return SOURCE_ALIASES[key] || input;
}

module.exports = { SOURCE_ALIASES, resolveSource };

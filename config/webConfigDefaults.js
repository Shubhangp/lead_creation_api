const { LENDER_CATALOG, buildLenderList } = require('./lenderCatalog');

function buildDefaultWebConfig() {
  return {
    redirectToSuccess: true,
    formMode: 'full',
    lenders: buildLenderList(),
  };
}

const CATALOG_BY_CODE = (() => {
  const map = new Map();
  for (const [key, entry] of Object.entries(LENDER_CATALOG)) {
    map.set(key, entry);
    if (entry.code) map.set(entry.code, entry);
  }
  return map;
})();

const CATALOG_BY_UNIQUE_NAME = (() => {
  const seen = new Map();
  for (const entry of Object.values(LENDER_CATALOG)) {
    seen.set(entry.name, (seen.get(entry.name) || 0) + 1);
  }
  const map = new Map();
  for (const entry of Object.values(LENDER_CATALOG)) {
    if (seen.get(entry.name) === 1) map.set(entry.name, entry);
  }
  return map;
})();


function rehydrateLenders(lenders) {
  return lenders
    .map((lender) => {
      if (!lender || typeof lender !== 'object') return null;

      const fresh =
        (lender.code && CATALOG_BY_CODE.get(lender.code)) ||
        (lender.name && CATALOG_BY_UNIQUE_NAME.get(lender.name)) ||
        null;

      return fresh ? { ...fresh } : lender;
    })
    .filter(Boolean);
}

function normalizeWebConfig(input) {
  if (!input || typeof input !== 'object') return buildDefaultWebConfig();

  const redirectToSuccess = !!input.redirectToSuccess;
  const formMode = input.formMode === 'mobileOnly' ? 'mobileOnly' : 'full';

  let lenders;
  if (Array.isArray(input.lenderCodes)) {
    lenders = buildLenderList(input.lenderCodes.length ? input.lenderCodes : null);
  } else if (Array.isArray(input.lenders) && input.lenders.length) {
    lenders = rehydrateLenders(input.lenders);
  } else {
    lenders = buildLenderList();
  }

  return { redirectToSuccess, formMode, lenders };
}

module.exports = { buildDefaultWebConfig, normalizeWebConfig };

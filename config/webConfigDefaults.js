const { LENDER_CATALOG, buildLenderList } = require('./lenderCatalog');

function buildDefaultWebConfig() {
  return {
    redirectToSuccess: true,
    formMode: 'full',
    lenders: buildLenderList(),
  };
}

/**
 * Reverse index for resolving a STORED lender object back to its live catalog
 * entry. Keyed by both the catalog key and the entry's own `code` — they are
 * normally identical, but the two are indexed separately so a future entry
 * whose key and code diverge still resolves either way.
 */
const CATALOG_BY_CODE = (() => {
  const map = new Map();
  for (const [key, entry] of Object.entries(LENDER_CATALOG)) {
    map.set(key, entry);
    if (entry.code) map.set(entry.code, entry);
  }
  return map;
})();

/**
 * Name-based fallback for snapshots written before every entry carried a
 * `code`. Only names that identify exactly ONE catalog entry are indexed:
 * 'Poonawalla Fincorp' belongs to both PoonawallaFincorp and PoonawallaCH, so
 * it is deliberately absent here rather than resolving to an arbitrary one.
 */
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

/**
 * Refresh stored lender objects against the live catalog.
 *
 * A webConfig saved on a distribution_rules row carries FULL lender objects,
 * so every field — website URL included — is frozen at the moment it was
 * saved. Editing config/lenderCatalog.js would otherwise never reach those
 * rows, and the landing page would keep serving the URL from whenever the
 * dashboard last wrote that source.
 *
 * Re-hydration makes the catalog authoritative again while leaving the STORED
 * ORDER intact — the order is a per-source editorial choice, the lender
 * details are not. Entries with no catalog match (a retired lender, a bespoke
 * one-off) are passed through untouched so nothing silently disappears from a
 * page that is live right now.
 */
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

/**
 * Normalize an incoming webConfig payload (from the dashboard or API) into the
 * canonical shape stored on the distribution_rules row. The dashboard sends a
 * compact shape:
 *   { redirectToSuccess: bool, formMode: 'full'|'mobileOnly', lenderCodes: [..] }
 * while older/direct callers may already send full `lenders` objects. Either
 * way the result carries FULL lender objects (with the {utm_medium} token
 * intact) so the frontend can render without a catalog — but those objects are
 * always sourced from the catalog, never from a stale snapshot.
 *
 * An empty/omitted lenderCodes means "use the default order".
 */
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

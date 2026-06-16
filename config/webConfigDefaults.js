const { buildLenderList } = require('./lenderCatalog');

function buildDefaultWebConfig() {
  return {
    redirectToSuccess: true,
    formMode: 'full',
    lenders: buildLenderList(),
  };
}

/**
 * Normalize an incoming webConfig payload (from the dashboard or API) into the
 * canonical shape stored on the distribution_rules row. The dashboard sends a
 * compact shape:
 *   { redirectToSuccess: bool, formMode: 'full'|'mobileOnly', lenderCodes: [..] }
 * while older/direct callers may already send full `lenders` objects. Either
 * way the stored webConfig always carries FULL lender objects (with the
 * {utm_medium} token intact) so the frontend can render without a catalog.
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
    lenders = input.lenders;
  } else {
    lenders = buildLenderList();
  }

  return { redirectToSuccess, formMode, lenders };
}

module.exports = { buildDefaultWebConfig, normalizeWebConfig };

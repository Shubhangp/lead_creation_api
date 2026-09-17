const DistributionRule = require('../models/distributionRuleModel');
const { buildDefaultWebConfig, normalizeWebConfig } = require('../config/webConfigDefaults');
const { resolveSource } = require('../config/sourceAliases');

async function resolveWebConfigForSource(source) {
  const canonical = resolveSource(source);

  let webConfig = null;
  let resolvedFrom = 'builtin-default';

  let rule = await DistributionRule.findBySource(source);
  if ((!rule || !rule.webConfig) && canonical !== source) {
    rule = await DistributionRule.findBySource(canonical);
  }

  if (rule && rule.webConfig) {
    webConfig = rule.webConfig;
    resolvedFrom = 'source';
  } else {
    const defaultRule = await DistributionRule.findBySource('default');
    if (defaultRule && defaultRule.webConfig) {
      webConfig = defaultRule.webConfig;
      resolvedFrom = 'default-row';
    }
  }

  if (!webConfig) {
    webConfig = buildDefaultWebConfig();
  }

  return { webConfig: normalizeWebConfig(webConfig), resolvedFrom };
}

module.exports = { resolveWebConfigForSource };

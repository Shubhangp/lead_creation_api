const DistributionRule = require('../models/distributionRuleModel');
const { buildDefaultWebConfig, normalizeWebConfig } = require('../config/webConfigDefaults');
const { resolveSource } = require('../config/sourceAliases');
const { resolveWebConfigForSource } = require('../services/webConfigService');

// Get all distribution rules
exports.getAllDistributionRules = async (req, res) => {
  try {
    const result = await DistributionRule.findAll();
    res.status(200).json({
      status: 'success',
      results: result.items.length,
      data: {
        rules: result.items
      }
    });
  } catch (error) {
    res.status(500).json({
      status: 'error',
      message: 'Failed to fetch distribution rules',
      error: error.message
    });
  }
};

// Get distribution rule by source
exports.getDistributionRuleBySource = async (req, res) => {
  try {
    const { source } = req.params;
    const rule = await DistributionRule.findBySource(source);
    
    if (!rule) {
      return res.status(404).json({
        status: 'fail',
        message: `No distribution rule found for source: ${source}`
      });
    }
    
    res.status(200).json({
      status: 'success',
      data: {
        rule
      }
    });
  } catch (error) {
    res.status(500).json({
      status: 'error',
      message: 'Failed to fetch distribution rule',
      error: error.message
    });
  }
};

// Create a new distribution rule
exports.createDistributionRule = async (req, res) => {
  try {
    const { source, rules, active = true, rcsConfig, webConfig } = req.body;

    // Validate required fields
    if (!source || !rules || !rules.immediate || !rules.delayed) {
      return res.status(400).json({
        status: 'fail',
        message: 'Missing required fields: source, rules.immediate, rules.delayed'
      });
    }
    
    // Check if rule already exists
    const existingRule = await DistributionRule.findBySource(source);
    if (existingRule) {
      return res.status(409).json({
        status: 'fail',
        message: `Distribution rule for source ${source} already exists`
      });
    }
    
    // Prepare rule data
    const ruleData = {
      source: source,
      rules,
      active,
      lastUpdatedBy: req.user ? req.user.email : 'system'
    };
    
    // Add rcsConfig if provided
    if (rcsConfig) {
      ruleData.rcsConfig = rcsConfig;
    }

    // Add webConfig (landing-page config) if provided — additive, independent
    // of the S2S `rules` / `rcsConfig`.
    if (webConfig) {
      ruleData.webConfig = normalizeWebConfig(webConfig);
    }

    // Create new rule
    const newRule = await DistributionRule.create(ruleData);
    
    res.status(201).json({
      status: 'success',
      data: {
        rule: newRule
      }
    });
  } catch (error) {
    res.status(500).json({
      status: 'error',
      message: 'Failed to create distribution rule',
      error: error.message
    });
  }
};

// Update a distribution rule
exports.updateDistributionRule = async (req, res) => {
  try {
    const { source } = req.params;
    const { rules, active, rcsConfig, webConfig } = req.body;

    const updateData = {};
    if (rules) updateData.rules = rules;
    if (active !== undefined) updateData.active = active;
    if (rcsConfig) updateData.rcsConfig = rcsConfig;
    if (webConfig) updateData.webConfig = normalizeWebConfig(webConfig);
    
    updateData.lastUpdatedBy = req.user ? req.user.email : 'system';
    
    const updatedRule = await DistributionRule.updateBySource(source, updateData);
    
    if (!updatedRule) {
      return res.status(404).json({
        status: 'fail',
        message: `No distribution rule found for source: ${source}`
      });
    }
    
    res.status(200).json({
      status: 'success',
      data: {
        rule: updatedRule
      }
    });
  } catch (error) {
    res.status(500).json({
      status: 'error',
      message: 'Failed to update distribution rule',
      error: error.message
    });
  }
};

// Delete a distribution rule
exports.deleteDistributionRule = async (req, res) => {
  try {
    const { source } = req.params;
    const deletedRule = await DistributionRule.deleteBySource(source);
    
    if (!deletedRule) {
      return res.status(404).json({
        status: 'fail',
        message: `No distribution rule found for source: ${source}`
      });
    }
    
    res.status(200).json({
      status: 'success',
      message: `Distribution rule for source ${source} successfully deleted`
    });
  } catch (error) {
    res.status(500).json({
      status: 'error',
      message: 'Failed to delete distribution rule',
      error: error.message
    });
  }
};

// ============================================================================
// WEB (LANDING) CONFIG — powers /form + /success. Additive to the S2S system.
// ============================================================================

// Resolve the effective webConfig for a source, with layered fallback:
//   1. the source's own webConfig
//   2. the 'default' source's webConfig
//   3. the built-in default (redirect + full backend lender list)
exports.getWebConfigBySource = async (req, res) => {
  try {
    const { source } = req.params;

    // Resolution (including the nick -> canonical-name step and the expansion
    // of a compact `lenderCodes` row into full lender objects) lives in
    // webConfigService, shared with the partner-reference endpoint so the page
    // and the reference gate can never read the flags differently.
    const { webConfig, resolvedFrom } = await resolveWebConfigForSource(source);

    res.status(200).json({
      status: 'success',
      data: {
        source,
        resolvedFrom,
        webConfig,
      },
    });
  } catch (error) {
    // Even on failure, hand the frontend a safe default so the page still works.
    res.status(200).json({
      status: 'success',
      data: {
        source: req.params.source,
        resolvedFrom: 'builtin-default-on-error',
        webConfig: buildDefaultWebConfig(),
      },
      warning: error.message,
    });
  }
};

// Upsert ONLY the webConfig for a source without disturbing its S2S `rules` /
// `rcsConfig`. Creates a landing-only row (empty S2S rules) if none exists.
// This is the endpoint the future dashboard will call.
exports.upsertWebConfigBySource = async (req, res) => {
  try {
    const { source } = req.params;

    if (!req.body.webConfig || typeof req.body.webConfig !== 'object') {
      return res.status(400).json({
        status: 'fail',
        message: 'webConfig object is required',
      });
    }

    const webConfig = normalizeWebConfig(req.body.webConfig);

    const existing = await DistributionRule.findBySource(source);

    if (existing) {
      const updated = await DistributionRule.updateBySource(source, {
        webConfig,
        lastUpdatedBy: req.user ? req.user.email : 'system',
      });
      return res.status(200).json({
        status: 'success',
        data: { rule: updated },
      });
    }

    // No row yet — create a landing-only row with empty S2S rules so the
    // distribution/RCS pipeline is unaffected.
    const created = await DistributionRule.create({
      source,
      rules: { immediate: [], delayed: [] },
      active: true,
      webConfig,
      lastUpdatedBy: req.user ? req.user.email : 'system',
    });

    res.status(201).json({
      status: 'success',
      data: { rule: created },
    });
  } catch (error) {
    res.status(500).json({
      status: 'error',
      message: 'Failed to upsert web config',
      error: error.message,
    });
  }
};
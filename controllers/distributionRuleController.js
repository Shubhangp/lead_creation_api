const DistributionRule = require('../models/distributionRuleModel');

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
    const { source, rules, active = true, rcsConfig } = req.body;
    
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
    const { rules, active, rcsConfig } = req.body;
    
    const updateData = {};
    if (rules) updateData.rules = rules;
    if (active !== undefined) updateData.active = active;
    if (rcsConfig) updateData.rcsConfig = rcsConfig;
    
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
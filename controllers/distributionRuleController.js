const DistributionRule = require('../models/distributionRuleModel');

// Get all distribution rules
exports.getAllDistributionRules = async (req, res) => {
  try {
    const rules = await DistributionRule.find({});
    res.status(200).json({
      status: 'success',
      results: rules.length,
      data: {
        rules
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
    const rule = await DistributionRule.findOne({ source: source.toUpperCase() });
    
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
    const { source, rules, active = true } = req.body;
    
    // Validate required fields
    if (!source || !rules || !rules.immediate || !rules.delayed) {
      return res.status(400).json({
        status: 'fail',
        message: 'Missing required fields: source, rules.immediate, rules.delayed'
      });
    }
    
    // Check if rule already exists
    const existingRule = await DistributionRule.findOne({ source: source.toUpperCase() });
    if (existingRule) {
      return res.status(409).json({
        status: 'fail',
        message: `Distribution rule for source ${source} already exists`
      });
    }
    
    // Create new rule
    const newRule = await DistributionRule.create({
      source: source.toUpperCase(),
      rules,
      active,
      lastUpdatedBy: req.user ? req.user.email : 'system'
    });
    
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
    const { rules, active } = req.body;
    
    const updateData = {};
    if (rules) updateData.rules = rules;
    if (active !== undefined) updateData.active = active;
    updateData.lastUpdated = Date.now();
    updateData.lastUpdatedBy = req.user ? req.user.email : 'system';
    
    const updatedRule = await DistributionRule.findOneAndUpdate(
      { source: source.toUpperCase() },
      updateData,
      { new: true, runValidators: true }
    );
    
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
    const deletedRule = await DistributionRule.findOneAndDelete({ source: source.toUpperCase() });
    
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
const mongoose = require('mongoose');

const distributionRuleSchema = new mongoose.Schema({
  source: {
    type: String,
    required: true,
    unique: true,
    trim: true
  },
  active: {
    type: Boolean,
    default: true
  },
  rules: {
    immediate: [{
      type: String,
      enum: ['SML', 'FREO', 'OVLY', 'LendingPlate', 'ZYPE', 'FINTIFI', 'FATAKPAY', 'RAMFINCROP', 'MyMoneyMantra']
    }],
    delayed: [{
      lender: {
        type: String,
        enum: ['SML', 'FREO', 'OVLY', 'LendingPlate', 'ZYPE', 'FINTIFI', 'FATAKPAY', 'RAMFINCROP', 'MyMoneyMantra']
      },
      delayMinutes: {
        type: Number,
        min: 0,
        default: 30
      }
    }]
  },
  lastUpdated: {
    type: Date,
    default: Date.now
  },
  lastUpdatedBy: {
    type: String
  }
}, { timestamps: true });

// distributionRuleSchema.index({ source: 1 });

const DistributionRule = mongoose.model('DistributionRule', distributionRuleSchema);

module.exports = DistributionRule;
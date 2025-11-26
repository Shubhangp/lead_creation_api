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
      enum: ['SML', 'FREO', 'OVLY', 'LendingPlate', 'ZYPE', 'FINTIFI', 'FATAKPAY', 'RAMFINCROP', 'MyMoneyMantra', 'INDIALENDS']
    }],
    delayed: [{
      lender: {
        type: String,
        enum: ['SML', 'FREO', 'OVLY', 'LendingPlate', 'ZYPE', 'FINTIFI', 'FATAKPAY', 'RAMFINCROP', 'MyMoneyMantra', 'INDIALENDS']
      },
      delayMinutes: {
        type: Number,
        min: 0,
        default: 30
      }
    }]
  },
  // RCS Configuration
  rcsConfig: {
    enabled: {
      type: Boolean,
      default: true
    },
    lenderPriority: [{
      lender: {
        type: String,
        enum: ['SML', 'FREO', 'OVLY', 'LendingPlate', 'ZYPE', 'FINTIFI', 'FATAKPAY', 'RAMFINCROP', 'MyMoneyMantra', 'INDIALENDS'],
        required: true
      },
      priority: {
        type: Number,
        required: true,
        min: 1
      },
      rcsDayDelay: {
        type: Number,
        default: 0,
        min: 0
      }
    }],
    zetCampaign: {
      enabled: {
        type: Boolean,
        default: true
      },
      dayDelay: {
        type: Number,
        default: 1,
        min: 0
      }
    },
    operatingHours: {
      startTime: {
        type: String,
        default: '10:00',
        validate: {
          validator: function(v) {
            return /^([01]?[0-9]|2[0-3]):[0-5][0-9]$/.test(v);
          },
          message: 'Invalid time format. Use HH:MM'
        }
      },
      endTime: {
        type: String,
        default: '19:00',
        validate: {
          validator: function(v) {
            return /^([01]?[0-9]|2[0-3]):[0-5][0-9]$/.test(v);
          },
          message: 'Invalid time format. Use HH:MM'
        }
      },
      timezone: {
        type: String,
        default: 'Asia/Kolkata'
      }
    }
  },
  lastUpdated: {
    type: Date,
    default: Date.now
  },
  lastUpdatedBy: {
    type: String
  }
}, { timestamps: true });

// Add index for better performance
distributionRuleSchema.index({ 'rcsConfig.lenderPriority.priority': 1 });

// Pre-save middleware to sort lender priority
distributionRuleSchema.pre('save', function(next) {
  if (this.rcsConfig && this.rcsConfig.lenderPriority) {
    this.rcsConfig.lenderPriority.sort((a, b) => a.priority - b.priority);
  }
  next();
});

const DistributionRule = mongoose.model('DistributionRule', distributionRuleSchema);

module.exports = DistributionRule;
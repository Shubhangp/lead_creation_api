const mongoose = require('mongoose');

const indiaLendsResponseLogSchema = new mongoose.Schema(
  {
    leadId: {
      type: mongoose.Schema.Types.ObjectId,
      ref: 'Lead',
      required: true,
      index: true
    },
    source: {
      type: String,
      required: true
    },
    accessToken: {
      type: String,
      default: null
    },
    dedupCheck: {
      type: mongoose.Schema.Types.Mixed,
      default: null
    },
    isDuplicate: {
      type: Boolean,
      default: false,
      index: true
    },
    duplicateStatus: {
      type: String,
      enum: ['0', '1', '2'],
      default: '0'
    },
    requestPayload: {
      type: mongoose.Schema.Types.Mixed,
      required: true
    },
    responseStatus: {
      type: Number,
      required: true,
      index: true
    },
    responseBody: {
      type: mongoose.Schema.Types.Mixed,
      required: true
    },
    errorDetails: {
      message: { type: String },
      code: { type: String },
      stack: { type: String }
    },
    retryCount: {
      type: Number,
      default: 0
    },
    isSuccess: {
      type: Boolean,
      default: function() {
        return this.responseStatus >= 200 && this.responseStatus < 300;
      },
      index: true
    }
  },
  {
    timestamps: true
  }
);

// Index for efficient querying
indiaLendsResponseLogSchema.index({ leadId: 1, createdAt: -1 });
indiaLendsResponseLogSchema.index({ source: 1, createdAt: -1 });
indiaLendsResponseLogSchema.index({ isDuplicate: 1, isSuccess: 1 });

// Virtual for checking if verification was sent
indiaLendsResponseLogSchema.virtual('verificationSent').get(function() {
  return this.responseBody?.info?.message?.includes('Verification code sent');
});

// Method to check if lead was successful
indiaLendsResponseLogSchema.methods.isLeadSuccessful = function() {
  return (
    this.isSuccess &&
    !this.isDuplicate &&
    this.responseBody?.info?.status === 100
  );
};

// Static method to get duplicate leads
indiaLendsResponseLogSchema.statics.getDuplicateLeads = function(startDate, endDate) {
  return this.find({
    isDuplicate: true,
    createdAt: { $gte: startDate, $lte: endDate }
  }).populate('leadId');
};

// Static method to get success rate
indiaLendsResponseLogSchema.statics.getSuccessRate = async function(startDate, endDate) {
  const total = await this.countDocuments({
    createdAt: { $gte: startDate, $lte: endDate }
  });
  
  const successful = await this.countDocuments({
    createdAt: { $gte: startDate, $lte: endDate },
    isSuccess: true,
    isDuplicate: false
  });
  
  return {
    total,
    successful,
    successRate: total > 0 ? (successful / total) * 100 : 0
  };
};

const indiaLendsResponseLog = mongoose.model(
  'IndiaLendsResponseLog',
  indiaLendsResponseLogSchema
);

module.exports = indiaLendsResponseLog;
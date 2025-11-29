const mongoose = require('mongoose');

const rcsQueueSchema = new mongoose.Schema({
  leadId: {
    type: mongoose.Schema.Types.ObjectId,
    ref: 'Lead',
    required: true
  },
  phone: {
    type: String,
    required: true
  },
  rcsType: {
    type: String,
    enum: ['LENDER_SUCCESS', 'ZET_CAMPAIGN'],
    required: true
  },
  lenderName: {
    type: String,
    enum: ['SML', 'FREO', 'OVLY', 'LendingPlate', 'ZYPE', 'FINTIFI', 'FATAKPAY', 'RAMFINCROP', 'MyMoneyMantra'],
    required: function() { return this.rcsType === 'LENDER_SUCCESS'; }
  },
  priority: {
    type: Number,
    required: function() { return this.rcsType === 'LENDER_SUCCESS'; }
  },
  scheduledTime: {
    type: Date,
    required: true
  },
  status: {
    type: String,
    enum: ['PENDING', 'SENT', 'FAILED', 'CANCELLED'],
    default: 'PENDING'
  },
  attempts: {
    type: Number,
    default: 0,
    max: 1
  },
  sentAt: {
    type: Date
  },
  failureReason: {
    type: String
  },
  rcsPayload: {
    type: mongoose.Schema.Types.Mixed
  },
  rcsResponse: {
    type: mongoose.Schema.Types.Mixed
  }
}, { timestamps: true });

// Indexes for better performance
rcsQueueSchema.index({ leadId: 1, rcsType: 1 });
rcsQueueSchema.index({ scheduledTime: 1, status: 1 });
rcsQueueSchema.index({ status: 1, attempts: 1 });

const RCSQueue = mongoose.model('RCSQueue', rcsQueueSchema);

// models/rcsLogModel.js
// const rcsLogSchema = new mongoose.Schema({
//   leadId: {
//     type: mongoose.Schema.Types.ObjectId,
//     ref: 'Lead',
//     required: true
//   },
//   queueId: {
//     type: mongoose.Schema.Types.ObjectId,
//     ref: 'RCSQueue',
//     required: true
//   },
//   phone: {
//     type: String,
//     required: true
//   },
//   rcsType: {
//     type: String,
//     enum: ['LENDER_SUCCESS', 'ZET_CAMPAIGN'],
//     required: true
//   },
//   lenderName: {
//     type: String
//   },
//   requestPayload: {
//     type: mongoose.Schema.Types.Mixed,
//     required: true
//   },
//   responseStatus: {
//     type: Number
//   },
//   responseBody: {
//     type: mongoose.Schema.Types.Mixed
//   },
//   sentAt: {
//     type: Date,
//     default: Date.now
//   },
//   success: {
//     type: Boolean,
//     required: true
//   },
//   errorMessage: {
//     type: String
//   }
// }, { timestamps: true });

// rcsLogSchema.index({ leadId: 1 });
// rcsLogSchema.index({ sentAt: 1 });
// rcsLogSchema.index({ success: 1 });

// const RCSLog = mongoose.model('RCSLog', rcsLogSchema);

module.exports = {
  RCSQueue,
  // RCSLog
};
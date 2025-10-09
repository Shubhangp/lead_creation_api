const mongoose = require('mongoose');

const ivrCallSchema = new mongoose.Schema({
  callId: {
    type: String,
    required: true,
    unique: true,
    index: true
  },
  uuid: {
    type: String,
    required: true
  },
  phoneNumber: {
    type: String,
    required: true,
    index: true
  },
  phoneNumberWithPrefix: {
    type: String,
    required: true
  },
  ourNumber: {
    type: String,
    required: true
  },
  digitPressed: {
    type: String,
    required: true
  },
  lenderId: {
    type: mongoose.Schema.Types.ObjectId,
    ref: 'Lender',
    required: false
  },
  rcsMessageSent: {
    type: Boolean,
    default: false
  },
  rcsMessageId: {
    type: String,
    default: null
  },
  billingCircle: {
    type: Object
  },
  rawIvrData: {
    type: mongoose.Schema.Types.Mixed,
    required: true
  }
}, {
  timestamps: true
});

// Index for querying by date range
ivrCallSchema.index({ createdAt: -1 });

// Index for analytics queries
ivrCallSchema.index({ digitPressed: 1, rcsMessageSent: 1 });

const IvrCall = mongoose.model('IvrCall', ivrCallSchema);

module.exports = IvrCall;
const mongoose = require('mongoose');

const ovlyResponseLogSchema = new mongoose.Schema({
  leadId: {
    type: mongoose.Schema.Types.ObjectId,
    ref: 'Lead',
    required: true,
  },
  requestPayload: {
    type: Object,
    required: true,
  },
  responseStatus: {
    type: Number || String,
    required: true,
  },
  responseBody: {
    type: Object,
    required: true,
  },
  createdAt: {
    type: Date,
    default: Date.now,
  },
});

module.exports = mongoose.model('OvlyResponseLog', ovlyResponseLogSchema);
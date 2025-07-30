const mongoose = require('mongoose');

const smlResponseLogSchema = new mongoose.Schema({
  leadId: {
    type: mongoose.Schema.Types.ObjectId,
    ref: 'Lead',
    // required: true,
  },
  source: {
    type: String
  },
  requestPayload: {
    type: Object,
    // required: true,
  },
  responseStatus: {
    type: Number,
    // required: true,
  },
  responseBody: {
    type: Object,
    // required: true,
  },
  createdAt: {
    type: Date,
    default: Date.now,
  },
});

module.exports = mongoose.model('SMLResponseLog', smlResponseLogSchema);
const mongoose = require('mongoose');

const zypeResponseLogSchema = new mongoose.Schema({
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
    type: String,
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

module.exports = mongoose.model('ZypeResponseLog', zypeResponseLogSchema);
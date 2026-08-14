const mongoose = require('mongoose');
const { deepEncryptFields } = require('../utils/piiCrypto');

const VrindaFintechResponseLogSchema = new mongoose.Schema({
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

// Encrypt PII (phone/PAN) nested in the request/response payloads at the storage
// boundary. Deep-walks the objects so lender-specific field names are covered.
VrindaFintechResponseLogSchema.pre('save', function (next) {
  try {
    if (this.requestPayload) this.requestPayload = deepEncryptFields(this.requestPayload);
    if (this.responseBody) this.responseBody = deepEncryptFields(this.responseBody);
  } catch (_) { /* never block a write on encryption */ }
  next();
});

module.exports = mongoose.model('VrindaFintechResponseLog', VrindaFintechResponseLogSchema);
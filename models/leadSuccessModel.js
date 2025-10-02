const mongoose = require('mongoose');

const leadSuccessSchema = new mongoose.Schema({
  leadId: {
    type: mongoose.Schema.Types.ObjectId,
    ref: 'Lead',
  },
  source: {
    type: String,
    // required: true,
  },
  phone: {
    type: String,
    // required: true,
  },
  email: {
    type: String,
    // required: true,
  },
  panNumber: {
    type: String,
    // required: true,
  },
  fullName: {
    type: String,
    // required: true,
  },
  OVLY: {
    type: Boolean,
    default: false,
    // required: true,
  },
  FREO: {
    type: Boolean,
    default: false,
    // required: true,
  },
  LendingPlate: {
    type: Boolean,
    default: false,
    // required: true,
  },
  ZYPE: {
    type: Boolean,
    default: false,
    // required: true,
  },
  FINTIFI: {
    type: Boolean,
    default: false,
    // required: true,
  },
  FATAKPAY: {
    type: Boolean,
    default: false,
    // required: true,
  },
  RAMFINCROP: {
    type: Boolean,
    default: false,
    // required: true,
  },
  MyMoneyMantra: {
    type: Boolean,
    default: false,
    // required: true,
  },
  createdAt: {
    type: Date,
    default: Date.now,
  },
});

module.exports = mongoose.model('leadSuccess', leadSuccessSchema);
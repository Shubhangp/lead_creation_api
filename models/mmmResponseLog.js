const mongoose = require('mongoose');

const mmmResponseLogSchema = new mongoose.Schema({
    leadId: {
        type: mongoose.Schema.Types.ObjectId,
        ref: 'Lead',
        // required: true,
    },
    correlationId: {
        type: String,
        // required: true,
        unique: true
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
    errorDetails: {
        type: Object,
        default: null
    },
    createdAt: {
        type: Date,
        default: Date.now,
    }
});

// Index for faster queries
mmmResponseLogSchema.index({ leadId: 1 });
mmmResponseLogSchema.index({ mmmLeadId: 1 });
mmmResponseLogSchema.index({ createdAt: -1 });

module.exports = mongoose.model('MMMResponseLog', mmmResponseLogSchema);
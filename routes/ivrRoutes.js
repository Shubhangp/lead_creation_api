const express = require('express');
const router = express.Router();
const { handleIvrWebhook, getIvrCallDetails, getAllIvrCalls, getIvrAnalytics } = require('../controllers/ivrController');

/**
 * @route   POST /api/ivr/webhook
 * @desc    Receive IVR webhook data from TATA Teleservices
 * @access  Public (should add authentication in production)
 */
router.post('/webhook', handleIvrWebhook);

/**
 * @route   GET /api/ivr/call/:callId
 * @desc    Get IVR call details by callId
 * @access  Private
 */
router.get('/call/:callId', getIvrCallDetails);

/**
 * @route   GET /api/ivr/calls
 * @desc    Get all IVR calls with filters and pagination
 * @access  Private
 * @query   page, limit, digitPressed, phoneNumber, startDate, endDate
 */
router.get('/calls', getAllIvrCalls);

/**
 * @route   GET /api/ivr/analytics
 * @desc    Get IVR analytics and statistics
 * @access  Private
 */
router.get('/analytics', getIvrAnalytics);

module.exports = router;
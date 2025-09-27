const express = require('express');
const rcsController = require('../controllers/rcsController');
const router = express.Router();

// ============ RCS PROCESSING ENDPOINTS ============
// Manual trigger to process pending RCS messages
router.post('/process-pending', rcsController.processPendingRCS);

// Reschedule failed RCS messages
router.post('/reschedule-failed', rcsController.rescheduleFailedRCS);

// ============ RCS QUEUE MANAGEMENT ============
// Get current RCS queue status and statistics
router.get('/queue/status', rcsController.getRCSQueueStatus);

// Cancel pending RCS messages for a specific lead
router.delete('/queue/lead/:leadId', rcsController.cancelRCSForLead);

// ============ RCS LOGS AND ANALYTICS ============
// Get RCS logs for a specific lead
router.get('/logs/lead/:leadId', rcsController.getRCSLogsForLead);

// Get RCS analytics and reporting data
router.get('/analytics', rcsController.getRCSAnalytics);

// ============ RCS CONFIGURATION ============
// Get RCS configuration for a source
router.get('/config/:source', rcsController.getRCSConfig);

// Update RCS configuration for a source
router.put('/config/:source', rcsController.updateRCSConfig);

// ============ TESTING ENDPOINTS ============
// Send test RCS message (for development/testing)
router.post('/test', rcsController.testRCS);

module.exports = router;
const express = require('express');
const router = express.Router();
const {
  streamLeadsToLender,
  startBackgroundDistribution,
  getBatchStats,
  getAllBatches,
  getLenderStats,
  getLeadsPreview
} = require('../controllers/leadDistributionController');

// Start distribution in background (Fire and forget - continues even if frontend closes)
router.post('/start', startBackgroundDistribution);

// Stream leads to lender (with real-time SSE progress - job continues even if connection drops)
router.post('/stream', streamLeadsToLender);

// Get preview of leads that match filters
router.post('/preview', getLeadsPreview);

// Get specific batch statistics
router.get('/batch/:batchId', getBatchStats);

// Get all batches (paginated)
router.get('/batches', getAllBatches);

// Get lender-specific statistics
router.get('/lender/:lender/stats', getLenderStats);

module.exports = router;
const express = require('express');
const router = express.Router();
const {
  streamLeadsToLender,
  getBatchStats,
  getAllBatches,
  getLenderStats,
  getLeadsPreview
} = require('../controllers/leadDistributionController');

// Stream leads to lender (with real-time progress)
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
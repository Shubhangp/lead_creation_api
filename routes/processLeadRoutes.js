const express = require('express');
const {
  uploadProcessLeads,
  uploadProcessLeadsChunk,
  completeProcessLeadsUpload,
  pushProcessLeads,
  getPushJobStatus,
  getAvailableLenders,
  getLeadCount,
  downloadTemplate,
} = require('../controllers/processLeadController');

const router = express.Router();

// Upload xlsx → save to process_leads table
// uploadProcessLeads already contains the multer middleware array
router.post('/upload', uploadProcessLeads);

// Chunked upload (for large files that exceed the platform's request size limit)
// Client sends raw binary slices to /upload/chunk, then calls /upload/complete.
router.post('/upload/chunk', uploadProcessLeadsChunk);
router.post('/upload/complete', completeProcessLeadsUpload);

// Trigger push: process_leads → leads table → lenders
router.post('/push', pushProcessLeads);

// Poll push job status
router.get('/push-jobs/:jobId', getPushJobStatus);

// Preview count before pushing
router.get('/count', getLeadCount);

// List valid lender keys
router.get('/lenders', getAvailableLenders);

// Download sample template
router.get('/template', downloadTemplate);

module.exports = router;
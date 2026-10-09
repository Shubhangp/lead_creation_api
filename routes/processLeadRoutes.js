const express = require('express');
const {
  uploadProcessLeads,
  uploadProcessLeadsChunk,
  completeProcessLeadsUpload,
  pushProcessLeads,
  getPushJobStatus,
  cancelPushJob,
  stopPushJobLender,
  listActivePushJobs,
  getAvailableLenders,
  getLeadCount,
  dedupCheckLeads,
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

// List running push jobs (so a Stop button is available after a page reload)
router.get('/push-jobs', listActivePushJobs);

// Poll push job status
router.get('/push-jobs/:jobId', getPushJobStatus);

// Stop a running push — nothing further is sent to lenders
router.post('/push-jobs/:jobId/cancel', cancelPushJob);

// Stop ONE lender of a running push — the other lenders keep going
router.post('/push-jobs/:jobId/lenders/:lender/stop', stopPushJobLender);

// Preview count before pushing
router.get('/count', getLeadCount);

// Read-only duplicate pre-check against the leads table (phone OR PAN, 90-day
// lookback by default). Used by the combined restructure+upload page.
router.post('/dedup-check', dedupCheckLeads);

// List valid lender keys
router.get('/lenders', getAvailableLenders);

// Download sample template
router.get('/template', downloadTemplate);

module.exports = router;
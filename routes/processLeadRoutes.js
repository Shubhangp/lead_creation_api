const express = require('express');
const {
  uploadProcessLeads,
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
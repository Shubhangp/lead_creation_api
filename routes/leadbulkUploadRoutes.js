const express = require('express');
const router = express.Router();
const {
  upload,
  bulkUpload,
  downloadTemplate,
  importLegacyRows,
} = require('../controllers/leadbulkUploadController');

function handleMulterError(err, req, res, next) {
  if (err && err.message) {
    return res.status(400).json({ success: false, message: err.message });
  }
  next(err);
}

router.get('/template', downloadTemplate);

// Old-data import from the dashboard: JSON batches, keeps createdAt from the
// file, skips rows whose phone or PAN already exists. No lender dispatch.
router.post('/legacy', importLegacyRows);

router.post(
  '/',
  upload.single('file'),
  bulkUpload,
  handleMulterError
);

module.exports = router;
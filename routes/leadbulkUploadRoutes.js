const express = require('express');
const router = express.Router();
const {
  upload,
  bulkUpload,
  downloadTemplate,
} = require('../controllers/leadbulkUploadController');

function handleMulterError(err, req, res, next) {
  if (err && err.message) {
    return res.status(400).json({ success: false, message: err.message });
  }
  next(err);
}

router.get('/template', downloadTemplate);

router.post(
  '/',
  upload.single('file'),
  bulkUpload,
  handleMulterError
);

module.exports = router;
// routes/lenderSyncRoutes.js
const express = require('express');
const router  = express.Router();
const multer  = require('multer');
const lenderSyncController = require('../controllers/lenderStatusUploadController');

const upload = multer({
  dest: '/tmp/uploads/',
  limits: { 
    fileSize: 20 * 1024 * 1024
  },
  fileFilter: (req, file, cb) => {
    const allowedExts = ['.csv', '.xlsx', '.xls'];
    const ext = file.originalname.toLowerCase().substring(file.originalname.lastIndexOf('.'));
    
    if (allowedExts.includes(ext)) {
      cb(null, true);
    } else {
      cb(new Error(`File type ${ext} not supported. Allowed: ${allowedExts.join(', ')}`));
    }
  }
});

router.get('/lenders', lenderSyncController.getLenders);
router.post('/upload', upload.single('file'), lenderSyncController.uploadAndSync);
router.get('/stats/:lender', lenderSyncController.getStats);

module.exports = router;
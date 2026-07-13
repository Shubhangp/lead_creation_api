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

// Chunk uploader: no extension filter (chunks are raw binary slices), small size cap
const chunkUpload = multer({
  dest: '/tmp/uploads/',
  limits: { fileSize: 4 * 1024 * 1024 }, // each chunk must stay under proxy body limit
});

router.get('/lenders', lenderSyncController.getLenders);
router.post('/upload', upload.single('file'), lenderSyncController.uploadAndSync);
router.post('/upload-chunk', chunkUpload.single('chunk'), lenderSyncController.uploadChunk);
router.post('/upload-finalize', express.json(), lenderSyncController.finalizeChunkedUpload);

module.exports = router;
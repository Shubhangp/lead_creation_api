'use strict';

// routes/whatsappCampaignRoutes.js
const express = require('express');
const router  = express.Router();
const multer  = require('multer');

const { authenticate } = require('../middlewares/auth');
const whatsappCampaignController = require('../controllers/whatsappCampaignController');

// Memory storage — the buffer goes straight to S3, never touches disk.
const upload = multer({
  storage: multer.memoryStorage(),
  limits: { fileSize: 10 * 1024 * 1024 }, // 10MB
  fileFilter: (req, file, cb) => {
    const allowedExts = ['.csv', '.xls', '.xlsx'];
    const ext = file.originalname.toLowerCase().substring(file.originalname.lastIndexOf('.'));
    if (allowedExts.includes(ext)) {
      cb(null, true);
    } else {
      cb(new Error(`File type ${ext} not supported. Allowed: ${allowedExts.join(', ')}`));
    }
  },
});

router.use(authenticate);

router.get('/partners', whatsappCampaignController.getPartners);
router.post('/upload', upload.single('file'), whatsappCampaignController.uploadCampaign);
router.get('/', whatsappCampaignController.listCampaigns);
router.patch('/:campaignId', whatsappCampaignController.updateCampaign);
router.get('/:campaignId/download-url', whatsappCampaignController.getDownloadUrl);

module.exports = router;

const express = require('express');
const { createLead, getLeads, processFile, getLeadById, createUATLead } = require('../controllers/leadController');
const apiKeyAuth = require('../middlewares/apiKeyAuth');
const apiKeyUATAuth = require('../middlewares/apiKeyUATAuth');
const upload = require('../middlewares/uploadMiddleware');
const router = express.Router();

router.route('/').post(apiKeyAuth, createLead);

router.route('/UAT').post(apiKeyUATAuth, createUATLead);

router.post('/upload', upload.single('file'), processFile);

router.route('/rate_cut/get/request/for/all/data').get(getLeads);

router.route('/rate_cut/get/by/id/request/:id').get(getLeadById);

module.exports = router;
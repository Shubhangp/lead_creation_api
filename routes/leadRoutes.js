const express = require('express');
const router = express.Router();
const { createLead, getLeads, getLeadById, createUATLead } = require('../controllers/leadController');
const apiKeyAuth = require('../middlewares/apiKeyAuth');
const apiKeyUATAuth = require('../middlewares/apiKeyUATAuth');

router.route('/').post(apiKeyAuth, createLead);

router.route('/UAT').post(apiKeyUATAuth, createUATLead);

router.route('/rate_cut/get/request/for/all/data').get(getLeads);

router.route('/rate_cut/get/by/id/request/:id').get(getLeadById);

module.exports = router;
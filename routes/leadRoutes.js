const express = require('express');
const router = express.Router();
const { createLead, getLeads, getLeadById } = require('../controllers/leadController');
const apiKeyAuth = require('../middlewares/apiKeyAuth');

router.route('/').post(apiKeyAuth, createLead);

router.route('rate_cut/get/request/for/all/data').get(getLeads);

router.route('rate_cut/get/by/id/request/:id').get(getLeadById);

module.exports = router;
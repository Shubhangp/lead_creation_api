// routes/leadSuccessRoutes.js
const express = require('express');
const router = express.Router();
const leadSuccessController = require('../controllers/leadSuccessController');

router.get('/:leadId', leadSuccessController.getLeadSuccessByLeadId);

router.patch('/:leadId/lender/:lenderName', leadSuccessController.updateLenderStatus);

module.exports = router;
const express = require('express');
const router = express.Router();
const { createApiKey, createApiKeyUAT } = require('../controllers/apiKeyController');

router.route('/').post(createApiKey);
router.route('/UAT').post(createApiKeyUAT);

module.exports = router;
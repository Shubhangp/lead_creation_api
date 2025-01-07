const express = require('express');
const router = express.Router();
const { createApiKey } = require('../controllers/apiKeyController');

router.route('/').post(createApiKey);

module.exports = router;
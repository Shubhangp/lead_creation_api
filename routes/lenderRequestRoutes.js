const express = require('express');
const { createLenderRequest } = require('../controllers/lenderRequestController.js');
const router = express.Router();

router.route('/').post(createLenderRequest);

module.exports = router;
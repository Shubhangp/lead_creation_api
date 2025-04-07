const express = require('express');
const { createLead } = require('../controllers/formController');
const router = express.Router();

router.route('/').post(createLead);

module.exports = router;
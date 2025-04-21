const express = require('express');
const router = express.Router();
const distributionRuleController = require('../controllers/distributionRuleController');
// const authMiddleware = require('../middleware/authMiddleware');

// router.use(authMiddleware.protect);

// router.use(authMiddleware.restrictTo('admin'));

router.route('/')
  .get(distributionRuleController.getAllDistributionRules)
  .post(distributionRuleController.createDistributionRule);

router.route('/:source')
  .get(distributionRuleController.getDistributionRuleBySource)
  .patch(distributionRuleController.updateDistributionRule)
  .delete(distributionRuleController.deleteDistributionRule);

module.exports = router;
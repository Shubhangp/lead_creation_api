const express = require('express');
const router = express.Router();
const distributionRuleController = require('../controllers/distributionRuleController');
// const authMiddleware = require('../middleware/authMiddleware');

// router.use(authMiddleware.protect);

// router.use(authMiddleware.restrictTo('admin'));

router.route('/')
  .get(distributionRuleController.getAllDistributionRules)
  .post(distributionRuleController.createDistributionRule);

// Landing-page (web) config — additive to the S2S distribution system.
// Declared before '/:source' so it never gets shadowed by the generic route.
router.route('/web-config/:source')
  .get(distributionRuleController.getWebConfigBySource)
  .put(distributionRuleController.upsertWebConfigBySource)
  .patch(distributionRuleController.upsertWebConfigBySource);

router.route('/:source')
  .get(distributionRuleController.getDistributionRuleBySource)
  .patch(distributionRuleController.updateDistributionRule)
  .delete(distributionRuleController.deleteDistributionRule);

module.exports = router;
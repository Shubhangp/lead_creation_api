const rcsService = require('../services/rcsService');
const { RCSQueue } = require('../models/rcsModels');
const DistributionRule = require('../models/distributionRuleModel');
const {
  saveEvent,
  listEventsByDate,
  listEventsByPhone,
  getEvent,
} = require("../models/rcsWebhookModel");

exports.rcsWebhook = async (req, res) => {
  try {
    const body = req.body;

    console.log("📩 Incoming RCS Webhook:");
    console.log(JSON.stringify(body, null, 2));

    if (!body || Object.keys(body).length === 0) {
      return res.status(400).json({ error: "Empty payload" });
    }

    const savedEvent = await saveEvent(body);

    console.log(`✅ Saved: ${savedEvent.eventId} | type: ${savedEvent.eventType}`);

    return res.status(200).json({
      success: true,
      eventId: savedEvent.eventId,
      messageId: savedEvent.messageId,
      eventType: savedEvent.eventType,
    });
  } catch (err) {
    console.error("❌ Webhook error:", err);
    return res.status(500).json({ error: "Internal Server Error" });
  }
};

/**
 * Manual trigger to process pending RCS messages
 * GET /api/rcs/process-pending
 */
exports.processPendingRCS = async (req, res) => {
  try {
    console.log('Manual RCS processing triggered');
    await rcsService.processPendingRCS();

    res.json({
      status: 'success',
      message: 'Pending RCS messages processed successfully',
      timestamp: new Date()
    });
  } catch (error) {
    console.error('Error in manual RCS processing:', error);
    res.status(500).json({
      status: 'error',
      message: error.message,
      timestamp: new Date()
    });
  }
};

/**
 * Get RCS queue status and statistics
 * GET /api/rcs/queue/status
 */
exports.getRCSQueueStatus = async (req, res) => {
  try {
    // Get status breakdown
    const stats = await RCSQueue.getAggregateStats();

    // Get pending messages (ready to send)
    const pendingMessages = await RCSQueue.findByStatusAndScheduledTime(
      'PENDING',
      new Date()
    );

    // Get upcoming messages (scheduled for future)
    const allPending = await RCSQueue.countByStatus('PENDING');
    const upcomingMessages = allPending - pendingMessages.length;

    // Get recent queue entries for preview
    const recentMessages = await RCSQueue.findRecent(10);

    res.json({
      status: 'success',
      data: {
        stats,
        pendingMessages: pendingMessages.length,
        upcomingMessages,
        recentMessages,
        isWithinOperatingHours: rcsService.isWithinOperatingHours(),
        currentTime: new Date()
      }
    });
  } catch (error) {
    console.error('Error getting RCS queue status:', error);
    res.status(500).json({
      status: 'error',
      message: error.message
    });
  }
};

/**
 * Get RCS logs for a specific lead
 * GET /api/rcs/logs/lead/:leadId
 */
exports.getRCSLogsForLead = async (req, res) => {
  try {
    const { leadId } = req.params;

    if (!leadId) {
      return res.status(400).json({
        status: 'error',
        message: 'Lead ID is required'
      });
    }

    // Get queue entries
    const queueEntries = await RCSQueue.findByLeadId(leadId);

    res.json({
      status: 'success',
      data: {
        queueEntries,
        totalQueueEntries: queueEntries.length
      }
    });
  } catch (error) {
    console.error('Error getting RCS logs for lead:', error);
    res.status(500).json({
      status: 'error',
      message: error.message
    });
  }
};

/**
 * Update RCS configuration for a source
 * PUT /api/rcs/config/:source
 */
exports.updateRCSConfig = async (req, res) => {
  try {
    const { source } = req.params;
    const { rcsConfig } = req.body;

    if (!source) {
      return res.status(400).json({
        status: 'error',
        message: 'Source is required'
      });
    }

    if (!rcsConfig) {
      return res.status(400).json({
        status: 'error',
        message: 'RCS configuration is required'
      });
    }

    // Validate RCS config structure
    if (rcsConfig.lenderPriority) {
      // Ensure no duplicate priorities
      const priorities = rcsConfig.lenderPriority.map(lp => lp.priority);
      const uniquePriorities = [...new Set(priorities)];
      if (priorities.length !== uniquePriorities.length) {
        return res.status(400).json({
          status: 'error',
          message: 'Duplicate priorities found in lender priority configuration'
        });
      }
    }

    // Update or create distribution rule
    const existingRule = await DistributionRule.findBySource(source);

    let distributionRule;
    if (existingRule) {
      distributionRule = await DistributionRule.updateBySource(source, {
        rcsConfig,
        lastUpdatedBy: req.user?.id || 'api'
      });
    } else {
      distributionRule = await DistributionRule.create({
        source,
        rcsConfig,
        rules: { immediate: [], delayed: [] },
        active: true,
        lastUpdatedBy: req.user?.id || 'api'
      });
    }

    res.json({
      status: 'success',
      message: `RCS configuration updated for source: ${source}`,
      data: distributionRule
    });
  } catch (error) {
    console.error('Error updating RCS config:', error);
    res.status(500).json({
      status: 'error',
      message: error.message
    });
  }
};

/**
 * Get RCS configuration for a source
 * GET /api/rcs/config/:source
 */
exports.getRCSConfig = async (req, res) => {
  try {
    const { source } = req.params;

    const distributionRule = await DistributionRule.findActiveBySource(source);

    if (!distributionRule) {
      return res.status(404).json({
        status: 'error',
        message: `No distribution rule found for source: ${source}`
      });
    }

    res.json({
      status: 'success',
      data: {
        source: distributionRule.source,
        rcsConfig: distributionRule.rcsConfig || {},
        lastUpdated: distributionRule.lastUpdated
      }
    });
  } catch (error) {
    console.error('Error getting RCS config:', error);
    res.status(500).json({
      status: 'error',
      message: error.message
    });
  }
};

/**
 * Cancel pending RCS messages for a lead
 * DELETE /api/rcs/queue/lead/:leadId
 */
exports.cancelRCSForLead = async (req, res) => {
  try {
    const { leadId } = req.params;

    if (!leadId) {
      return res.status(400).json({
        status: 'error',
        message: 'Lead ID is required'
      });
    }

    const result = await RCSQueue.updateManyByLeadIdAndStatus(
      leadId,
      'PENDING',
      {
        status: 'CANCELLED',
        failureReason: 'Cancelled by user'
      }
    );

    res.json({
      status: 'success',
      message: `Cancelled ${result.modifiedCount} pending RCS messages for lead ${leadId}`,
      data: {
        leadId,
        cancelledCount: result.modifiedCount
      }
    });
  } catch (error) {
    console.error('Error cancelling RCS for lead:', error);
    res.status(500).json({
      status: 'error',
      message: error.message
    });
  }
};

/**
 * Test RCS message (for development/testing)
 * POST /api/rcs/test
 */
exports.testRCS = async (req, res) => {
  try {
    const { phone, rcsType, lenderName } = req.body;

    // Validation
    if (!phone || !rcsType) {
      return res.status(400).json({
        status: 'error',
        message: 'Phone and rcsType are required'
      });
    }

    if (!['LENDER_SUCCESS', 'ZET_CAMPAIGN'].includes(rcsType)) {
      return res.status(400).json({
        status: 'error',
        message: 'Invalid rcsType. Must be LENDER_SUCCESS or ZET_CAMPAIGN'
      });
    }

    if (rcsType === 'LENDER_SUCCESS' && !lenderName) {
      return res.status(400).json({
        status: 'error',
        message: 'lenderName is required for LENDER_SUCCESS type'
      });
    }

    // Create test payload
    let payload;
    const mockLead = {
      _id: 'test_' + Date.now(),
      phone: phone.replace('+91', ''),
      fullName: 'Test User',
      source: 'TEST'
    };

    if (rcsType === 'LENDER_SUCCESS') {
      payload = rcsService.generateLenderSuccessPayload(phone, lenderName, mockLead);
    } else {
      payload = rcsService.generateZetCampaignPayload(phone, mockLead);
    }

    // Send test RCS
    const result = await rcsService.sendRCS(payload);

    res.json({
      status: 'success',
      message: 'Test RCS sent',
      data: {
        payload,
        result,
        testInfo: {
          phone,
          rcsType,
          lenderName,
          timestamp: new Date()
        }
      }
    });
  } catch (error) {
    console.error('Error sending test RCS:', error);
    res.status(500).json({
      status: 'error',
      message: error.message
    });
  }
};

/**
 * Reschedule failed RCS messages
 * POST /api/rcs/reschedule-failed
 */
exports.rescheduleFailedRCS = async (req, res) => {
  try {
    const { maxAttempts = 3 } = req.body;

    const result = await RCSQueue.updateManyByStatusAndAttempts(
      'FAILED',
      maxAttempts,
      {
        status: 'PENDING',
        scheduledTime: new Date().toISOString(),
        failureReason: null
      }
    );

    res.json({
      status: 'success',
      message: `Rescheduled ${result.modifiedCount} failed RCS messages`,
      data: {
        rescheduledCount: result.modifiedCount
      }
    });
  } catch (error) {
    console.error('Error rescheduling failed RCS:', error);
    res.status(500).json({
      status: 'error',
      message: error.message
    });
  }
};
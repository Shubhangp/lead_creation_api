const rcsService = require('../services/rcsService');
const { RCSQueue } = require('../models/rcsModels');
const DistributionRule = require('../models/distributionRuleModel');

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
    const stats = await RCSQueue.aggregate([
      {
        $group: {
          _id: '$status',
          count: { $sum: 1 }
        }
      }
    ]);

    // Get pending messages (ready to send)
    const pendingMessages = await RCSQueue.find({ 
      status: 'PENDING',
      scheduledTime: { $lte: new Date() }
    }).countDocuments();

    // Get upcoming messages (scheduled for future)
    const upcomingMessages = await RCSQueue.find({ 
      status: 'PENDING',
      scheduledTime: { $gt: new Date() }
    }).countDocuments();

    // Get recent queue entries for preview
    const recentMessages = await RCSQueue.find({}, 'leadId rcsType status createdAt')
      .populate('leadId', 'fullName phone source')
      .sort({ createdAt: -1 })
      .limit(10)
      .lean();

    res.json({
      status: 'success',
      data: {
        stats,
        pendingMessages,
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

    // Get RCS logs
    // const logs = await RCSLog.find({ leadId }, 'queueId rcsType responseStatus sentAt success createdAt')
    //   .populate('queueId', 'status scheduledTime createdAt')
    //   .sort({ createdAt: -1 })
    //   .lean();

    // Get queue entries
    const queueEntries = await RCSQueue.find({ leadId }, 'leadId rcsType status scheduledTime attempts createdAt')
      .populate('leadId', 'fullName phone source')
      .sort({ createdAt: -1 })
      .lean();

    res.json({
      status: 'success',
      data: {
        // logs,
        queueEntries,
        // totalLogs: logs.length,
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
 * Get RCS analytics and reporting
 * GET /api/rcs/analytics?startDate=2024-01-01&endDate=2024-01-31
 */
// exports.getRCSAnalytics = async (req, res) => {
//   try {
//     const { startDate, endDate, rcsType, lenderName } = req.query;
    
//     // Build match filter
//     const matchFilter = {};
    
//     if (startDate && endDate) {
//       matchFilter.sentAt = {
//         $gte: new Date(startDate),
//         $lte: new Date(endDate)
//       };
//     }
    
//     if (rcsType) {
//       matchFilter.rcsType = rcsType;
//     }
    
//     if (lenderName) {
//       matchFilter.lenderName = lenderName;
//     }

//     // Get detailed analytics
//     const analytics = await RCSLog.aggregate([
//       { $match: matchFilter },
//       {
//         $group: {
//           _id: {
//             rcsType: '$rcsType',
//             success: '$success',
//             lenderName: '$lenderName',
//             date: { $dateToString: { format: '%Y-%m-%d', date: '$sentAt' } }
//           },
//           count: { $sum: 1 }
//         }
//       },
//       {
//         $group: {
//           _id: {
//             rcsType: '$_id.rcsType',
//             date: '$_id.date'
//           },
//           totalMessages: { $sum: '$count' },
//           successfulMessages: {
//             $sum: {
//               $cond: [{ $eq: ['$_id.success', true] }, '$count', 0]
//             }
//           },
//           failedMessages: {
//             $sum: {
//               $cond: [{ $eq: ['$_id.success', false] }, '$count', 0]
//             }
//           }
//         }
//       },
//       { $sort: { '_id.date': -1 } }
//     ]);

//     // Get summary statistics
//     const summary = await RCSLog.aggregate([
//       { $match: matchFilter },
//       {
//         $group: {
//           _id: null,
//           totalSent: { $sum: 1 },
//           totalSuccess: { $sum: { $cond: ['$success', 1, 0] } },
//           totalFailed: { $sum: { $cond: ['$success', 0, 1] } },
//           avgResponseTime: { $avg: '$responseTime' }
//         }
//       }
//     ]);

//     // Get lender-wise breakdown
//     const lenderBreakdown = await RCSLog.aggregate([
//       { $match: matchFilter },
//       {
//         $group: {
//           _id: '$lenderName',
//           totalSent: { $sum: 1 },
//           successful: { $sum: { $cond: ['$success', 1, 0] } },
//           failed: { $sum: { $cond: ['$success', 0, 1] } }
//         }
//       },
//       { $sort: { totalSent: -1 } }
//     ]);

//     res.json({
//       status: 'success',
//       data: {
//         analytics,
//         summary: summary[0] || { totalSent: 0, totalSuccess: 0, totalFailed: 0 },
//         lenderBreakdown,
//         filters: { startDate, endDate, rcsType, lenderName }
//       }
//     });
//   } catch (error) {
//     console.error('Error getting RCS analytics:', error);
//     res.status(500).json({
//       status: 'error',
//       message: error.message
//     });
//   }
// };

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

    const distributionRule = await DistributionRule.findOneAndUpdate(
      { source },
      { 
        $set: { 
          rcsConfig,
          lastUpdated: new Date(),
          lastUpdatedBy: req.user?.id || 'api'
        }
      },
      { new: true, upsert: true }
    );

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

    const distributionRule = await DistributionRule.findOne({ source, active: true });

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

    const result = await RCSQueue.updateMany(
      { leadId, status: 'PENDING' },
      { 
        $set: { 
          status: 'CANCELLED',
          failureReason: 'Cancelled by user'
        }
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

    const result = await RCSQueue.updateMany(
      { 
        status: 'FAILED',
        attempts: { $lt: maxAttempts }
      },
      { 
        $set: { 
          status: 'PENDING',
          scheduledTime: new Date(),
          failureReason: null
        }
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
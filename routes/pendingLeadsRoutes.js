const express = require('express');
const router = express.Router();
const PendingLead = require('../models/pendingLeadModel');
const continuousScheduler = require('../scheduler/continuousScheduler');

/**
 * GET /api/pending-leads/stats
 * Get comprehensive statistics
 */
router.get('/stats', async (req, res) => {
  try {
    const stats = await continuousScheduler.getStats();

    res.json({
      status: 'success',
      data: stats
    });
  } catch (error) {
    console.error('Error fetching stats:', error);
    res.status(500).json({
      status: 'error',
      message: error.message
    });
  }
});

/**
 * GET /api/pending-leads
 * Get all pending lead entries
 */
router.get('/', async (req, res) => {
  try {
    const { limit = 100, lastKey } = req.query;

    const result = await PendingLead.getAllPending(
      parseInt(limit),
      lastKey ? JSON.parse(lastKey) : null
    );

    res.json({
      status: 'success',
      count: result.count,
      data: result.items,
      lastEvaluatedKey: result.lastEvaluatedKey,
      hasMore: !!result.lastEvaluatedKey
    });
  } catch (error) {
    console.error('Error fetching pending leads:', error);
    res.status(500).json({
      status: 'error',
      message: error.message
    });
  }
});

/**
 * GET /api/pending-leads/count
 * Get quick count of pending entries
 */
router.get('/count', async (req, res) => {
  try {
    const count = await PendingLead.getPendingCount();

    res.json({
      status: 'success',
      data: {
        pendingEntries: count
      }
    });
  } catch (error) {
    console.error('Error counting pending leads:', error);
    res.status(500).json({
      status: 'error',
      message: error.message
    });
  }
});

/**
 * GET /api/pending-leads/lead/:leadId
 * Get pending entry for a specific lead
 */
router.get('/lead/:leadId', async (req, res) => {
  try {
    const { leadId } = req.params;
    const pendingLead = await PendingLead.findByLeadId(leadId);

    if (!pendingLead) {
      return res.status(404).json({
        status: 'error',
        message: 'No pending entry found for this lead'
      });
    }

    res.json({
      status: 'success',
      data: pendingLead
    });
  } catch (error) {
    console.error('Error fetching pending lead:', error);
    res.status(500).json({
      status: 'error',
      message: error.message
    });
  }
});

/**
 * POST /api/pending-leads/scheduler/start
 * Start continuous processing
 */
router.post('/scheduler/start', async (req, res) => {
  try {
    await continuousScheduler.start();

    res.json({
      status: 'success',
      message: 'Continuous scheduler started',
      config: {
        batchSize: continuousScheduler.batchSize,
        maxConcurrent: continuousScheduler.maxConcurrentProcessing
      }
    });
  } catch (error) {
    console.error('Error starting scheduler:', error);
    res.status(500).json({
      status: 'error',
      message: error.message
    });
  }
});

/**
 * POST /api/pending-leads/scheduler/stop
 * Stop continuous processing
 */
router.post('/scheduler/stop', async (req, res) => {
  try {
    continuousScheduler.stop();

    res.json({
      status: 'success',
      message: 'Continuous scheduler stopped'
    });
  } catch (error) {
    console.error('Error stopping scheduler:', error);
    res.status(500).json({
      status: 'error',
      message: error.message
    });
  }
});

/**
 * PUT /api/pending-leads/scheduler/config
 * Update scheduler configuration on-the-fly
 */
router.put('/scheduler/config', async (req, res) => {
  try {
    const { batchSize, maxConcurrent, delayBetweenBatches, delayBetweenRequests } = req.body;

    continuousScheduler.updateConfig({
      batchSize,
      maxConcurrent,
      delayBetweenBatches,
      delayBetweenRequests
    });

    res.json({
      status: 'success',
      message: 'Configuration updated',
      config: {
        batchSize: continuousScheduler.batchSize,
        maxConcurrent: continuousScheduler.maxConcurrentProcessing,
        delayBetweenBatches: continuousScheduler.delayBetweenBatches,
        delayBetweenRequests: continuousScheduler.delayBetweenRequests
      }
    });
  } catch (error) {
    console.error('Error updating config:', error);
    res.status(500).json({
      status: 'error',
      message: error.message
    });
  }
});

/**
 * POST /api/pending-leads/process/lead/:leadId
 * Manually process a specific lead
 */
router.post('/process/lead/:leadId', async (req, res) => {
  try {
    const { leadId } = req.params;
    const result = await continuousScheduler.processSpecificLead(leadId);

    res.json({
      status: 'success',
      message: 'Lead processed',
      data: result
    });
  } catch (error) {
    console.error('Error processing lead:', error);
    res.status(500).json({
      status: 'error',
      message: error.message
    });
  }
});

/**
 * POST /api/pending-leads/process/lender/:lenderName
 * Force process all pending leads for a specific lender
 */
router.post('/process/lender/:lenderName', async (req, res) => {
  try {
    const { lenderName } = req.params;
    
    // Start processing asynchronously
    continuousScheduler.processLenderNow(lenderName)
      .then(result => {
        console.log(`Completed processing for ${lenderName}:`, result);
      })
      .catch(error => {
        console.error(`Error processing ${lenderName}:`, error);
      });

    res.json({
      status: 'success',
      message: `Processing started for ${lenderName}`,
      note: 'Processing is running in the background. Check logs for completion.'
    });
  } catch (error) {
    console.error('Error starting lender processing:', error);
    res.status(500).json({
      status: 'error',
      message: error.message
    });
  }
});

/**
 * DELETE /api/pending-leads/lead/:leadId
 * Cancel pending lead (delete entry)
 */
router.delete('/lead/:leadId', async (req, res) => {
  try {
    const { leadId } = req.params;
    const result = await PendingLead.deleteByLeadId(leadId);

    if (!result.deleted) {
      return res.status(404).json({
        status: 'error',
        message: 'No pending entry found for this lead'
      });
    }

    res.json({
      status: 'success',
      message: 'Pending lead entry deleted'
    });
  } catch (error) {
    console.error('Error deleting pending lead:', error);
    res.status(500).json({
      status: 'error',
      message: error.message
    });
  }
});

/**
 * PUT /api/pending-leads/lead/:leadId/lenders
 * Update lender list for a pending lead
 */
router.put('/lead/:leadId/lenders', async (req, res) => {
  try {
    const { leadId } = req.params;
    const { lendersToAdd, lendersToRemove } = req.body;

    const pendingLead = await PendingLead.findByLeadId(leadId);
    
    if (!pendingLead) {
      return res.status(404).json({
        status: 'error',
        message: 'No pending entry found for this lead'
      });
    }

    let lenderNames = [...pendingLead.lenderNames];

    // Add lenders
    if (lendersToAdd && Array.isArray(lendersToAdd)) {
      lenderNames = [...new Set([...lenderNames, ...lendersToAdd])];
    }

    // Remove lenders
    if (lendersToRemove && Array.isArray(lendersToRemove)) {
      lenderNames = lenderNames.filter(l => !lendersToRemove.includes(l));
    }

    // If no lenders left, delete entry
    if (lenderNames.length === 0) {
      await PendingLead.deleteByLeadId(leadId);
      return res.json({
        status: 'success',
        message: 'No lenders remaining, entry deleted'
      });
    }

    // Update entry
    const updated = await PendingLead.updateById(pendingLead.pendingLeadId, {
      lenderNames
    });

    res.json({
      status: 'success',
      data: updated
    });
  } catch (error) {
    console.error('Error updating lenders:', error);
    res.status(500).json({
      status: 'error',
      message: error.message
    });
  }
});

/**
 * POST /api/pending-leads/cleanup
 * Cleanup old completed entries (maintenance)
 */
router.post('/cleanup', async (req, res) => {
  try {
    const { daysOld = 7 } = req.body;
    const deletedCount = await PendingLead.cleanupOldEntries(daysOld);

    res.json({
      status: 'success',
      message: `Cleaned up ${deletedCount} old entries`,
      deletedCount
    });
  } catch (error) {
    console.error('Error cleaning up:', error);
    res.status(500).json({
      status: 'error',
      message: error.message
    });
  }
});

/**
 * GET /api/pending-leads/health
 * Health check endpoint
 */
router.get('/health', async (req, res) => {
  try {
    const stats = await continuousScheduler.getStats();
    
    res.json({
      status: 'healthy',
      scheduler: {
        running: stats.isRunning,
        processing: stats.isProcessing,
        uptime: stats.uptime
      },
      pending: {
        entries: stats.pending.totalEntries,
        lenders: stats.pending.totalPendingLenders
      },
      timestamp: new Date().toISOString()
    });
  } catch (error) {
    res.status(500).json({
      status: 'unhealthy',
      error: error.message
    });
  }
});

module.exports = router;
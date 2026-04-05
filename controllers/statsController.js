const path = require('path');

// ─── Lender Configuration ────────────────────────────────────────────────────
//
// ADD NEW LENDERS HERE.
//
// statsType:
//   'demographic' — Lead model (age, salary, gender, consent breakdowns)
//   'status'      — Response log models that use source-createdAt-index
//   'message'     — FatakPay style models (message/offer/eligibility breakdowns)
//   'queue'       — Queue models (RCS, etc.) that use status-based indexes
//
// Controller method signatures expected by each model:
//   getQuickStats(null, startDate?, endDate?)   → { totalLogs, ... }
//   getStats(startDate?, endDate?)              → full stats object
//   getStatsByDate(startDate, endDate)          → array of daily objects

const LENDER_CONFIG = {
  leads: {
    modelPath: '../models/leadModel.js',
    statsType: 'demographic',
    displayName: 'Leads'
  },
  fatakpay: {
    modelPath: '../models/fatakPayResponseLog',
    statsType: 'message',
    displayName: 'FatakPay'
  },
  fatakpaypl: {
    modelPath: '../models/fatakPayPLResponseLog',
    statsType: 'message',
    displayName: 'FatakPayPL'
  },
  ovly: {
    modelPath: '../models/ovlyResponseLog',
    statsType: 'status',
    displayName: 'Ovly'
  },
  lendingplate: {
    modelPath: '../models/leadingPlateResponseLog.js',
    statsType: 'status',
    displayName: 'Lending Plate'
  },
  mpokket: {
    modelPath: '../models/mpokketResponseLog.js',
    statsType: 'status',
    displayName: 'Mpokket'
  },
  ramfincrop: {
    modelPath: '../models/ramFinCropLogModel.js',
    statsType: 'status',
    displayName: 'RamFinCorp'
  },
  indialends: {
    modelPath: '../models/indiaLendsResponseLog.js',
    statsType: 'status',
    displayName: 'India Lends'
  },
  zype: {
    modelPath: '../models/ZypeResponseLogModel.js',
    statsType: 'status',
    displayName: 'Zype'
  },
  creditsea: {
    modelPath: '../models/creditSeaResponseLog.js',
    statsType: 'status',
    displayName: 'Credit Sea'
  },
  // ── RCS Queue ──────────────────────────────────────────────────────────────
  rcs_queue: {
    modelPath: '../models/rcsModels.js',
    statsType: 'queue',
    displayName: 'RCS Queue'
  }
};

class UnifiedStatsController {

  // ─── model loader ─────────────────────────────────────────────────────────

  static getLenderModel(lenderName) {
    const lenderKey = lenderName.toLowerCase();
    const lenderConfig = LENDER_CONFIG[lenderKey];

    if (!lenderConfig) {
      throw new Error(
        `Unknown lender: ${lenderName}. Available lenders: ${Object.keys(LENDER_CONFIG).join(', ')}`
      );
    }

    try {
      const raw = require(lenderConfig.modelPath);
      // Support both module.exports = Class and module.exports = { ClassName }
      const Model = raw.default || (typeof raw === 'function' ? raw : Object.values(raw)[0]);
      return { model: Model, config: lenderConfig };
    } catch (error) {
      throw new Error(`Failed to load model for ${lenderName}: ${error.message}`);
    }
  }

  // ─── GET /lenders ─────────────────────────────────────────────────────────

  static async getAvailableLenders(req, res) {
    try {
      const lenders = Object.keys(LENDER_CONFIG).map(key => ({
        name: key,
        displayName: LENDER_CONFIG[key].displayName,
        statsType: LENDER_CONFIG[key].statsType
      }));

      return res.status(200).json({ success: true, data: lenders });
    } catch (error) {
      console.error('Error fetching available lenders:', error);
      return res.status(500).json({
        success: false,
        message: 'Failed to fetch available lenders',
        error: error.message
      });
    }
  }

  // ─── GET /unified-stats ───────────────────────────────────────────────────
  //
  // All models must implement: getStats(startDate?, endDate?)
  //
  // FatakPay/FatakPayPL: returns { unavailable: true, reason: '...' } when the
  //   source-createdAt-index is still CREATING; the frontend handles this gracefully.
  //
  // RCS Queue: returns aggregate cross-status stats for the date window.

  static async getStats(req, res) {
    try {
      const { lender, startDate, endDate } = req.query;

      if (!lender) {
        return res.status(400).json({
          success: false,
          message: 'Lender parameter is required',
          availableLenders: Object.keys(LENDER_CONFIG)
        });
      }

      const { model: Model, config } = UnifiedStatsController.getLenderModel(lender);

      let start = startDate;
      let end = endDate;
      if (!start || !end) {
        const now = new Date();
        end = now.toISOString();
        start = new Date(new Date(now).setDate(now.getDate() - 30)).toISOString();
      }

      // All models now accept (startDate, endDate) — no null leadId needed
      const stats = await Model.getStats(start, end);

      stats.lender = lender;
      stats.displayName = config.displayName;
      stats.statsType = config.statsType;

      return res.status(200).json({ success: true, data: stats });
    } catch (error) {
      console.error('Error fetching stats:', error);
      return res.status(500).json({
        success: false,
        message: 'Failed to fetch statistics',
        error: error.message
      });
    }
  }

  // ─── GET /unified-stats/by-date ───────────────────────────────────────────
  //
  // All models must implement: getStatsByDate(startDate, endDate)

  static async getStatsByDate(req, res) {
    try {
      const { lender, startDate, endDate } = req.query;

      if (!lender) {
        return res.status(400).json({
          success: false,
          message: 'Lender parameter is required',
          availableLenders: Object.keys(LENDER_CONFIG)
        });
      }

      if (!startDate || !endDate) {
        return res.status(400).json({
          success: false,
          message: 'startDate and endDate are required'
        });
      }

      const { model: Model } = UnifiedStatsController.getLenderModel(lender);

      const stats = await Model.getStatsByDate(startDate, endDate);

      return res.status(200).json({ success: true, data: stats });
    } catch (error) {
      console.error('Error fetching stats by date:', error);
      return res.status(500).json({
        success: false,
        message: 'Failed to fetch statistics by date',
        error: error.message
      });
    }
  }

  // ─── GET /unified-stats/quick ─────────────────────────────────────────────
  //
  // All models must implement: getQuickStats(null, startDate?, endDate?)
  // The first argument is a legacy "source" filter; passing null means all sources.

  static async getQuickStats(req, res) {
    try {
      const { lender, startDate, endDate } = req.query;

      if (!lender) {
        return res.status(400).json({
          success: false,
          message: 'Lender parameter is required',
          availableLenders: Object.keys(LENDER_CONFIG)
        });
      }

      const { model: Model, config } = UnifiedStatsController.getLenderModel(lender);

      // Unified signature: getQuickStats(null, startDate?, endDate?)
      // null = "all sources" (first param is source filter for some models)
      const quickStats = await Model.getQuickStats(null, startDate, endDate);

      if (!quickStats || typeof quickStats !== 'object') {
        throw new Error('getQuickStats returned invalid data');
      }

      quickStats.lender = lender;
      quickStats.displayName = config.displayName;

      return res.status(200).json({ success: true, data: quickStats });
    } catch (error) {
      console.error('Error fetching quick stats:', error);
      return res.status(500).json({
        success: false,
        message: 'Failed to fetch quick statistics',
        error: error.message,
        stack: process.env.NODE_ENV === 'development' ? error.stack : undefined
      });
    }
  }

  // ─── GET /unified-stats/logs ──────────────────────────────────────────────

  static async getLogsByDateRange(req, res) {
    try {
      const { lender, startDate, endDate, limit, lastEvaluatedKey } = req.query;

      if (!lender) {
        return res.status(400).json({
          success: false,
          message: 'Lender parameter is required',
          availableLenders: Object.keys(LENDER_CONFIG)
        });
      }

      if (!startDate || !endDate) {
        return res.status(400).json({
          success: false,
          message: 'startDate and endDate are required'
        });
      }

      const { model: Model } = UnifiedStatsController.getLenderModel(lender);

      const options = {
        limit: parseInt(limit) || 100,
        lastEvaluatedKey: lastEvaluatedKey ? JSON.parse(lastEvaluatedKey) : undefined
      };

      const result = await Model.findByDateRange(startDate, endDate, options);

      return res.status(200).json({ success: true, data: result });
    } catch (error) {
      console.error('Error fetching logs by date range:', error);
      return res.status(500).json({
        success: false,
        message: 'Failed to fetch logs by date range',
        error: error.message
      });
    }
  }

  // ─── GET /unified-stats/all ───────────────────────────────────────────────

  static async getAllLendersStats(req, res) {
    try {
      const { startDate, endDate } = req.query;

      let start = startDate;
      let end = endDate;
      if (!start || !end) {
        const now = new Date();
        end = now.toISOString();
        start = new Date(new Date(now).setDate(now.getDate() - 30)).toISOString();
      }

      const allStats = {};

      for (const [lenderName, lenderConfig] of Object.entries(LENDER_CONFIG)) {
        try {
          const { model: Model } = UnifiedStatsController.getLenderModel(lenderName);
          const stats = await Model.getStats(start, end);
          allStats[lenderName] = {
            ...stats,
            displayName: lenderConfig.displayName,
            statsType: lenderConfig.statsType
          };
        } catch (error) {
          console.error(`Error fetching stats for ${lenderName}:`, error);
          allStats[lenderName] = {
            error: error.message,
            displayName: lenderConfig.displayName
          };
        }
      }

      return res.status(200).json({ success: true, data: allStats });
    } catch (error) {
      console.error('Error fetching all lenders stats:', error);
      return res.status(500).json({
        success: false,
        message: 'Failed to fetch all lenders statistics',
        error: error.message
      });
    }
  }

  // ─── GET /unified-stats/comparison ───────────────────────────────────────

  static async getComparison(req, res) {
    try {
      const { lenders, startDate, endDate } = req.query;

      if (!lenders) {
        return res.status(400).json({
          success: false,
          message: 'Lenders parameter is required (comma-separated list)',
          availableLenders: Object.keys(LENDER_CONFIG)
        });
      }

      let start = startDate;
      let end = endDate;
      if (!start || !end) {
        const now = new Date();
        end = now.toISOString();
        start = new Date(new Date(now).setDate(now.getDate() - 30)).toISOString();
      }

      const lenderList = lenders.split(',').map(l => l.trim().toLowerCase());
      const comparison = {};

      for (const lenderName of lenderList) {
        try {
          const { model: Model, config } = UnifiedStatsController.getLenderModel(lenderName);
          const stats = await Model.getStats(start, end);
          comparison[lenderName] = {
            displayName: config.displayName,
            totalLogs: stats.totalLogs,
            successRate: stats.successRate,
            statsType: config.statsType,
            ...stats
          };
        } catch (error) {
          console.error(`Error fetching stats for ${lenderName}:`, error);
          comparison[lenderName] = { error: error.message };
        }
      }

      return res.status(200).json({ success: true, data: comparison });
    } catch (error) {
      console.error('Error fetching comparison:', error);
      return res.status(500).json({
        success: false,
        message: 'Failed to fetch comparison statistics',
        error: error.message
      });
    }
  }
}

module.exports = UnifiedStatsController;
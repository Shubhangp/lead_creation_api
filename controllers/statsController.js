const path = require('path');

// Lender configuration mapping - ADD NEW LENDERS HERE
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
    modelPath: '../models/FatakPayResponseLogPL',
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
  }
};

class UnifiedStatsController {
  // Dynamically load lender model
  static getLenderModel(lenderName) {
    const lenderKey = lenderName.toLowerCase();
    const lenderConfig = LENDER_CONFIG[lenderKey];

    if (!lenderConfig) {
      throw new Error(`Unknown lender: ${lenderName}. Available lenders: ${Object.keys(LENDER_CONFIG).join(', ')}`);
    }

    try {
      // Dynamically require the model
      const Model = require(lenderConfig.modelPath);
      return {
        model: Model,
        config: lenderConfig
      };
    } catch (error) {
      throw new Error(`Failed to load model for ${lenderName}: ${error.message}`);
    }
  }

  // Get available lenders
  static async getAvailableLenders(req, res) {
    try {
      const lenders = Object.keys(LENDER_CONFIG).map(key => ({
        name: key,
        displayName: LENDER_CONFIG[key].displayName,
        statsType: LENDER_CONFIG[key].statsType
      }));

      return res.status(200).json({
        success: true,
        data: lenders
      });
    } catch (error) {
      console.error('Error fetching available lenders:', error);
      return res.status(500).json({
        success: false,
        message: 'Failed to fetch available lenders',
        error: error.message
      });
    }
  }

  // Get comprehensive stats
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

      // Dynamically load the lender model
      const { model: Model, config } = UnifiedStatsController.getLenderModel(lender);

      // Default to last 30 days if no dates provided
      let start = startDate;
      let end = endDate;

      if (!start || !end) {
        const now = new Date();
        end = now.toISOString();
        const thirtyDaysAgo = new Date(now.setDate(now.getDate() - 30));
        start = thirtyDaysAgo.toISOString();
      }

      const stats = await Model.getStats(start, end);

      // Add lender metadata
      stats.lender = lender;
      stats.displayName = config.displayName;
      stats.statsType = config.statsType;

      return res.status(200).json({
        success: true,
        data: stats
      });
    } catch (error) {
      console.error('Error fetching stats:', error);
      return res.status(500).json({
        success: false,
        message: 'Failed to fetch statistics',
        error: error.message
      });
    }
  }

  // Get stats grouped by date
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

      // Dynamically load the lender model
      const { model: Model } = UnifiedStatsController.getLenderModel(lender);

      const stats = await Model.getStatsByDate(startDate, endDate);

      return res.status(200).json({
        success: true,
        data: stats
      });
    } catch (error) {
      console.error('Error fetching stats by date:', error);
      return res.status(500).json({
        success: false,
        message: 'Failed to fetch statistics by date',
        error: error.message
      });
    }
  }

  // Get quick summary stats
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

      // Dynamically load the lender model
      const { model: Model, config } = UnifiedStatsController.getLenderModel(lender);

      // Call getQuickStats with optional date range
      const quickStats = await Model.getQuickStats(null, startDate, endDate);

      // Verify we got valid data back
      if (!quickStats || typeof quickStats !== 'object') {
        throw new Error('getQuickStats returned invalid data');
      }

      // Add lender metadata
      quickStats.lender = lender;
      quickStats.displayName = config.displayName;

      return res.status(200).json({
        success: true,
        data: quickStats
      });
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

  // Get logs by date range
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

      // Dynamically load the lender model
      const { model: Model } = UnifiedStatsController.getLenderModel(lender);

      const options = {
        limit: parseInt(limit) || 100,
        lastEvaluatedKey: lastEvaluatedKey ? JSON.parse(lastEvaluatedKey) : undefined
      };

      const result = await Model.findByDateRange(startDate, endDate, options);

      return res.status(200).json({
        success: true,
        data: result
      });
    } catch (error) {
      console.error('Error fetching logs by date range:', error);
      return res.status(500).json({
        success: false,
        message: 'Failed to fetch logs by date range',
        error: error.message
      });
    }
  }

  // Get all stats for all lenders (summary)
  static async getAllLendersStats(req, res) {
    try {
      const { startDate, endDate } = req.query;

      // Default to last 30 days
      let start = startDate;
      let end = endDate;

      if (!start || !end) {
        const now = new Date();
        end = now.toISOString();
        const thirtyDaysAgo = new Date(now.setDate(now.getDate() - 30));
        start = thirtyDaysAgo.toISOString();
      }

      const allStats = {};

      // Fetch stats for each lender dynamically
      for (const [lenderName, lenderConfig] of Object.entries(LENDER_CONFIG)) {
        try {
          // Dynamically load model
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

      return res.status(200).json({
        success: true,
        data: allStats
      });
    } catch (error) {
      console.error('Error fetching all lenders stats:', error);
      return res.status(500).json({
        success: false,
        message: 'Failed to fetch all lenders statistics',
        error: error.message
      });
    }
  }

  // Get comparison stats for multiple lenders
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

      // Default to last 30 days
      let start = startDate;
      let end = endDate;

      if (!start || !end) {
        const now = new Date();
        end = now.toISOString();
        const thirtyDaysAgo = new Date(now.setDate(now.getDate() - 30));
        start = thirtyDaysAgo.toISOString();
      }

      const lenderList = lenders.split(',').map(l => l.trim().toLowerCase());
      const comparison = {};

      for (const lenderName of lenderList) {
        try {
          // Dynamically load model
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

      return res.status(200).json({
        success: true,
        data: comparison
      });
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
const express = require('express');
const router = express.Router();
const UnifiedStatsController = require('../controllers/statsController');

/**
 * @route   GET /api/unified-stats/lenders
 * @desc    Get list of available lenders
 * @access  Private
 */
router.get('/lenders', UnifiedStatsController.getAvailableLenders);

/**
 * @route   GET /api/unified-stats/quick
 * @desc    Get quick summary statistics for a specific lender
 * @query   lender (required) - Lender name (e.g., 'fatakpay', 'ovly')
 * @access  Private
 * @example /api/unified-stats/quick?lender=fatakpay
 */
router.get('/quick', UnifiedStatsController.getQuickStats);

/**
 * @route   GET /api/unified-stats
 * @desc    Get comprehensive statistics for a specific lender
 * @query   lender (required) - Lender name
 * @query   startDate (optional) - ISO date string
 * @query   endDate (optional) - ISO date string
 * @access  Private
 * @example /api/unified-stats?lender=ovly&startDate=2025-01-01T00:00:00Z&endDate=2025-01-09T23:59:59Z
 */
router.get('/', UnifiedStatsController.getStats);

/**
 * @route   GET /api/unified-stats/by-date
 * @desc    Get statistics grouped by date for a specific lender
 * @query   lender (required) - Lender name
 * @query   startDate (required) - ISO date string
 * @query   endDate (required) - ISO date string
 * @access  Private
 * @example /api/unified-stats/by-date?lender=fatakpay&startDate=2025-01-01T00:00:00Z&endDate=2025-01-09T23:59:59Z
 */
router.get('/by-date', UnifiedStatsController.getStatsByDate);

/**
 * @route   GET /api/unified-stats/logs
 * @desc    Get logs by date range for a specific lender with pagination
 * @query   lender (required) - Lender name
 * @query   startDate (required) - ISO date string
 * @query   endDate (required) - ISO date string
 * @query   limit (optional) - Number of records (default: 100)
 * @query   lastEvaluatedKey (optional) - Pagination key
 * @access  Private
 * @example /api/unified-stats/logs?lender=ovly&startDate=2025-01-01T00:00:00Z&endDate=2025-01-09T23:59:59Z&limit=50
 */
router.get('/logs', UnifiedStatsController.getLogsByDateRange);

/**
 * @route   GET /api/unified-stats/all
 * @desc    Get statistics for all lenders (summary)
 * @query   startDate (optional) - ISO date string
 * @query   endDate (optional) - ISO date string
 * @access  Private
 * @example /api/unified-stats/all?startDate=2025-01-01T00:00:00Z&endDate=2025-01-09T23:59:59Z
 */
router.get('/all', UnifiedStatsController.getAllLendersStats);

/**
 * @route   GET /api/unified-stats/comparison
 * @desc    Get comparison statistics for multiple lenders
 * @query   lenders (required) - Comma-separated lender names (e.g., 'fatakpay,ovly')
 * @query   startDate (optional) - ISO date string
 * @query   endDate (optional) - ISO date string
 * @access  Private
 * @example /api/unified-stats/comparison?lenders=fatakpay,ovly&startDate=2025-01-01T00:00:00Z
 */
router.get('/comparison', UnifiedStatsController.getComparison);

module.exports = router;
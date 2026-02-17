const express = require('express');
const router  = express.Router();

const { authenticate, requireAdmin } = require('../middlewares/auth');
const authController   = require('../controllers/authController');
const leadPortalController = require('../controllers/leadPortalController');

// ─────────────────────────────────────────────────────────────────────────────
// AUTH ROUTES  →  /api/auth/...
// ─────────────────────────────────────────────────────────────────────────────

/**
 * POST /api/auth/login
 * Public — no token required.
 * Body: { email, password }
 * Response: { success, token, user }
 */
router.post('/auth/login', authController.login);

/**
 * POST /api/auth/register
 * Admin only — protect in production behind requireAdmin.
 * Body: { email, password, name, lenderName, source, role? }
 * Response: { success, user }
 */
router.post('/auth/register', authenticate, requireAdmin, authController.register);

/**
 * GET /api/auth/me
 * Returns the currently logged-in user's profile.
 * Header: Authorization: Bearer <token>
 */
router.get('/auth/me', authenticate, authController.me);

/**
 * PATCH /api/auth/password
 * Change password for the logged-in user.
 * Header: Authorization: Bearer <token>
 * Body: { currentPassword, newPassword }
 */
router.patch('/auth/password', authenticate, authController.updatePassword);


// ─────────────────────────────────────────────────────────────────────────────
// LENDER ROUTES  →  /api/lender/...
// ALL routes below require a valid JWT.
// The source scope is automatically applied from req.user.source.
// ─────────────────────────────────────────────────────────────────────────────

/**
 * GET /api/lender/stats
 * Summary counts for the logged-in lender's source.
 * Query: ?startDate=YYYY-MM-DD&endDate=YYYY-MM-DD
 * Response:
 * {
 *   source, dateRange,
 *   stats: { totalSent, totalInRange, accepted, sent }
 * }
 */
router.get('/lender/stats', authenticate, leadPortalController.getStats);

/**
 * GET /api/lender/leads/all
 * All leads (accepted + sent) for this source.
 * Query: ?startDate=&endDate=&page=1&limit=50&search=&status=all|accepted|sent
 * Response: { source, dateRange, pagination, leads: [...] }
 *
 * Lead shape:
 * { successId, leadId, name, mobile, pan, email, accepted, status, dateSent, lenders? }
 */
router.get('/lender/leads/all', authenticate, leadPortalController.getAllLeads);

/**
 * GET /api/lender/leads/accepted
 * Only leads with accepted >= 1.
 * Query: ?startDate=&endDate=&page=1&limit=50&search=
 * Response: { source, dateRange, pagination, leads: [...] }
 *
 * Lead shape includes `lenders` object showing which lenders accepted.
 */
router.get('/lender/leads/accepted', authenticate, leadPortalController.getAcceptedLeads);

/**
 * GET /api/lender/leads/sent
 * Only leads with accepted === 0 (sent but no lender accepted yet).
 * Query: ?startDate=&endDate=&page=1&limit=50&search=
 * Response: { source, dateRange, pagination, leads: [...] }
 */
router.get('/lender/leads/sent', authenticate, leadPortalController.getSentLeads);

/**
 * GET /api/lender/leads/:leadId
 * Single lead detail. Returns 403 if lead belongs to a different source.
 * Response: { lead: { ...fullDetails, lenders: { OVLY: bool, FREO: bool, ... } } }
 */
router.get('/lender/leads/:leadId', authenticate, leadPortalController.getLeadById);


module.exports = router;
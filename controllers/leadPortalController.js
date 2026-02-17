const Lead        = require('../models/leadModel');
const LeadSuccess = require('../models/leadSuccessModel');

// ─────────────────────────────────────────────────────────────────────────────
// HELPERS
// ─────────────────────────────────────────────────────────────────────────────

const LENDERS = [
  'OVLY', 'FREO', 'LendingPlate', 'ZYPE', 'FINTIFI',
  'FATAKPAY', 'RAMFINCROP', 'MyMoneyMantra', 'INDIALENDS', 'CRMPaisa', 'SML', 'MPOKKET',
];

/**
 * Count how many lenders accepted a lead_success record.
 */
function countAccepted(record) {
  return LENDERS.reduce((n, l) => n + (record[l] === true ? 1 : 0), 0);
}

/**
 * Parse & validate date range query params.
 * Falls back to "last 30 days" when not provided.
 */
function parseDateRange(query) {
  let { startDate, endDate } = query;

  if (!startDate || !endDate) {
    const now  = new Date();
    endDate    = now.toISOString();
    const past = new Date(now);
    past.setDate(past.getDate() - 30);
    startDate  = past.toISOString();
  } else {
    // Accept plain dates like "2026-01-01" → add time bounds
    if (!startDate.includes('T')) startDate = `${startDate}T00:00:00.000Z`;
    if (!endDate.includes('T'))   endDate   = `${endDate}T23:59:59.999Z`;
  }

  return { startDate, endDate };
}

/**
 * Fetch ALL pages of lead_success for a source in a date range.
 */
async function fetchAllSuccess(source, startDate, endDate) {
  let all     = [];
  let lastKey = null;

  do {
    const { items, lastEvaluatedKey } = await LeadSuccess.findBySource(source, {
      limit:          500,
      startDate,
      endDate,
      sortAscending:  false,
      lastEvaluatedKey: lastKey,
    });
    all     = all.concat(items);
    lastKey = lastEvaluatedKey;
  } while (lastKey);

  return all;
}

// ─────────────────────────────────────────────────────────────────────────────
// GET /api/lender/stats
// Returns summary counts for the logged-in lender's source.
// Query: ?startDate=YYYY-MM-DD&endDate=YYYY-MM-DD
// ─────────────────────────────────────────────────────────────────────────────
async function getStats(req, res) {
  try {
    const source              = req.user.source;   // ← scoped to logged-in user
    const { startDate, endDate } = parseDateRange(req.query);

    // Total leads sent (from leads table)
    const sentResult = await Lead.findBySource(source, {
      limit:    1,    // we only need the count — use countBySource instead
    });
    const totalSent = await Lead.countBySource(source);

    // LeadSuccess items for this source
    const successItems = await fetchAllSuccess(source, startDate, endDate);

    const accepted = successItems.filter(i => countAccepted(i) > 0);
    const sent     = successItems.filter(i => countAccepted(i) === 0);

    return res.status(200).json({
      success: true,
      source,
      dateRange:      { startDate, endDate },
      stats: {
        totalSent:          totalSent,
        totalInRange:       successItems.length,
        accepted:           accepted.length,
        sent:               sent.length,       // 0 lenders accepted
        // disbursed: coming later
      },
    });
  } catch (err) {
    console.error('[lender/getStats]', err);
    return res.status(500).json({ success: false, message: 'Failed to fetch stats.' });
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// GET /api/lender/leads/accepted
// Leads where at least 1 lender accepted (accepted > 0).
// Query: ?startDate=&endDate=&page=1&limit=50&search=
// ─────────────────────────────────────────────────────────────────────────────
async function getAcceptedLeads(req, res) {
  try {
    const source                 = req.user.source;
    const { startDate, endDate } = parseDateRange(req.query);
    const page   = Math.max(1, parseInt(req.query.page  || '1'));
    const limit  = Math.min(200, Math.max(1, parseInt(req.query.limit || '50')));
    const search = (req.query.search || '').toLowerCase().trim();

    // Fetch all success records for this source in range
    const successItems = await fetchAllSuccess(source, startDate, endDate);

    // Keep only accepted (≥1 lender)
    let accepted = successItems
      .filter(i => countAccepted(i) > 0)
      .map(i => ({
        successId:   i.successId,
        leadId:      i.leadId,
        name:        i.fullName    || null,
        mobile:      i.phone       || null,
        pan:         i.panNumber   || null,
        email:       i.email       || null,
        accepted:    countAccepted(i),
        dateSent:    i.createdAt,
        source:      i.source,
        // Individual lender flags
        lenders: LENDERS.reduce((acc, l) => ({ ...acc, [l]: i[l] === true }), {}),
      }));

    // Optional search filter
    if (search) {
      accepted = accepted.filter(
        l =>
          (l.name   && l.name.toLowerCase().includes(search))   ||
          (l.mobile && l.mobile.includes(search))               ||
          (l.pan    && l.pan.toLowerCase().includes(search))    ||
          (l.email  && l.email.toLowerCase().includes(search))
      );
    }

    // Sort by accepted count desc, then by date desc
    accepted.sort((a, b) => b.accepted - a.accepted || new Date(b.dateSent) - new Date(a.dateSent));

    // Paginate
    const total      = accepted.length;
    const totalPages = Math.ceil(total / limit);
    const data       = accepted.slice((page - 1) * limit, page * limit);

    return res.status(200).json({
      success: true,
      source,
      dateRange: { startDate, endDate },
      pagination: { page, limit, total, totalPages },
      leads: data,
    });
  } catch (err) {
    console.error('[lender/getAcceptedLeads]', err);
    return res.status(500).json({ success: false, message: 'Failed to fetch accepted leads.' });
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// GET /api/lender/leads/sent
// Leads where 0 lenders accepted.
// Query: ?startDate=&endDate=&page=1&limit=50&search=
// ─────────────────────────────────────────────────────────────────────────────
async function getSentLeads(req, res) {
  try {
    const source                 = req.user.source;
    const { startDate, endDate } = parseDateRange(req.query);
    const page   = Math.max(1, parseInt(req.query.page  || '1'));
    const limit  = Math.min(200, Math.max(1, parseInt(req.query.limit || '50')));
    const search = (req.query.search || '').toLowerCase().trim();

    const successItems = await fetchAllSuccess(source, startDate, endDate);

    // Keep only "sent" (0 lenders accepted)
    let sent = successItems
      .filter(i => countAccepted(i) === 0)
      .map(i => ({
        successId: i.successId,
        leadId:    i.leadId,
        name:      i.fullName  || null,
        mobile:    i.phone     || null,
        pan:       i.panNumber || null,
        email:     i.email     || null,
        accepted:  0,
        dateSent:  i.createdAt,
        source:    i.source,
      }));

    if (search) {
      sent = sent.filter(
        l =>
          (l.name   && l.name.toLowerCase().includes(search))  ||
          (l.mobile && l.mobile.includes(search))              ||
          (l.pan    && l.pan.toLowerCase().includes(search))   ||
          (l.email  && l.email.toLowerCase().includes(search))
      );
    }

    // Sort by newest first
    sent.sort((a, b) => new Date(b.dateSent) - new Date(a.dateSent));

    const total      = sent.length;
    const totalPages = Math.ceil(total / limit);
    const data       = sent.slice((page - 1) * limit, page * limit);

    return res.status(200).json({
      success: true,
      source,
      dateRange: { startDate, endDate },
      pagination: { page, limit, total, totalPages },
      leads: data,
    });
  } catch (err) {
    console.error('[lender/getSentLeads]', err);
    return res.status(500).json({ success: false, message: 'Failed to fetch sent leads.' });
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// GET /api/lender/leads/all
// All leads (accepted + sent combined) for this source.
// Useful for the "All Leads" tab in the dashboard.
// Query: ?startDate=&endDate=&page=1&limit=50&search=&status=accepted|sent
// ─────────────────────────────────────────────────────────────────────────────
async function getAllLeads(req, res) {
  try {
    const source                 = req.user.source;
    const { startDate, endDate } = parseDateRange(req.query);
    const page   = Math.max(1, parseInt(req.query.page  || '1'));
    const limit  = Math.min(200, Math.max(1, parseInt(req.query.limit || '50')));
    const search = (req.query.search || '').toLowerCase().trim();
    const status = req.query.status || 'all';   // 'all' | 'accepted' | 'sent'

    const successItems = await fetchAllSuccess(source, startDate, endDate);

    let leads = successItems.map(i => {
      const acceptedCount = countAccepted(i);
      return {
        successId:    i.successId,
        leadId:       i.leadId,
        name:         i.fullName  || null,
        mobile:       i.phone     || null,
        pan:          i.panNumber || null,
        email:        i.email     || null,
        accepted:     acceptedCount,
        status:       acceptedCount > 0 ? 'Accepted' : 'Sent',
        dateSent:     i.createdAt,
        source:       i.source,
        // Lender breakdown only for accepted leads
        ...(acceptedCount > 0 && {
          lenders: LENDERS.reduce((acc, l) => ({ ...acc, [l]: i[l] === true }), {}),
        }),
      };
    });

    // Status filter
    if (status === 'accepted') leads = leads.filter(l => l.status === 'Accepted');
    if (status === 'sent')     leads = leads.filter(l => l.status === 'Sent');

    // Search filter
    if (search) {
      leads = leads.filter(
        l =>
          (l.name   && l.name.toLowerCase().includes(search))  ||
          (l.mobile && l.mobile.includes(search))              ||
          (l.pan    && l.pan.toLowerCase().includes(search))   ||
          (l.email  && l.email.toLowerCase().includes(search))
      );
    }

    // Sort: accepted first, then by date desc
    leads.sort((a, b) =>
      b.accepted - a.accepted || new Date(b.dateSent) - new Date(a.dateSent)
    );

    const total      = leads.length;
    const totalPages = Math.ceil(total / limit);
    const data       = leads.slice((page - 1) * limit, page * limit);

    return res.status(200).json({
      success:   true,
      source,
      dateRange: { startDate, endDate },
      pagination: { page, limit, total, totalPages },
      leads:     data,
    });
  } catch (err) {
    console.error('[lender/getAllLeads]', err);
    return res.status(500).json({ success: false, message: 'Failed to fetch leads.' });
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// GET /api/lender/leads/:leadId
// Single lead detail — must belong to user's source.
// ─────────────────────────────────────────────────────────────────────────────
async function getLeadById(req, res) {
  try {
    const source   = req.user.source;
    const { leadId } = req.params;

    // Fetch lead_success by leadId
    const successRecord = await LeadSuccess.findByLeadId(leadId);
    if (!successRecord) {
      return res.status(404).json({ success: false, message: 'Lead not found.' });
    }

    // Source guard — user can only see their own source data
    if (successRecord.source !== source) {
      return res.status(403).json({ success: false, message: 'Access denied.' });
    }

    // Optionally enrich with raw lead data
    let leadData = null;
    try {
      leadData = await Lead.findById(leadId);
    } catch (_) {
      // non-fatal — raw lead may not exist
    }

    const acceptedCount = countAccepted(successRecord);

    return res.status(200).json({
      success: true,
      lead: {
        successId:   successRecord.successId,
        leadId:      successRecord.leadId,
        name:        successRecord.fullName  || leadData?.fullName  || null,
        mobile:      successRecord.phone     || leadData?.phone     || null,
        pan:         successRecord.panNumber || leadData?.panNumber || null,
        email:       successRecord.email     || leadData?.email     || null,
        dob:         leadData?.dateOfBirth   || null,
        salary:      leadData?.salary        || null,
        jobType:     leadData?.jobType       || null,
        creditScore: leadData?.creditScore   || leadData?.cibilScore || null,
        pincode:     leadData?.pincode       || null,
        accepted:    acceptedCount,
        status:      acceptedCount > 0 ? 'Accepted' : 'Sent',
        dateSent:    successRecord.createdAt,
        source:      successRecord.source,
        lenders:     LENDERS.reduce((acc, l) => ({
          ...acc,
          [l]: successRecord[l] === true,
        }), {}),
      },
    });
  } catch (err) {
    console.error('[lender/getLeadById]', err);
    return res.status(500).json({ success: false, message: 'Failed to fetch lead.' });
  }
}

module.exports = {
  getStats,
  getAcceptedLeads,
  getSentLeads,
  getAllLeads,
  getLeadById,
};
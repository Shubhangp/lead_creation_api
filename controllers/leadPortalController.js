const Lead = require('../models/leadModel');

// All lenders — immediate API push + MIS-synced
const ALL_LENDERS = [
  'OVLY', 'FREO', 'LendingPlate', 'ZYPE', 'FINTIFI',
  'FATAKPAY', 'FATAKPAYPL', 'RAMFINCROP', 'MyMoneyMantra',
  'INDIALENDS', 'CRMPaisa', 'CreditSea', 'SML', 'MPOKKET',
  'CASHVIA', 'DIGICREDIT', 'TAP4CREDIT', 'SPEEDOLOAN',
  'PAISABOXX', 'HEROFINCORP', 'PREFR',
];

// ── Helpers ───────────────────────────────────────────────────────────────────

function parseDateRange(query) {
  let { startDate, endDate } = query;

  if (!startDate || !endDate) {
    const now  = new Date();
    endDate    = now.toISOString();
    const past = new Date(now);
    past.setDate(past.getDate() - 30);
    startDate  = past.toISOString();
  } else {
    if (!startDate.includes('T')) startDate = `${startDate}T00:00:00.000Z`;
    if (!endDate.includes('T'))   endDate   = `${endDate}T23:59:59.999Z`;
  }

  return { startDate, endDate };
}

// successfulLenders is now the source of truth — stored directly on the lead
function getSuccessfulLenders(lead) {
  return Array.isArray(lead.successfulLenders) ? lead.successfulLenders : [];
}

// Build per-lender status map for the detail view
// Uses lenderStatuses.{KEY} flat attributes + successfulLenders for booleans
function buildLenderMap(lead) {
  const successful = getSuccessfulLenders(lead);
  const map = {};

  for (const lender of ALL_LENDERS) {
    const statusKey = `lenderStatuses.${lender}`;
    const statusEntry = lead[statusKey] || null;

    map[lender] = {
      accepted: successful.includes(lender),
      status:   statusEntry?.status   || null,
      details:  statusEntry           || null,
    };
  }

  return map;
}

// Fetch all leads for a source + date range (paginated internally)
async function fetchLeadsInRange(source, startDate, endDate) {
  const all     = [];
  let lastKey   = null;
  const seenIds = new Set();

  do {
    const { items, lastEvaluatedKey } = await Lead.findBySource(source, {
      limit:            500,
      startDate,
      endDate,
      sortAscending:    false,
      lastEvaluatedKey: lastKey,
    });

    for (const item of items) {
      if (!seenIds.has(item.leadId)) {
        seenIds.add(item.leadId);
        all.push(item);
      }
    }

    lastKey = lastEvaluatedKey;
  } while (lastKey);

  return all;
}

// ── Route handlers ────────────────────────────────────────────────────────────

/**
 * GET /api/lender/stats
 * Query: ?startDate=YYYY-MM-DD&endDate=YYYY-MM-DD
 */
async function getStats(req, res) {
  try {
    const source = req.user.source;
    const { startDate, endDate } = parseDateRange(req.query);

    console.log(`[getStats] source=${source}, ${startDate} → ${endDate}`);

    // All-time total sent
    let totalSentAllTime = 0;
    try {
      totalSentAllTime = await Lead.countBySource(source);
    } catch (e) {
      console.error('[getStats] countBySource error:', e.message);
    }

    // Leads in date range — source of truth for all stats
    const leads = await fetchLeadsInRange(source, startDate, endDate);

    const accepted = leads.filter(l => getSuccessfulLenders(l).length > 0);
    const sent     = leads.filter(l => getSuccessfulLenders(l).length === 0);

    // Per-lender acceptance breakdown
    const lenderBreakdown = {};
    for (const lender of ALL_LENDERS) {
      lenderBreakdown[lender] = 0;
    }
    for (const lead of accepted) {
      for (const lender of getSuccessfulLenders(lead)) {
        if (lenderBreakdown[lender] !== undefined) {
          lenderBreakdown[lender]++;
        }
      }
    }

    return res.status(200).json({
      success: true,
      source,
      dateRange: { startDate, endDate },
      stats: {
        totalSent:        totalSentAllTime,
        totalSentInRange: leads.length,
        accepted:         accepted.length,
        sent:             sent.length,
        lenderBreakdown,
      },
    });
  } catch (err) {
    console.error('[getStats] Error:', err);
    return res.status(500).json({ success: false, message: 'Failed to fetch stats.' });
  }
}

/**
 * GET /api/lender/leads/accepted
 */
async function getAcceptedLeads(req, res) {
  try {
    const source = req.user.source;
    const { startDate, endDate } = parseDateRange(req.query);
    const page   = Math.max(1, parseInt(req.query.page  || '1'));
    const limit  = Math.min(200, Math.max(1, parseInt(req.query.limit || '50')));
    const search = (req.query.search || '').toLowerCase().trim();

    const leads = await fetchLeadsInRange(source, startDate, endDate);

    let accepted = leads
      .filter(l => getSuccessfulLenders(l).length > 0)
      .map(l => {
        const successfulLenders = getSuccessfulLenders(l);
        return {
          leadId:           l.leadId,
          name:             l.fullName  || null,
          mobile:           l.phone     || null,
          pan:              l.panNumber || null,
          email:            l.email     || null,
          accepted:         successfulLenders.length,
          successfulLenders,
          dateSent:         l.createdAt,
          source:           l.source,
          status:           'Accepted',
        };
      });

    if (search) {
      accepted = accepted.filter(l =>
        (l.name   && l.name.toLowerCase().includes(search))  ||
        (l.mobile && l.mobile.includes(search))              ||
        (l.pan    && l.pan.toLowerCase().includes(search))   ||
        (l.email  && l.email.toLowerCase().includes(search))
      );
    }

    accepted.sort((a, b) => b.accepted - a.accepted || new Date(b.dateSent) - new Date(a.dateSent));

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
    console.error('[getAcceptedLeads]', err);
    return res.status(500).json({ success: false, message: 'Failed to fetch accepted leads.' });
  }
}

/**
 * GET /api/lender/leads/sent
 */
async function getSentLeads(req, res) {
  try {
    const source = req.user.source;
    const { startDate, endDate } = parseDateRange(req.query);
    const page   = Math.max(1, parseInt(req.query.page  || '1'));
    const limit  = Math.min(200, Math.max(1, parseInt(req.query.limit || '50')));
    const search = (req.query.search || '').toLowerCase().trim();

    const leads = await fetchLeadsInRange(source, startDate, endDate);

    let sent = leads
      .filter(l => getSuccessfulLenders(l).length === 0)
      .map(l => ({
        leadId:   l.leadId,
        name:     l.fullName  || null,
        mobile:   l.phone     || null,
        pan:      l.panNumber || null,
        email:    l.email     || null,
        accepted: 0,
        dateSent: l.createdAt,
        source:   l.source,
        status:   'Sent',
      }));

    if (search) {
      sent = sent.filter(l =>
        (l.name   && l.name.toLowerCase().includes(search))  ||
        (l.mobile && l.mobile.includes(search))              ||
        (l.pan    && l.pan.toLowerCase().includes(search))   ||
        (l.email  && l.email.toLowerCase().includes(search))
      );
    }

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
    console.error('[getSentLeads]', err);
    return res.status(500).json({ success: false, message: 'Failed to fetch sent leads.' });
  }
}

/**
 * GET /api/lender/leads
 * Query: ?status=all|accepted|sent
 */
async function getAllLeads(req, res) {
  try {
    const source = req.user.source;
    const { startDate, endDate } = parseDateRange(req.query);
    const page   = Math.max(1, parseInt(req.query.page  || '1'));
    const limit  = Math.min(200, Math.max(1, parseInt(req.query.limit || '50')));
    const search = (req.query.search || '').toLowerCase().trim();
    const status = req.query.status || 'all';

    const leads = await fetchLeadsInRange(source, startDate, endDate);

    let result = leads.map(l => {
      const successfulLenders = getSuccessfulLenders(l);
      return {
        leadId:           l.leadId,
        name:             l.fullName  || null,
        mobile:           l.phone     || null,
        pan:              l.panNumber || null,
        email:            l.email     || null,
        accepted:         successfulLenders.length,
        successfulLenders,
        status:           successfulLenders.length > 0 ? 'Accepted' : 'Sent',
        dateSent:         l.createdAt,
        source:           l.source,
      };
    });

    if (status === 'accepted') result = result.filter(l => l.status === 'Accepted');
    if (status === 'sent')     result = result.filter(l => l.status === 'Sent');

    if (search) {
      result = result.filter(l =>
        (l.name   && l.name.toLowerCase().includes(search))  ||
        (l.mobile && l.mobile.includes(search))              ||
        (l.pan    && l.pan.toLowerCase().includes(search))   ||
        (l.email  && l.email.toLowerCase().includes(search))
      );
    }

    result.sort((a, b) => b.accepted - a.accepted || new Date(b.dateSent) - new Date(a.dateSent));

    const total      = result.length;
    const totalPages = Math.ceil(total / limit);
    const data       = result.slice((page - 1) * limit, page * limit);

    return res.status(200).json({
      success:   true,
      source,
      dateRange: { startDate, endDate },
      pagination: { page, limit, total, totalPages },
      leads:     data,
    });
  } catch (err) {
    console.error('[getAllLeads]', err);
    return res.status(500).json({ success: false, message: 'Failed to fetch leads.' });
  }
}

/**
 * GET /api/lender/leads/:leadId
 * Full detail view — includes per-lender status + details from lenderStatuses
 */
async function getLeadById(req, res) {
  try {
    const source       = req.user.source;
    const { leadId }   = req.params;

    const lead = await Lead.findById(leadId);
    if (!lead) {
      return res.status(404).json({ success: false, message: 'Lead not found.' });
    }

    if (lead.source !== source) {
      return res.status(403).json({ success: false, message: 'Access denied.' });
    }

    const successfulLenders = getSuccessfulLenders(lead);

    return res.status(200).json({
      success: true,
      lead: {
        leadId,
        name:             lead.fullName   || null,
        mobile:           lead.phone      || null,
        pan:              lead.panNumber  || null,
        email:            lead.email      || null,
        dob:              lead.dateOfBirth || null,
        salary:           lead.salary     || null,
        jobType:          lead.jobType    || null,
        creditScore:      lead.creditScore || lead.cibilScore || null,
        pincode:          lead.pincode    || null,
        accepted:         successfulLenders.length,
        successfulLenders,
        status:           successfulLenders.length > 0 ? 'Accepted' : 'Sent',
        dateSent:         lead.createdAt,
        source:           lead.source,
        // Full per-lender breakdown with status details from MIS uploads
        lenders:          buildLenderMap(lead),
      },
    });
  } catch (err) {
    console.error('[getLeadById]', err);
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

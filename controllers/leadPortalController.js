const Lead        = require('../models/leadModel');
const LeadSuccess = require('../models/leadSuccessModel');

const LENDERS = [
  'OVLY', 'FREO', 'LendingPlate', 'ZYPE', 'FINTIFI',
  'FATAKPAY', 'FATAKPAYPL', 'RAMFINCROP', 'MyMoneyMantra', 'INDIALENDS', 'CRMPaisa', 'SML', 'MPOKKET',
];

function countAccepted(record) {
  return LENDERS.reduce((n, l) => n + (record[l] === true ? 1 : 0), 0);
}

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

/**
 * ✅ FIXED: Deduplicate by successId to prevent duplicates from DynamoDB
 */
async function fetchAllSuccess(source, startDate, endDate) {
  let all     = [];
  let lastKey = null;
  const seenIds = new Set();

  do {
    const { items, lastEvaluatedKey } = await LeadSuccess.findBySource(source, {
      limit:          500,
      startDate,
      endDate,
      sortAscending:  false,
      lastEvaluatedKey: lastKey,
    });
    
    items.forEach(item => {
      if (!seenIds.has(item.successId)) {
        seenIds.add(item.successId);
        all.push(item);
      } else {
        console.warn(`[fetchAllSuccess] Skipping duplicate successId: ${item.successId}`);
      }
    });
    
    lastKey = lastEvaluatedKey;
  } while (lastKey);

  console.log(`[fetchAllSuccess] Total items: ${all.length}, Unique IDs: ${seenIds.size}`);
  return all;
}

/**
 * ✅ NEW: Fetch all leads from Lead table for date range (for accurate totalSent in range)
 */
async function fetchAllLeadsInRange(source, startDate, endDate) {
  let all = [];
  let lastKey = null;
  const seenIds = new Set();

  do {
    const { items, lastEvaluatedKey } = await Lead.findBySource(source, {
      limit: 500,
      startDate,
      endDate,
      sortAscending: false,
      lastEvaluatedKey: lastKey,
    });
    
    items.forEach(item => {
      if (!seenIds.has(item.leadId)) {
        seenIds.add(item.leadId);
        all.push(item);
      }
    });
    
    lastKey = lastEvaluatedKey;
  } while (lastKey);

  console.log(`[fetchAllLeadsInRange] Total leads in range: ${all.length}, Unique IDs: ${seenIds.size}`);
  return all;
}

/**
 * ✅ FIXED: GET /api/lender/stats
 * Uses Lead model's optimized functions for accurate source-specific counts
 * Query: ?startDate=YYYY-MM-DD&endDate=YYYY-MM-DD
 */
async function getStats(req, res) {
  try {
    const source = req.user.source;
    const { startDate, endDate } = parseDateRange(req.query);

    console.log(`[getStats] Fetching stats for source: ${source}, ${startDate} to ${endDate}`);

    // ✅ 1. Get all-time total sent using Lead.countBySource
    let totalSentAllTime = 0;
    try {
      totalSentAllTime = await Lead.getQuickStats(source, null, null);
      console.log(`[getStats] All-time total sent: ${totalSentAllTime}`);
    } catch (leadError) {
      console.error('[getStats] Error fetching all-time count:', leadError.message);
    }

    // ✅ 2. Get total sent IN DATE RANGE using Lead.findBySource with date filter
    let totalSentInRange = 0;
    try {
      const leadsInRange = await fetchAllLeadsInRange(source, startDate, endDate);
      totalSentInRange = leadsInRange.length;
      console.log(`[getStats] Total sent in range (${startDate} to ${endDate}): ${totalSentInRange}`);
    } catch (rangeError) {
      console.error('[getStats] Error fetching range count:', rangeError.message);
    }

    // ✅ 3. Get LeadSuccess items for acceptance stats
    const successItems = await fetchAllSuccess(source, startDate, endDate);
    console.log(`[getStats] Success items in range: ${successItems.length}`);

    const accepted = successItems.filter(i => countAccepted(i) > 0);
    const sent     = successItems.filter(i => countAccepted(i) === 0);

    console.log(`[getStats] Accepted: ${accepted.length}, Sent (0 lenders): ${sent.length}`);

    return res.status(200).json({
      success: true,
      source,
      dateRange: { startDate, endDate },
      stats: {
        totalSent: totalSentAllTime,           // ✅ All-time total
        totalSentInRange: totalSentInRange,    // ✅ NEW: Total in this date range
        totalInRange: successItems.length,     // Total with lender status
        accepted: accepted.length,             // Accepted by at least 1 lender
        sent: sent.length,                     // Sent but 0 lenders accepted
      },
    });
  } catch (err) {
    console.error('[lender/getStats] Error:', err);
    console.error('[lender/getStats] Stack:', err.stack);
    return res.status(500).json({ success: false, message: 'Failed to fetch stats.' });
  }
}

async function getAcceptedLeads(req, res) {
  try {
    const source = req.user.source;
    const { startDate, endDate } = parseDateRange(req.query);
    const page   = Math.max(1, parseInt(req.query.page  || '1'));
    const limit  = Math.min(200, Math.max(1, parseInt(req.query.limit || '50')));
    const search = (req.query.search || '').toLowerCase().trim();

    console.log(`[getAcceptedLeads] source=${source}, page=${page}, limit=${limit}`);

    const successItems = await fetchAllSuccess(source, startDate, endDate);

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
        status:      'Accepted',
        lenders: LENDERS.reduce((acc, l) => ({ ...acc, [l]: i[l] === true }), {}),
      }));

    if (search) {
      accepted = accepted.filter(
        l =>
          (l.name   && l.name.toLowerCase().includes(search))   ||
          (l.mobile && l.mobile.includes(search))               ||
          (l.pan    && l.pan.toLowerCase().includes(search))    ||
          (l.email  && l.email.toLowerCase().includes(search))
      );
    }

    accepted.sort((a, b) => b.accepted - a.accepted || new Date(b.dateSent) - new Date(a.dateSent));

    const total      = accepted.length;
    const totalPages = Math.ceil(total / limit);
    const data       = accepted.slice((page - 1) * limit, page * limit);

    console.log(`[getAcceptedLeads] Returning ${data.length} of ${total} leads`);

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

async function getSentLeads(req, res) {
  try {
    const source = req.user.source;
    const { startDate, endDate } = parseDateRange(req.query);
    const page   = Math.max(1, parseInt(req.query.page  || '1'));
    const limit  = Math.min(200, Math.max(1, parseInt(req.query.limit || '50')));
    const search = (req.query.search || '').toLowerCase().trim();

    const successItems = await fetchAllSuccess(source, startDate, endDate);

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
        status:    'Sent',
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

async function getAllLeads(req, res) {
  try {
    const source = req.user.source;
    const { startDate, endDate } = parseDateRange(req.query);
    const page   = Math.max(1, parseInt(req.query.page  || '1'));
    const limit  = Math.min(200, Math.max(1, parseInt(req.query.limit || '50')));
    const search = (req.query.search || '').toLowerCase().trim();
    const status = req.query.status || 'all';

    console.log(`[getAllLeads] source=${source}, page=${page}, limit=${limit}, status=${status}`);

    const successItems = await fetchAllSuccess(source, startDate, endDate);
    console.log(`[getAllLeads] Fetched ${successItems.length} unique items from LeadSuccess`);

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
        ...(acceptedCount > 0 && {
          lenders: LENDERS.reduce((acc, l) => ({ ...acc, [l]: i[l] === true }), {}),
        }),
      };
    });

    if (status === 'accepted') leads = leads.filter(l => l.status === 'Accepted');
    if (status === 'sent')     leads = leads.filter(l => l.status === 'Sent');

    if (search) {
      leads = leads.filter(
        l =>
          (l.name   && l.name.toLowerCase().includes(search))  ||
          (l.mobile && l.mobile.includes(search))              ||
          (l.pan    && l.pan.toLowerCase().includes(search))   ||
          (l.email  && l.email.toLowerCase().includes(search))
      );
    }

    leads.sort((a, b) =>
      b.accepted - a.accepted || new Date(b.dateSent) - new Date(a.dateSent)
    );

    const total      = leads.length;
    const totalPages = Math.ceil(total / limit);
    const data       = leads.slice((page - 1) * limit, page * limit);

    console.log(`[getAllLeads] Returning ${data.length} of ${total} leads`);

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

async function getLeadById(req, res) {
  try {
    const source   = req.user.source;
    const { leadId } = req.params;

    console.log(`[getLeadById] Fetching leadId=${leadId} for source=${source}`);

    const successRecord = await LeadSuccess.findByLeadId(leadId);
    if (!successRecord) {
      console.log(`[getLeadById] No success record found for leadId=${leadId}`);
      return res.status(404).json({ success: false, message: 'Lead not found.' });
    }

    if (successRecord.source !== source) {
      console.log(`[getLeadById] Source mismatch: ${successRecord.source} !== ${source}`);
      return res.status(403).json({ success: false, message: 'Access denied.' });
    }

    let leadData = null;
    try {
      leadData = await Lead.findById(leadId);
    } catch (err) {
      console.log('[getLeadById] Raw lead not found, using success record only');
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
    console.error('[lender/getLeadById] Stack:', err.stack);
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
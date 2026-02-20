const Lead        = require('../models/leadModel');
const LeadSuccess = require('../models/leadSuccessModel');

const LENDERS = [
  'OVLY', 'FREO', 'LendingPlate', 'ZYPE', 'FINTIFI',
  'FATAKPAY', 'FATAKPAYPL', 'RAMFINCROP', 'MyMoneyMantra',
  'INDIALENDS', 'CRMPaisa', 'SML', 'MPOKKET',
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

// ─────────────────────────────────────────────────────────────────────────────
// GET /api/lender/stats
// ─────────────────────────────────────────────────────────────────────────────
async function getStats(req, res) {
  try {
    const source                 = req.user.source;
    const { startDate, endDate } = parseDateRange(req.query);

    // ✅ Run totalSent count + LeadSuccess fetch IN PARALLEL
    const [totalSent, successItems] = await Promise.all([
      Lead.countBySource(source),
      LeadSuccess._fetchAllBySource(source, startDate, endDate)
    ]);
    console.log(successItems, totalSent);
    const accepted = successItems.filter(i => countAccepted(i) > 0);
    const sent     = successItems.filter(i => countAccepted(i) === 0);

    return res.status(200).json({
      success: true,
      source,
      dateRange: { startDate, endDate },
      stats: {
        totalSent,
        totalInRange: successItems.length,
        accepted:     accepted.length,
        sent:         sent.length,
      },
    });
  } catch (err) {
    console.error('[lender/getStats]', err);
    return res.status(500).json({ success: false, message: 'Failed to fetch stats.' });
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// GET /api/lender/leads/accepted
// ─────────────────────────────────────────────────────────────────────────────
async function getAcceptedLeads(req, res) {
  try {
    const source                 = req.user.source;
    const { startDate, endDate } = parseDateRange(req.query);
    const page   = Math.max(1,   parseInt(req.query.page  || '1'));
    const limit  = Math.min(200, Math.max(1, parseInt(req.query.limit || '50')));
    const search = (req.query.search || '').toLowerCase().trim();

    const successItems = await LeadSuccess._fetchAllBySource(source, startDate, endDate);

    let accepted = successItems
      .filter(i => countAccepted(i) > 0)
      .map(i => ({
        successId: i.successId,
        leadId:    i.leadId,
        name:      i.fullName    || null,
        mobile:    i.phone       || null,
        pan:       i.panNumber   || null,
        email:     i.email       || null,
        accepted:  countAccepted(i),
        dateSent:  i.createdAt,
        source:    i.source,
        lenders:   LENDERS.reduce((acc, l) => ({ ...acc, [l]: i[l] === true }), {}),
      }));

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
      success:    true,
      source,
      dateRange:  { startDate, endDate },
      pagination: { page, limit, total, totalPages },
      leads:      data,
    });
  } catch (err) {
    console.error('[lender/getAcceptedLeads]', err);
    return res.status(500).json({ success: false, message: 'Failed to fetch accepted leads.' });
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// GET /api/lender/leads/sent
// ─────────────────────────────────────────────────────────────────────────────
async function getSentLeads(req, res) {
  try {
    const source                 = req.user.source;
    const { startDate, endDate } = parseDateRange(req.query);
    const page   = Math.max(1,   parseInt(req.query.page  || '1'));
    const limit  = Math.min(200, Math.max(1, parseInt(req.query.limit || '50')));
    const search = (req.query.search || '').toLowerCase().trim();

    const successItems = await LeadSuccess._fetchAllBySource(source, startDate, endDate);

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
      sent = sent.filter(l =>
        (l.name   && l.name.toLowerCase().includes(search)) ||
        (l.mobile && l.mobile.includes(search))             ||
        (l.pan    && l.pan.toLowerCase().includes(search))  ||
        (l.email  && l.email.toLowerCase().includes(search))
      );
    }

    sent.sort((a, b) => new Date(b.dateSent) - new Date(a.dateSent));

    const total      = sent.length;
    const totalPages = Math.ceil(total / limit);
    const data       = sent.slice((page - 1) * limit, page * limit);

    return res.status(200).json({
      success:    true,
      source,
      dateRange:  { startDate, endDate },
      pagination: { page, limit, total, totalPages },
      leads:      data,
    });
  } catch (err) {
    console.error('[lender/getSentLeads]', err);
    return res.status(500).json({ success: false, message: 'Failed to fetch sent leads.' });
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// GET /api/lender/leads/all
// ─────────────────────────────────────────────────────────────────────────────
async function getAllLeads(req, res) {
  try {
    const source                 = req.user.source;
    const { startDate, endDate } = parseDateRange(req.query);
    const page   = Math.max(1,   parseInt(req.query.page  || '1'));
    const limit  = Math.min(200, Math.max(1, parseInt(req.query.limit || '50')));
    const search = (req.query.search || '').toLowerCase().trim();
    const status = req.query.status || 'all';

    const successItems = await LeadSuccess._fetchAllBySource(source, startDate, endDate);

    let leads = successItems.map(i => {
      const acceptedCount = countAccepted(i);
      return {
        successId: i.successId,
        leadId:    i.leadId,
        name:      i.fullName  || null,
        mobile:    i.phone     || null,
        pan:       i.panNumber || null,
        email:     i.email     || null,
        accepted:  acceptedCount,
        status:    acceptedCount > 0 ? 'Accepted' : 'Sent',
        dateSent:  i.createdAt,
        source:    i.source,
        ...(acceptedCount > 0 && {
          lenders: LENDERS.reduce((acc, l) => ({ ...acc, [l]: i[l] === true }), {}),
        }),
      };
    });

    if (status === 'accepted') leads = leads.filter(l => l.status === 'Accepted');
    if (status === 'sent')     leads = leads.filter(l => l.status === 'Sent');

    if (search) {
      leads = leads.filter(l =>
        (l.name   && l.name.toLowerCase().includes(search)) ||
        (l.mobile && l.mobile.includes(search))             ||
        (l.pan    && l.pan.toLowerCase().includes(search))  ||
        (l.email  && l.email.toLowerCase().includes(search))
      );
    }

    leads.sort((a, b) => b.accepted - a.accepted || new Date(b.dateSent) - new Date(a.dateSent));

    const total      = leads.length;
    const totalPages = Math.ceil(total / limit);
    const data       = leads.slice((page - 1) * limit, page * limit);

    return res.status(200).json({
      success:    true,
      source,
      dateRange:  { startDate, endDate },
      pagination: { page, limit, total, totalPages },
      leads:      data,
    });
  } catch (err) {
    console.error('[lender/getAllLeads]', err);
    return res.status(500).json({ success: false, message: 'Failed to fetch leads.' });
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// GET /api/lender/leads/:leadId
// ─────────────────────────────────────────────────────────────────────────────
async function getLeadById(req, res) {
  try {
    const source     = req.user.source;
    const { leadId } = req.params;

    // ✅ Fetch successRecord + raw leadData in parallel
    const [successRecord, leadData] = await Promise.all([
      LeadSuccess.findByLeadId(leadId),
      Lead.findById(leadId).catch(() => null)   // non-fatal
    ]);

    if (!successRecord) {
      return res.status(404).json({ success: false, message: 'Lead not found.' });
    }

    if (successRecord.source !== source) {
      return res.status(403).json({ success: false, message: 'Access denied.' });
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
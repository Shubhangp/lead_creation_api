const Lead = require('../models/leadModel');
const PendingLead = require('../models/pendingLeadModel');
const DistributionRule = require('../models/distributionRuleModel');
const timeUtils = require('../utils/timeutils');
const rcsService = require('../services/rcsService');

// Import lender services
const {
  sendToSML,
  sendToFreo,
  sendToZYPE,
  sendToLendingPlate,
  sendToFINTIFI,
  sendToFATAKPAY,
  sendToOVLY,
  sendToRAMFINCROP,
  sendToMyMoneyMantra,
  sendToMpokket,
  sendToIndiaLends,
  sendToCrmPaisa
} = require('../services/lenderService');

// Import log models for tracking
const SMLResponseLog = require('../models/smlResponseLogModel');
const FreoResponseLog = require('../models/freoResponseLogModel');
const OvlyResponseLog = require('../models/ovlyResponseLog');
const LendingPlateResponseLog = require('../models/leadingPlateResponseLog');
const ZypeResponseLog = require('../models/ZypeResponseLogModel');
const FintifiResponseLog = require('../models/fintifiResponseLog');
const FatakPayResponseLog = require('../models/fatakPayResponseLog');
const RamFinCropLog = require('../models/ramFinCropLogModel');
const IndiaLendsResponseLog = require('../models/indiaLendsResponseLog');
const MpokketResponseLog = require('../models/mpokketResponseLog');
const CrmPaisaResponseLog = require('../models/crmPaisaResponseLogModel');
const MMMResponseLog = require('../models/mmmResponseLog');
const LeadSuccess = require('../models/leadSuccessModel');

// Create a lead with optimized time-based distribution
exports.createLead = async (req, res) => {
  const {
    source, fullName, firstName, lastName, phone, email,
    age, dateOfBirth, gender, panNumber, jobType, businessType,
    salary, creditScore, cibilScore, address, pincode, consent
  } = req.body;

  // Validations
  if (!source || !fullName || !phone || !email || !panNumber || consent === undefined) {
    return res.status(400).json({ 
      message: 'Source, fullName, phone, email, panNumber, and consent are required.' 
    });
  }

  if (fullName.length < 2 || fullName.length > 100) {
    return res.status(400).json({ 
      message: 'Full name must be between 2 and 100 characters.' 
    });
  }

  const emailRegex = /[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,4}$/;
  if (!emailRegex.test(email)) {
    return res.status(400).json({ message: 'Invalid email format.' });
  }

  const panRegex = /^[A-Z]{5}[0-9]{4}[A-Z]$/;
  if (!panRegex.test(panNumber)) {
    return res.status(400).json({ 
      message: 'Invalid PAN number format. Must match ABCDE1234F.' 
    });
  }

  const isValidDate = /^\d{4}-\d{2}-\d{2}$/.test(dateOfBirth) && !isNaN(Date.parse(dateOfBirth));
  if (dateOfBirth && (!isValidDate || new Date(dateOfBirth) > new Date())) {
    return res.status(400).json({ 
      message: 'Invalid date of birth or date cannot be in the future.' 
    });
  }

  const finalSalary = salary || '50000';
  const finalJobType = jobType || 'SALARIED';

  try {
    // Create lead data object
    const leadData = {
      source,
      fullName,
      firstName,
      lastName,
      phone,
      email,
      age,
      dateOfBirth,
      gender,
      panNumber,
      jobType: finalJobType,
      businessType,
      salary: finalSalary,
      creditScore,
      cibilScore,
      address,
      pincode,
      consent
    };

    // Create lead in database
    const savedLead = await Lead.create(leadData);

    // Get distribution rules
    const distributionRules = await getDistributionRules(source);

    // Process lenders with optimized approach
    const {
      sentImmediately,
      pendingLenders,
      immediateSuccessfulLenders
    } = await processLendersOptimized(savedLead, distributionRules);

    // Schedule RCS if any lenders were successful
    if (immediateSuccessfulLenders.length > 0) {
      setTimeout(async () => {
        await scheduleRCSAfterAllLenders(savedLead.leadId);
      }, 5000);
    }

    res.status(201).json({
      status: 'success',
      data: {
        lead: savedLead,
        sentImmediately,
        pendingLenders: pendingLenders.length,
        immediateSuccessful: immediateSuccessfulLenders.length
      },
    });

  } catch (error) {
    console.error('Error in createLead:', error);

    if (error.code === 'DUPLICATE_PHONE') {
      return res.status(409).json({ message: 'Phone number already exists' });
    }

    if (error.code === 'DUPLICATE_PAN') {
      return res.status(409).json({ message: 'Duplicate PAN number' });
    }

    if (error.errors) {
      return res.status(400).json({ message: 'Validation failed', errors: error.errors });
    }

    res.status(500).json({ message: 'Server error', error: error.message });
  }
};

/**
 * Process lenders with optimized approach:
 * - Send immediately if within time range
 * - Collect all out-of-time lenders in ONE pending entry
 */
async function processLendersOptimized(lead, distributionRules) {
  const lendersToSendNow = [];
  const lendersOutOfTime = [];
  const immediateSuccessfulLenders = [];

  // Process immediate lenders
  for (const lenderConfig of distributionRules.immediate) {
    const lenderName = typeof lenderConfig === 'string' ? lenderConfig : lenderConfig.lender;
    const timeRange = typeof lenderConfig === 'object' ? lenderConfig.timeRange : null;

    // Skip if sending to self
    if (lenderName === lead.source) continue;

    // Check time range
    const canSendNow = timeUtils.isWithinTimeRange(timeRange);

    if (canSendNow) {
      lendersToSendNow.push({ lenderName, lenderConfig });
    } else {
      lendersOutOfTime.push(lenderName);
    }
  }

  // Process delayed lenders (will be handled by delayed mechanism)
  scheduleDelayedLenders(lead, distributionRules.delayed);

  // Send to lenders that are within time range NOW
  for (const { lenderName } of lendersToSendNow) {
    try {
      const result = await sendToLender(lead, lenderName);
      
      if (isLenderSuccess(result, lenderName)) {
        immediateSuccessfulLenders.push(lenderName);
        console.log(`✓ Lead ${lead.leadId} sent to ${lenderName} immediately`);
      }
    } catch (error) {
      console.error(`✗ Error sending lead to ${lenderName}:`, error.message);
    }
  }

  // Update lead with successful lenders
  if (immediateSuccessfulLenders.length > 0) {
    try {
      await Lead.updateByIdNoValidation(lead.leadId, { 
        immediateSuccessfulLenders 
      });
    } catch (error) {
      console.error('Error updating lead with successful lenders:', error.message);
    }
  }

  // Create ONE pending entry with ALL out-of-time lenders
  if (lendersOutOfTime.length > 0) {
    await PendingLead.createOrUpdate(
      lead.leadId,
      lendersOutOfTime,
      lead,
      'immediate'
    );
    console.log(`Queued lead ${lead.leadId} for ${lendersOutOfTime.length} lenders: ${lendersOutOfTime.join(', ')}`);
  }

  return {
    sentImmediately: lendersToSendNow.length,
    pendingLenders: lendersOutOfTime,
    immediateSuccessfulLenders
  };
}

/**
 * Schedule delayed lenders
 */
function scheduleDelayedLenders(lead, delayedLenders) {
  for (const lenderConfig of delayedLenders) {
    if (lenderConfig.lender === lead.source) continue;

    const delayMs = lenderConfig.delayMinutes * 60 * 1000;

    setTimeout(async () => {
      try {
        const canSendNow = timeUtils.isWithinTimeRange(lenderConfig.timeRange);

        if (canSendNow) {
          const result = await sendToLender(lead, lenderConfig.lender);
          
          if (isLenderSuccess(result, lenderConfig.lender)) {
            console.log(`✓ Delayed lender ${lenderConfig.lender} succeeded for lead ${lead.leadId}`);
          }
        } else {
          // Add to pending entry
          await PendingLead.createOrUpdate(
            lead.leadId,
            [lenderConfig.lender],
            lead,
            'delayed'
          );
          console.log(`Queued delayed lender ${lenderConfig.lender} for lead ${lead.leadId}`);
        }
      } catch (error) {
        console.error(`Error with delayed lender ${lenderConfig.lender}:`, error.message);
      }
    }, delayMs);

    console.log(`Scheduled lead ${lead.leadId} to ${lenderConfig.lender} after ${lenderConfig.delayMinutes} minutes`);
  }
}

/**
 * Send to specific lender
 */
async function sendToLender(lead, lender) {
  const lenderHandlers = {
    'SML': sendToSML,
    'FREO': sendToFreo,
    'OVLY': sendToOVLY,
    'LendingPlate': sendToLendingPlate,
    'ZYPE': sendToZYPE,
    'FINTIFI': sendToFINTIFI,
    'FATAKPAY': sendToFATAKPAY,
    'RAMFINCROP': sendToRAMFINCROP,
    'MyMoneyMantra': sendToMyMoneyMantra,
    'INDIALENDS': sendToIndiaLends,
    'MPOKKET': sendToMpokket,
    'CRMPaisa': sendToCrmPaisa,
  };

  if (lenderHandlers[lender]) {
    return await lenderHandlers[lender](lead);
  } else {
    throw new Error(`No handler found for lender: ${lender}`);
  }
}

/**
 * Get distribution rules
 */
async function getDistributionRules(source) {
  try {
    const dbRules = await DistributionRule.findActiveBySource(source);
    
    if (dbRules && dbRules.rules) {
      return dbRules.rules;
    }
    
    // Default rules with time ranges
    const defaultRules = {
      FREO: {
        immediate: [
          { lender: 'ZYPE', timeRange: { start: '09:00', end: '17:00', timezone: 'Asia/Kolkata' } },
          { lender: 'OVLY', timeRange: { start: '09:00', end: '17:00', timezone: 'Asia/Kolkata' } },
          { lender: 'LendingPlate', timeRange: { start: '10:00', end: '18:00', timezone: 'Asia/Kolkata' } },
          { lender: 'FATAKPAY', timeRange: { start: '09:00', end: '19:00', timezone: 'Asia/Kolkata' } },
          { lender: 'INDIALENDS', timeRange: { start: '09:00', end: '17:00', timezone: 'Asia/Kolkata' } }
        ],
        delayed: []
      },
      default: {
        immediate: [
          { lender: 'OVLY', timeRange: { start: '09:00', end: '17:00', timezone: 'Asia/Kolkata' } },
          { lender: 'FATAKPAY', timeRange: { start: '09:00', end: '19:00', timezone: 'Asia/Kolkata' } },
          { lender: 'INDIALENDS', timeRange: { start: '09:00', end: '17:00', timezone: 'Asia/Kolkata' } }
        ],
        delayed: []
      }
    };
    
    return defaultRules[source] || defaultRules.default;
  } catch (error) {
    console.error('Error fetching distribution rules:', error);
    return { immediate: [], delayed: [] };
  }
}

/**
 * Check if lender response indicates success
 */
function isLenderSuccess(result, lenderName) {
  if (!result) return false;
  
  const successCriteria = {
    'SML': (result) => result.responseBody?.message === 'Lead created successfully',
    'FREO': (result) => result.responseBody?.success === true,
    'OVLY': (result) => result.responseStatus === 'success' && result.responseBody?.isDuplicateLead === "false",
    'LendingPlate': (result) => result.responseStatus === 'Success',
    'ZYPE': (result) => result.responseStatus === 'ACCEPT' || result.responseBody?.status === 'ACCEPT',
    'FINTIFI': (result) => result.responseStatus === 200,
    'FATAKPAY': (result) => result.responseBody?.message === 'You are eligible.',
    'RAMFINCROP': (result) => result.responseStatus === 'success',
    'MPOKKET': (result) => result.responseStatus === 200,
    'CRMPaisa': (result) => result.responseStatus === 1,
    'INDIALENDS': (result) => result.responseStatus === 200 || result.responseStatus === 201,
  };

  const checkSuccess = successCriteria[lenderName];
  return checkSuccess ? checkSuccess(result) : false;
}

/**
 * Schedule RCS after all lenders processed
 */
async function scheduleRCSAfterAllLenders(leadId) {
  try {
    const lead = await Lead.findById(leadId);
    if (!lead) return;

    const allSuccessfulLenders = await getAllSuccessfulLendersForLead(leadId, lead);
    await rcsService.scheduleRCSForLead(leadId, allSuccessfulLenders);
    
    console.log(`RCS scheduled for lead ${leadId} with ${allSuccessfulLenders.length} successful lenders`);
  } catch (error) {
    console.error('Error scheduling RCS:', error);
  }
}

/**
 * Get all successful lenders from logs
 */
async function getAllSuccessfulLendersForLead(leadId, lead) {
  const successfulLenders = [];
  
  try {
    // Check all lender logs
    const checks = [
      { log: SMLResponseLog, check: (l) => l.responseBody?.message === 'Lead created successfully', name: 'SML' },
      { log: FreoResponseLog, check: (l) => l.responseBody?.success === true, name: 'FREO' },
      { log: OvlyResponseLog, check: (l) => l.responseStatus === 'success', name: 'OVLY' },
      { log: LendingPlateResponseLog, check: (l) => l.responseStatus === 'Success', name: 'LendingPlate' },
      { log: ZypeResponseLog, check: (l) => l.responseStatus === 'ACCEPT' || l.responseBody?.status === 'ACCEPT', name: 'ZYPE' },
      { log: FintifiResponseLog, check: (l) => l.responseStatus === 200, name: 'FINTIFI' },
      { log: FatakPayResponseLog, check: (l) => l.responseBody?.message === 'You are eligible.', name: 'FATAKPAY' },
      { log: RamFinCropLog, check: (l) => l.responseStatus === 'success', name: 'RAMFINCROP' },
      { log: MMMResponseLog, check: (l) => l.responseStatus === 200 || l.responseStatus === 201, name: 'MyMoneyMantra' },
      { log: IndiaLendsResponseLog, check: (l) => l.responseStatus === 200 || l.responseStatus === 201, name: 'INDIALENDS' },
      { log: MpokketResponseLog, check: (l) => l.responseStatus === 200, name: 'MPOKKET' },
      { log: CrmPaisaResponseLog, check: (l) => l.responseBody?.Message === 'Lead generated successfully.', name: 'CRMPaisa' }
    ];

    for (const { log, check, name } of checks) {
      try {
        const results = await log.findByLeadId(leadId);
        if (results && results.find(check)) {
          successfulLenders.push(name);
        }
      } catch (error) {
        console.error(`Error checking ${name} log:`, error.message);
      }
    }

    // Create/update lead success record
    if (lead && successfulLenders.length > 0) {
      const lenderFlags = {};
      successfulLenders.forEach(lender => {
        lenderFlags[lender] = true;
      });

      const { record, created } = await LeadSuccess.findOrCreate({
        leadId,
        source: lead.source,
        phone: lead.phone,
        email: lead.email,
        panNumber: lead.panNumber,
        fullName: lead.fullName,
        ...lenderFlags
      });

      if (!created) {
        await LeadSuccess.updateByLeadId(leadId, lenderFlags);
      }
    }

  } catch (error) {
    console.error('Error getting successful lenders:', error);
  }

  return successfulLenders;
}
//////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////

exports.createUATLead = async (req, res) => {
  const { 
    source, fullName, firstName, lastName, phone, email, 
    age, dateOfBirth, gender, panNumber, jobType, businessType, 
    salary, creditScore, address, pincode, consent 
  } = req.body;

  // Input validation
  if (!source || !fullName || !phone || !email || !panNumber || consent === undefined) {
    return res.status(400).json({ 
      message: 'Source, fullName, phone, email, panNumber, and consent are required.' 
    });
  }

  // Full Name validation
  if (fullName.length < 2 || fullName.length > 100) {
    return res.status(400).json({ 
      message: 'Full name must be between 2 and 100 characters.' 
    });
  }

  // Email validation
  const emailRegex = /[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,4}$/;
  if (!emailRegex.test(email)) {
    return res.status(400).json({ message: 'Invalid email format.' });
  }

  // PAN number validation
  const panRegex = /^[A-Z]{5}[0-9]{4}[A-Z]$/;
  if (!panRegex.test(panNumber)) {
    return res.status(400).json({ 
      message: 'Invalid PAN number format. Must match ABCDE1234F.' 
    });
  }

  // Date of Birth validation
  const isValidDate = /^\d{4}-\d{2}-\d{2}$/.test(dateOfBirth) && !isNaN(Date.parse(dateOfBirth));
  if (dateOfBirth && (!isValidDate || new Date(dateOfBirth) > new Date())) {
    return res.status(400).json({ 
      message: 'Invalid date of birth or date cannot be in the future.' 
    });
  }

  // Conditional defaults for salary and jobType
  const finalSalary = salary || '50000';
  const finalJobType = jobType || 'SALARIED';

  try {
    // Create lead data object
    const leadData = {
      source,
      fullName,
      firstName,
      lastName,
      phone,
      email,
      age,
      dateOfBirth,
      gender,
      panNumber,
      jobType: finalJobType,
      businessType,
      salary: finalSalary,
      creditScore,
      address,
      pincode,
      consent
    };

    // Create lead using DynamoDB model
    const savedLead = await LeadUAT.create(leadData);

    res.status(201).json({
      status: 'success',
      data: {
        lead: savedLead,
      },
    });

  } catch (error) {
    // Handle duplicate PAN error
    if (error.code === 'DUPLICATE_PAN') {
      return res.status(409).json({ 
        message: 'Duplicate PAN number or phone' 
      });
    }

    // Handle validation errors
    if (error.errors) {
      return res.status(400).json({ 
        message: 'Validation failed', 
        errors: error.errors 
      });
    }

    // Generic server error
    res.status(500).json({ 
      message: 'Server error', 
      error: error.message 
    });
  }
};

////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////

// Bulk lead passing function
const sendLeadsToLender = async (lender, leads) => {
  switch (lender) {
    case "SML":
      return Promise.all(leads.map((lead) => sendToSML(lead)));
    case "FREO":
      return Promise.all(leads.map((lead) => sendToFreo(lead)));
    case "ZYPE":
      return Promise.all(leads.map((lead) => sendToZYPE(lead)));
    case "LendingPlate":
      return Promise.all(leads.map((lead) => sendToLendingPlate(lead)));
    case "FINTIFI":
      return Promise.all(leads.map((lead) => sendToFINTIFI(lead)));
    case "FATAKPAY":
      return Promise.all(leads.map((lead) => sendToFATAKPAY(lead)));
    case "OVLY":
      return Promise.all(leads.map((lead) => sendToOVLY(lead)));
    default:
      return { lender, status: "Failed", message: "Lender not configured" };
  }
};

exports.processFile = async (req, res) => {
  try {
    if (!req.file) {
      return res.status(400).json({ message: "No file uploaded." });
    }

    const lenders = req.body.lenders;
    if (!lenders || lenders.length === 0) {
      return res.status(400).json({ message: "Select at least one lender." });
    }

    const filePath = req.file.path;
    const leads = readFile(filePath);
    console.log(`📊 Total leads found: ${leads.length}`);
    console.log(leads[0]);

    // Prepare leads data for DynamoDB
    const leadsData = leads.map((lead) => ({
      source: req.body.source,
      fullName: `${lead.fullName}`,
      phone: `${lead.phone}`,
      email: lead.email,
      dateOfBirth: lead.dateOfBirth,
      gender: lead.gender,
      panNumber: lead.panNumber,
      jobType: lead.jobType,
      salary: `${lead.salary}`,
      address: `${lead.address}`,
      pincode: `${lead.pincode}`,
      consent: true
    }));

    // Save leads to DynamoDB using bulk insert
    const bulkResult = await ExcelLead.createBulk(leadsData);
    
    const savedLeads = bulkResult.successful;
    const failedLeads = bulkResult.failed;

    console.log(`✅ Successfully saved: ${savedLeads.length} leads`);
    if (failedLeads.length > 0) {
      console.log(`❌ Failed to save: ${failedLeads.length} leads`);
    }

    // Send leads to selected lenders individually
    const allResponses = {};
    for (const lender of lenders) {
      console.log(`📤 Sending ${savedLeads.length} leads to ${lender}`);
      try {
        const lenderResponses = await sendLeadsToLender(lender, savedLeads);
        allResponses[lender] = {
          status: "success",
          totalLeads: savedLeads.length,
          responses: lenderResponses
        };
      } catch (error) {
        console.error(`Error sending leads to ${lender}:`, error);
        allResponses[lender] = {
          status: "error",
          message: error.message,
          totalLeads: savedLeads.length
        };
      }
    }

    deleteFile(filePath);

    res.status(200).json({
      message: "Leads processed successfully",
      totalLeads: leads.length,
      successfulLeads: savedLeads.length,
      failedLeads: failedLeads.length,
      savedLeads: savedLeads,
      failedLeadsDetails: failedLeads,
      lenderResponses: allResponses
    });

  } catch (error) {
    console.error("Error processing leads:", error);
    res.status(500).json({ message: "Internal server error", error: error.message });
  }
};

///////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////

// Get lead by ID
exports.getLead = async (req, res) => {
  try {
    const lead = await Lead.findById(req.params.id);
    
    if (!lead) {
      return res.status(404).json({ message: 'Lead not found' });
    }

    res.status(200).json({
      status: 'success',
      data: { lead }
    });
  } catch (error) {
    res.status(500).json({ message: 'Server error', error: error.message });
  }
};

// Get lead by phone
exports.getLeadByPhone = async (req, res) => {
  try {
    const lead = await Lead.findByPhone(req.params.phone);
    
    if (!lead) {
      return res.status(404).json({ message: 'Lead not found' });
    }

    res.status(200).json({
      status: 'success',
      data: { lead }
    });
  } catch (error) {
    res.status(500).json({ message: 'Server error', error: error.message });
  }
};

// Get lead by PAN
exports.getLeadByPan = async (req, res) => {
  try {
    const lead = await Lead.findByPanNumber(req.params.panNumber);
    
    if (!lead) {
      return res.status(404).json({ message: 'Lead not found' });
    }

    res.status(200).json({
      status: 'success',
      data: { lead }
    });
  } catch (error) {
    res.status(500).json({ message: 'Server error', error: error.message });
  }
};

// Get all leads by source
exports.getLeadsBySource = async (req, res) => {
  try {
    const { source } = req.params;
    const { startDate, endDate, limit = 100 } = req.query;

    const leads = await Lead.findBySource(source, {
      startDate,
      endDate,
      limit: parseInt(limit),
      sortAscending: false
    });

    res.status(200).json({
      status: 'success',
      results: leads.length,
      data: { leads }
    });
  } catch (error) {
    res.status(500).json({ message: 'Server error', error: error.message });
  }
};

// Get all leads (paginated)
exports.getAllLeads = async (req, res) => {
  try {
    const { limit = 100, lastEvaluatedKey } = req.query;

    const result = await Lead.findAll({
      limit: parseInt(limit),
      lastEvaluatedKey: lastEvaluatedKey ? JSON.parse(lastEvaluatedKey) : undefined
    });

    res.status(200).json({
      status: 'success',
      results: result.items.length,
      data: { 
        leads: result.items,
        lastEvaluatedKey: result.lastEvaluatedKey 
      }
    });
  } catch (error) {
    res.status(500).json({ message: 'Server error', error: error.message });
  }
};

// Update lead
exports.updateLead = async (req, res) => {
  try {
    const updates = req.body;
    
    // Remove fields that shouldn't be updated
    delete updates.leadId;
    delete updates.createdAt;

    const updatedLead = await Lead.updateById(req.params.id, updates);

    res.status(200).json({
      status: 'success',
      data: { lead: updatedLead }
    });
  } catch (error) {
    if (error.message === 'Lead not found') {
      return res.status(404).json({ message: 'Lead not found' });
    }
    if (error.code === 'DUPLICATE_PHONE') {
      return res.status(409).json({ message: 'Phone number already exists' });
    }
    if (error.code === 'DUPLICATE_PAN') {
      return res.status(409).json({ message: 'PAN number already exists' });
    }
    if (error.errors) {
      return res.status(400).json({ 
        message: 'Validation failed', 
        errors: error.errors 
      });
    }
    res.status(500).json({ message: 'Server error', error: error.message });
  }
};

// Delete lead
exports.deleteLead = async (req, res) => {
  try {
    await Lead.deleteById(req.params.id);

    res.status(204).json({
      status: 'success',
      data: null
    });
  } catch (error) {
    res.status(500).json({ message: 'Server error', error: error.message });
  }
};

// Search leads by multiple filters
exports.searchLeads = async (req, res) => {
  try {
    const filters = {};
    const { gender, jobType, businessType, limit = 100 } = req.query;

    if (gender) filters.gender = gender;
    if (jobType) filters.jobType = jobType;
    if (businessType) filters.businessType = businessType;

    const leads = await Lead.findByFilters(filters, { 
      limit: parseInt(limit) 
    });

    res.status(200).json({
      status: 'success',
      results: leads.length,
      data: { leads }
    });
  } catch (error) {
    res.status(500).json({ message: 'Server error', error: error.message });
  }
};

// Count leads by source
exports.countLeadsBySource = async (req, res) => {
  try {
    const { source } = req.params;
    const count = await Lead.countBySource(source);

    res.status(200).json({
      status: 'success',
      data: { source, count }
    });
  } catch (error) {
    res.status(500).json({ message: 'Server error', error: error.message });
  }
};

///////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////

// const pushLeadsToLender = async (req, res) => {
//   try {
//     const {
//       lender,
//       source,
//       startDate,
//       endDate,
//       limit,
//       skip = 0,
//       excludeAlreadyPushed = false
//     } = req.body;

//     // Validate required fields
//     if (!lender) {
//       return res.status(400).json({
//         success: false,
//         message: 'Lender is required'
//       });
//     }

//     // Build query filters
//     const query = {};

//     // Filter by source
//     if (source) {
//       if (Array.isArray(source)) {
//         query.source = { $in: source };
//       } else {
//         query.source = source;
//       }
//     }

//     // Filter by date range
//     if (startDate || endDate) {
//       query.createdAt = {};
//       if (startDate) {
//         query.createdAt.$gte = new Date(startDate);
//       }
//       if (endDate) {
//         query.createdAt.$lte = new Date(endDate);
//       }
//     }

//     // Optional: Exclude leads already pushed to this lender
//     if (excludeAlreadyPushed) {
//       query[`pushedTo.${lender}`] = { $exists: false };
//     }

//     // Fetch leads from database
//     const leads = await Lead.find(query)
//       .sort({ createdAt: -1 })
//       .skip(skip)
//       .limit(limit || 100);

//     if (leads.length === 0) {
//       return res.status(404).json({
//         success: false,
//         message: 'No leads found matching the criteria'
//       });
//     }

//     // Send leads to lender
//     const result = await sendLeadsToLender(lender, leads);

//     // Update leads with push status (optional)
//     const updatePromises = leads.map(lead => 
//       Lead.findByIdAndUpdate(lead._id, {
//         $set: {
//           [`pushedTo.${lender}`]: {
//             pushedAt: new Date(),
//             status: 'success'
//           }
//         }
//       })
//     );
//     await Promise.all(updatePromises);

//     res.status(200).json({
//       success: true,
//       message: `Successfully pushed ${leads.length} leads to ${lender}`,
//       data: {
//         lender,
//         totalLeadsPushed: leads.length,
//         results: result
//       }
//     });

//   } catch (error) {
//     console.error('Error pushing leads to lender:', error);
//     res.status(500).json({
//       success: false,
//       message: 'Failed to push leads to lender',
//       error: error.message
//     });
//   }
// };

// /**
//  * Controller to get leads with filters (without sending to lender)
//  */
// const getLeadsWithFilters = async (req, res) => {
//   try {
//     const {
//       source,
//       startDate,
//       endDate,
//       page = 1,
//       limit = 50,
//       sortBy = 'createdAt',
//       sortOrder = 'desc'
//     } = req.query;

//     // Build query filters
//     const query = {};

//     if (source) {
//       if (source.includes(',')) {
//         query.source = { $in: source.split(',') };
//       } else {
//         query.source = source;
//       }
//     }

//     if (startDate || endDate) {
//       query.createdAt = {};
//       if (startDate) {
//         query.createdAt.$gte = new Date(startDate);
//       }
//       if (endDate) {
//         query.createdAt.$lte = new Date(endDate);
//       }
//     }

//     const skip = (page - 1) * limit;
//     const sort = { [sortBy]: sortOrder === 'desc' ? -1 : 1 };

//     const [leads, totalCount] = await Promise.all([
//       Lead.find(query)
//         .sort(sort)
//         .skip(skip)
//         .limit(parseInt(limit)),
//       Lead.countDocuments(query)
//     ]);

//     res.status(200).json({
//       success: true,
//       data: {
//         leads,
//         pagination: {
//           total: totalCount,
//           page: parseInt(page),
//           limit: parseInt(limit),
//           totalPages: Math.ceil(totalCount / limit)
//         }
//       }
//     });

//   } catch (error) {
//     console.error('Error fetching leads:', error);
//     res.status(500).json({
//       success: false,
//       message: 'Failed to fetch leads',
//       error: error.message
//     });
//   }
// };

// /**
//  * Bulk send function - sends all leads to the specified lender
//  */
// exports.sendLeadsToLender = async (lender, leads) => {
//   try {
//     let results;
    
//     switch (lender) {
//       case "SML":
//         results = await Promise.allSettled(leads.map((lead) => sendToSML(lead)));
//         break;
//       case "FREO":
//         results = await Promise.allSettled(leads.map((lead) => sendToFreo(lead)));
//         break;
//       case "ZYPE":
//         results = await Promise.allSettled(leads.map((lead) => sendToZYPE(lead)));
//         break;
//       case "LendingPlate":
//         results = await Promise.allSettled(leads.map((lead) => sendToLendingPlate(lead)));
//         break;
//       case "FINTIFI":
//         results = await Promise.allSettled(leads.map((lead) => sendToFINTIFI(lead)));
//         break;
//       case "FATAKPAY":
//         results = await Promise.allSettled(leads.map((lead) => sendToFATAKPAY(lead)));
//         break;
//       case "OVLY":
//         results = await Promise.allSettled(leads.map((lead) => sendToOVLY(lead)));
//         break;
//       case "RAMFINCROP":
//         results = await Promise.allSettled(leads.map((lead) => sendToRAMFINCROP(lead)));
//         break;
//       case "MPOKKET":
//         results = await Promise.allSettled(leads.map((lead) => sendToMpokket(lead)));
//         break;
//       case "INDIALENDS":
//         results = await Promise.allSettled(leads.map((lead) => sendToIndiaLends(lead)));
//         break;
//       case "CRMPaisa":
//         results = await Promise.allSettled(leads.map((lead) => sendToCrmPaisa(lead)));
//         break;
//       default:
//         return { lender, status: "Failed", message: "Lender not configured" };
//     }

//     // Summarize results
//     const successful = results.filter(r => r.status === 'fulfilled').length;
//     const failed = results.filter(r => r.status === 'rejected').length;

//     return {
//       lender,
//       total: leads.length,
//       successful,
//       failed,
//       details: results
//     };

//   } catch (error) {
//     console.error(`Error sending leads to ${lender}:`, error);
//     return {
//       lender,
//       status: "Failed",
//       message: error.message
//     };
//   }
// };

// module.exports = {
//   pushLeadsToLender,
//   getLeadsWithFilters
// };
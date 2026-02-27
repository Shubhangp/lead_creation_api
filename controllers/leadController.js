const Lead = require('../models/leadModel');
// const PendingLead = require('../models/pendingLeadModel');
const DistributionRule = require('../models/distributionRuleModel');
// const timeUtils = require('../utils/timeutils');
const rcsService = require('../services/rcsService');

// Import lender services
const {
  sendToSML,
  sendToFreo,
  sendToZYPE,
  sendToLendingPlate,
  sendToFINTIFI,
  sendToFATAKPAY,
  sendToFATAKPAYPL,
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
const FatakPayResponseLogPL = require('../models/fatakPayPLResponseLog');

// Create a lead
exports.createLead = async (req, res) => {
  const {
    source, fullName, firstName, lastName, phone, email,
    age, dateOfBirth, gender, panNumber, jobType, businessType,
    salary, creditScore, cibilScore, address, pincode, consent
  } = req.body;

  if (!source || !fullName || !phone || !email || !panNumber || consent === undefined) {
    return res.status(400).json({ 
      message: 'Source, fullName, phone, email, panNumber, and consent are required.' 
    });
  }

  // Full name length validation
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

  // PAN validation
  const panRegex = /^[A-Z]{5}[0-9]{4}[A-Z]$/;
  if (!panRegex.test(panNumber)) {
    return res.status(400).json({ 
      message: 'Invalid PAN number format. Must match ABCDE1234F.' 
    });
  }

  // Date of birth validation
  const isValidDate = /^\d{4}-\d{2}-\d{2}$/.test(dateOfBirth) && !isNaN(Date.parse(dateOfBirth));
  if (dateOfBirth && (!isValidDate || new Date(dateOfBirth) > new Date())) {
    return res.status(400).json({ 
      message: 'Invalid date of birth or date cannot be in the future.' 
    });
  }

  // Set defaults
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

    // Create lead using DynamoDB model
    const savedLead = await Lead.create(leadData);

    // Get distribution rules (your existing function)
    const distributionRules = await getDistributionRules(source);

    // Process immediate lenders (your existing function)
    const immediateSuccessfulLenders = await processLenders(
      savedLead, 
      distributionRules.immediate, 
      'immediate'
    );

    if (immediateSuccessfulLenders.length > 0) {
      setTimeout(async () => {
        await scheduleRCSAfterAllLenders(savedLead.leadId);
      }, 5000);
    }

    // Schedule delayed lenders (your existing function)
    scheduleDelayedLenders(savedLead, distributionRules.delayed);

    res.status(201).json({
      status: 'success',
      data: {
        lead: savedLead,
      },
    });

  } catch (error) {
    console.log(error);

    // Handle duplicate phone error
    if (error.code === 'DUPLICATE_PHONE') {
      return res.status(409).json({ 
        message: 'Phone number already exists' 
      });
    }

    // Handle duplicate PAN error
    if (error.code === 'DUPLICATE_PAN') {
      return res.status(409).json({ 
        message: 'Duplicate PAN number' 
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

// Process lenders based on group (immediate or delayed)
async function processLenders(lead, lenders, type) {
  const successfulLenders = [];
  
  for (const lender of lenders) {
    const lenderName = typeof lender === 'string' ? lender : lender.lender;

    // Don't send to the same lender that created the lead
    if (lenderName !== lead.source) {
      try {
        const result = await sendToLender(lead, lenderName);
        
        // Check if lender API was successful
        if (isLenderSuccess(result, lenderName)) {
          successfulLenders.push(lenderName);
        }
        
        // Log successful distribution
        console.log(`Lead ${lead.leadId} sent to ${lenderName} (${type})`);
      } catch (error) {
        console.error(`Error sending lead to ${lenderName}:`, error.message);
      }
    }
  }

  // Store successful lenders for RCS scheduling
  if (type === 'immediate' && successfulLenders.length > 0) {

    // Update lead with immediate successful lenders
    if (Lead && lead.leadId) {
      try {
        await Lead.updateByIdNoValidation(lead.leadId, { 
          immediateSuccessfulLenders: successfulLenders 
        });
      } catch (error) {
        console.error(`Error updating lead with successful lenders:`, error.message);
      }
    }
  }
  
  return successfulLenders;
}

// Schedule delayed lenders using a job queue
function scheduleDelayedLenders(lead, delayedLenders) {
  let completedLendersCount = 0;
  const totalDelayedLenders = delayedLenders.filter(lender => lender.lender !== lead.source).length;
  
  for (const lenderConfig of delayedLenders) {
    if (lenderConfig.lender !== lead.source) {
      const delayMs = lenderConfig.delayMinutes * 60 * 1000;

      setTimeout(async () => {
        try {
          const result = await sendToLender(lead, lenderConfig.lender);
            console.log("scheduleDelayedLenders: 341 line", result, lenderConfig.lender);
          
          if (isLenderSuccess(result, lenderConfig.lender)) {
            console.log(`Delayed lender ${lenderConfig.lender} succeeded for lead ${lead.leadId}`);
            console.log("scheduleDelayedLenders: 344 line", result, lenderConfig.lender);
          }
          
          console.log(`Delayed lead ${lead.leadId} sent to ${lenderConfig.lender} after ${lenderConfig.delayMinutes} minutes`);
        } catch (error) {
          console.error(`Error sending delayed lead to ${lenderConfig.lender}:`, error.message);
        } finally {
          // Track completion
          completedLendersCount++;
          
          // When all delayed lenders are processed, schedule RCS
          if (completedLendersCount === totalDelayedLenders) {
            // Use the DynamoDB primary key (leadId) instead of Mongo-style _id
            await scheduleRCSAfterAllLenders(lead.leadId);
          }
        }
      }, delayMs);

      console.log(`Scheduled lead ${lead.leadId} to be sent to ${lenderConfig.lender} after ${lenderConfig.delayMinutes} minutes`);
    }
  }

  // If no delayed lenders, schedule RCS immediately after processing immediate lenders
  if (totalDelayedLenders === 0) {
    setTimeout(() => scheduleRCSAfterAllLenders(lead.leadId), 5000); // 5 second delay
  }
}

// Send lead to specific lender
async function sendToLender(lead, lender) {

  // Create handler map for all lenders
  const lenderHandlers = {
    'SML': sendToSML,
    'FREO': sendToFreo,
    'OVLY': sendToOVLY,
    'LendingPlate': sendToLendingPlate,
    'ZYPE': sendToZYPE,
    'FINTIFI': sendToFINTIFI,
    'FATAKPAY': sendToFATAKPAY,
    'FATAKPAYPL': sendToFATAKPAYPL,
    'RAMFINCROP': sendToRAMFINCROP,
    "MyMoneyMantra": sendToMyMoneyMantra,
    "INDIALENDS": sendToIndiaLends,
    "MPOKKET": sendToMpokket,
    "CRMPaisa": sendToCrmPaisa,
  };

  // Call the appropriate handler for the lender
  if (lenderHandlers[lender]) {
    return await lenderHandlers[lender](lead);
  } else {
    throw new Error(`No handler found for lender: ${lender}`);
  }
}

async function getDistributionRules(source) {
  try {
    const dbRules = await DistributionRule.findActiveBySource(source);
    console.log("Get DBrules", dbRules);
    
    if (dbRules) {
      return dbRules.rules;
    }
    
    const defaultRules = {
      FREO: {
        immediate: ['ZYPE', 'OVLY', 'LendingPlate', 'FATAKPAY', 'INDIALENDS'],
        delayed: [
          { lender: 'SML', delayMinutes: 1440 }
        ]
      },
      MyMoneyMantra: {
        immediate: ['ZYPE', 'OVLY', 'LendingPlate', 'FATAKPAY', 'INDIALENDS'],
        delayed: [
          { lender: 'SML', delayMinutes: 1440 }
        ]
      },
      SML: {
        immediate: ['FREO', 'OVLY', 'INDIALENDS'],
        delayed: [
          { lender: 'LendingPlate', delayMinutes: 1 },
          { lender: 'ZYPE', delayMinutes: 1 },
          { lender: 'FINTIFI', delayMinutes: 1 },
          { lender: 'RAMFINCROP', delayMinutes: 1 },
          { lender: 'FATAKPAY', delayMinutes: 1 }
        ]
      },
      OVLY: {
        immediate: ['FREO', 'SML', 'INDIALENDS'],
        delayed: [
          { lender: 'LendingPlate', delayMinutes: 1 },
          { lender: 'ZYPE', delayMinutes: 1 },
          { lender: 'FINTIFI', delayMinutes: 1 },
          { lender: 'RAMFINCROP', delayMinutes: 1 },
          { lender: 'FATAKPAY', delayMinutes: 1 }
        ]
      },
      default: {
        immediate: ['FREO', 'SML', 'OVLY', 'INDIALENDS'],
        delayed: [
          { lender: 'LendingPlate', delayMinutes: 1 },
          { lender: 'ZYPE', delayMinutes: 1 },
          { lender: 'FINTIFI', delayMinutes: 1 },
          { lender: 'RAMFINCROP', delayMinutes: 1 },
          { lender: 'FATAKPAY', delayMinutes: 1 }
        ]
      }
    };
    
    return defaultRules[source] || defaultRules.default;
  } catch (error) {
    console.error('Error fetching distribution rules:', error);
    return {
      immediate: ['ZYPE', 'OVLY', 'LendingPlate', 'FATAKPAY', 'RAMFINCROP', 'INDIALENDS'],
      delayed: [
        { lender: 'SML', delayMinutes: 1440 },
        { lender: 'FINTIFI', delayMinutes: 1440 }
      ]
    };
  }
}

// If lender response indicates success
function isLenderSuccess(result, lenderName) {
  if (!result) return false;
  
  const successCriteria = {
    'SML': (result) => result.responseBody?.message === 'Lead created successfully',
    'FREO': (result) => result.responseBody?.success === true,
    'OVLY': (result) => result.responseStatus === 'success' && result.responseBody?.isDuplicateLead === "false",
    'LendingPlate': (result) => result.responseStatus === 'Success',
    'ZYPE': (result) => result.responseStatus === 'ACCEPT' || result.responseBody?.status === 'ACCEPT',
    'FINTIFI': (result) => result.responseStatus === 200,
    'FATAKPAY': (result) => result.responseBody.message === 'You are eligible.',
    'RAMFINCROP': (result) => result.responseStatus === 'success',
    "MPOKKET": (result) => result.responseStatus === 200,
    'CRMPaisa': (result) => result.responseStatus === 1,
  };
 
  const checkSuccess = successCriteria[lenderName];
  return checkSuccess ? checkSuccess(result) : false;
}

// Schedule RCS after all lenders have been processed
async function scheduleRCSAfterAllLenders(leadId) {
  console.log("LC scheduleRCSAfterAllLenders : 486 line", leadId);
  try {
    const lead = await Lead.findById(leadId);
    if (!lead) return;

    // Get all successful lenders from database logs
    const allSuccessfulLenders = await getAllSuccessfulLendersForLead(leadId, lead);
    console.log("LC scheduleRCSAfterAllLenders : 493 line", allSuccessfulLenders);
    
    // Schedule RCS based on results
    await rcsService.scheduleRCSForLead(leadId, allSuccessfulLenders);
    
    console.log(`RCS scheduled for lead ${leadId} with ${allSuccessfulLenders.length} successful lenders`);
  } catch (error) {
    console.error('Error scheduling RCS after all lenders:', error);
  }
}

// Helper function to get successful lenders from all log collections
async function getAllSuccessfulLendersForLead(leadId, lead) {
  const successfulLenders = [];
  console.log("LC getAllSuccessfulLendersForLead : 507 line", leadId, lead, successfulLenders);
  
  try {
    // Check SML
    const smlResults = await SMLResponseLog.findByLeadId(leadId);
    console.log('sml', smlResults);
    const smlResult = smlResults.find(log => 
      log.responseBody?.message === 'Lead created successfully'
    );
    if (smlResult) successfulLenders.push('SML');

    // Check FREO  
    const freoResults = await FreoResponseLog.findByLeadId(leadId);
    console.log('freo', freoResults);
    const freoResult = freoResults.find(log => 
      log.responseBody?.success === true
    );
    if (freoResult) successfulLenders.push('FREO');

    // Check OVLY
    const ovlyResults = await OvlyResponseLog.findByLeadId(leadId);
    console.log('ovly', ovlyResults);
    const ovlyResult = ovlyResults.items?.find(log => 
      log.responseStatus === 'success'
    );
    if (ovlyResult) successfulLenders.push('OVLY');

    // Check LendingPlate
    const lpResults = await LendingPlateResponseLog.findByLeadId(leadId);
    console.log('lp', lpResults);
    const lpResult = lpResults.find(log => 
      log.responseStatus === 'Success'
    );
    if (lpResult) successfulLenders.push('LendingPlate');

    // Check ZYPE
    const zypeResults = await ZypeResponseLog.findByLeadId(leadId);
    console.log('zype', zypeResults);
    const zypeResult = zypeResults.items?.find(log => 
      log.responseStatus === 'ACCEPT' || log.responseBody?.status === 'ACCEPT'
    );
    if (zypeResult) successfulLenders.push('ZYPE');

    // Check FINTIFI
    const fintifiResults = await FintifiResponseLog.findByLeadId(leadId);
    console.log('fintifi', fintifiResults);
    const fintifiResult = fintifiResults.items.find(log => 
      log.responseStatus === 200
    );
    if (fintifiResult) successfulLenders.push('FINTIFI');

    // Check FATAKPAY
    const fatakResults = await FatakPayResponseLog.findByLeadId(leadId);
    console.log('fatakpay', fatakResults);
    const fatakResult = fatakResults.items.find(log => 
      log.responseBody.message === 'You are eligible.'
    );
    if (fatakResult) successfulLenders.push('FATAKPAY');

    // Check FATAKPAYPL
    const fatakPLResults = await FatakPayResponseLogPL.findByLeadId(leadId);
    console.log('fatakpl', fatakPLResults);
    const fatakPLResult = fatakPLResults.items.find(log => 
      log.responseBody.message === 'You are eligible.'
    );
    if (fatakPLResult) successfulLenders.push('FATAKPAY');

    // Check RAMFINCROP
    const ramResults = await RamFinCropLog.findByLeadId(leadId);
    console.log('ram', ramResults);
    const ramResult = ramResults.items.find(log => 
      log.responseStatus === 'success'
    );
    if (ramResult) successfulLenders.push('RAMFINCROP');

    // Check MyMoneyMantra
    const mmmResults = await MMMResponseLog.findByLeadId(leadId);
    console.log('mmm', mmmResults);
    const mmmResult = mmmResults.find(log => 
      log.responseStatus === 200 || log.responseStatus === 201
    );
    if (mmmResult) successfulLenders.push('MyMoneyMantra');

    // Check CRMPaisa
    const CRMPaisaResults = await CrmPaisaResponseLog.findByLeadId(leadId);
    console.log('crmpaisa', CRMPaisaResults);
    const CRMPaisaResult = CRMPaisaResults.items.find(log => 
      log.responseBody?.Message === 'Lead generated successfully.'
    );
    if (CRMPaisaResult) successfulLenders.push('CRMPaisa');

    // Check IndiaLends
    const IndiaLendsResults = await IndiaLendsResponseLog.findByLeadId(leadId);
    console.log('IL', IndiaLendsResults);
    const IndiaLendsResult = IndiaLendsResults.items.find(log => 
      log.responseBody?.info?.message === 'Verification code sent to your mobile phone'
    );
    if (IndiaLendsResult) successfulLenders.push('IndiaLends');

    // Check Mpokket
    const MpokketResults = await MpokketResponseLog.findByLeadId(leadId);
    console.log('mpokket', MpokketResults);
    const MpokketResult = MpokketResults.items.find(log => 
      log.responseBody?.data?.message === 'Data Accepted Successfully'
    );
    if (MpokketResult) successfulLenders.push('Mpokket');

    console.log("LC getAllSuccessfulLendersForLead : 573 line", successfulLenders);

    // --- Create entry in leadSuccess ---
    if (lead && successfulLenders.length > 0) {
      // Prepare lender flags
      const lenderFlags = {};
      successfulLenders.forEach(lender => {
        lenderFlags[lender] = true;
      });

      console.log("LC getAllSuccessfulLendersForLead : 583 line", lenderFlags);

      // Find or create lead success record
      const { record, created } = await LeadSuccess.findOrCreate({
        leadId,
        source: lead.source,
        phone: lead.phone,
        email: lead.email,
        panNumber: lead.panNumber,
        fullName: lead.fullName,
        ...lenderFlags
      });

      // If record already exists, update it with new successful lenders
      if (!created) {
        await LeadSuccess.updateByLeadId(leadId, lenderFlags);
      }
    }

  } catch (error) {
    console.error('Error getting successful lenders:', error);
  }

  console.log("RCS lenders list: line 606", successfulLenders);
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
    case "FATAKPAYPL":
      return Promise.all(leads.map((lead) => sendToFATAKPAYPL(lead)));
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
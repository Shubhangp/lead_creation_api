const Lead = require('../models/leadModel');
const LeadDistributionStats = require('../models/leadDistributionStatsModel');
const { Readable } = require('stream');
const { pipeline } = require('stream/promises');

// Import lender-specific sending functions
const {
  sendToSML,
  sendToFreo,
  sendToZYPE,
  sendToLendingPlate,
  sendToFINTIFI,
  sendToFATAKPAY,
  sendToOVLY,
  sendToRAMFINCROP,
  sendToMpokket,
  sendToIndiaLends,
  sendToCrmPaisa
} = require('../services/lenderService');

/**
 * Calculate age from date of birth
 */
const calculateAge = (dob) => {
  if (!dob) return null;
  const today = new Date();
  const birthDate = new Date(dob);
  let age = today.getFullYear() - birthDate.getFullYear();
  const monthDiff = today.getMonth() - birthDate.getMonth();
  if (monthDiff < 0 || (monthDiff === 0 && today.getDate() < birthDate.getDate())) {
    age--;
  }
  return age;
};

/**
 * Filter leads based on criteria
 */
const filterLead = (lead, filters) => {
  // Date range filter
  if (filters.startDate && new Date(lead.createdAt) < new Date(filters.startDate)) {
    return false;
  }
  if (filters.endDate && new Date(lead.createdAt) > new Date(filters.endDate)) {
    return false;
  }

  // Source filter
  if (filters.sources && filters.sources.length > 0) {
    if (!filters.sources.includes(lead.source)) {
      return false;
    }
  }

  // Age filter (calculate from DOB)
  if (filters.minAge || filters.maxAge) {
    const age = lead.age || calculateAge(lead.dateOfBirth);
    if (!age) return false;
    if (filters.minAge && age < filters.minAge) return false;
    if (filters.maxAge && age > filters.maxAge) return false;
  }

  // Salary filter
  if (filters.minSalary && (!lead.salary || lead.salary < filters.minSalary)) {
    return false;
  }
  if (filters.maxSalary && (!lead.salary || lead.salary > filters.maxSalary)) {
    return false;
  }

  // Job type filter
  if (filters.jobTypes && filters.jobTypes.length > 0) {
    if (!lead.jobType || !filters.jobTypes.includes(lead.jobType)) {
      return false;
    }
  }

  // Credit score filter
  if (filters.minCreditScore && (!lead.creditScore || lead.creditScore < filters.minCreditScore)) {
    return false;
  }
  if (filters.maxCreditScore && (!lead.creditScore || lead.creditScore > filters.maxCreditScore)) {
    return false;
  }

  // Gender filter
  if (filters.gender && lead.gender !== filters.gender) {
    return false;
  }

  // Pincode filter
  if (filters.pincodes && filters.pincodes.length > 0) {
    if (!lead.pincode || !filters.pincodes.includes(lead.pincode)) {
      return false;
    }
  }

  return true;
};

/**
 * Get lender sending function
 */
const getLenderSendFunction = (lender) => {
  const lenderMap = {
    'SML': sendToSML,
    'FREO': sendToFreo,
    'ZYPE': sendToZYPE,
    'LendingPlate': sendToLendingPlate,
    'FINTIFI': sendToFINTIFI,
    'FATAKPAY': sendToFATAKPAY,
    'OVLY': sendToOVLY,
    'RAMFINCROP': sendToRAMFINCROP,
    'MPOKKET': sendToMpokket,
    'INDIALENDS': sendToIndiaLends,
    'CRMPaisa': sendToCrmPaisa
  };

  return lenderMap[lender];
};

/**
 * Stream leads and send to lender with progress tracking
 */
const streamLeadsToLender = async (req, res) => {
  const {
    lender,
    filters = {},
    batchSize = 10, // How many leads to process in parallel
    delayMs = 100 // Delay between batches to prevent rate limiting
  } = req.body;

  // Validate lender
  if (!lender) {
    return res.status(400).json({
      success: false,
      message: 'Lender is required'
    });
  }

  const sendFunction = getLenderSendFunction(lender);
  if (!sendFunction) {
    return res.status(400).json({
      success: false,
      message: 'Invalid lender specified'
    });
  }

  try {
    // Create batch statistics record
    const batch = await LeadDistributionStats.createBatch({
      lender,
      filters
    });

    // Set up SSE (Server-Sent Events) for real-time progress
    res.writeHead(200, {
      'Content-Type': 'text/event-stream',
      'Cache-Control': 'no-cache',
      'Connection': 'keep-alive',
      'X-Accel-Buffering': 'no' // Disable nginx buffering
    });

    const sendProgress = (data) => {
      res.write(`data: ${JSON.stringify(data)}\n\n`);
    };

    // Send initial batch info
    sendProgress({
      type: 'batch_started',
      batchId: batch.batchId,
      lender,
      filters
    });

    // Query leads based on source if provided
    let allLeads = [];
    if (filters.sources && filters.sources.length > 0) {
      // Query each source
      for (const source of filters.sources) {
        const sourceLeads = await Lead.findBySource(source, {
          limit: 1000, // Adjust as needed
          startDate: filters.startDate,
          endDate: filters.endDate
        });
        allLeads = allLeads.concat(sourceLeads);
      }
    } else {
      // Scan all leads (use with caution)
      const result = await Lead.findAll({ limit: 1000 });
      allLeads = result.items;
    }

    // Apply additional filters
    const filteredLeads = allLeads.filter(lead => filterLead(lead, filters));

    sendProgress({
      type: 'filtering_complete',
      totalLeads: filteredLeads.length
    });

    // Update batch with total leads
    await LeadDistributionStats.updateBatchStats(batch.batchId, {
      totalLeads: filteredLeads.length
    });

    // Process leads in batches
    let processedCount = 0;
    let successCount = 0;
    let failCount = 0;

    for (let i = 0; i < filteredLeads.length; i += batchSize) {
      const batchLeads = filteredLeads.slice(i, i + batchSize);
      
      // Process batch in parallel
      const results = await Promise.allSettled(
        batchLeads.map(async (lead) => {
          try {
            const result = await sendFunction(lead);
            
            // Update lead with push status
            await Lead.updateByIdNoValidation(lead.leadId, {
              [`pushedTo.${lender}`]: {
                batchId: batch.batchId,
                pushedAt: new Date().toISOString(),
                status: 'success'
              }
            });

            // Update batch stats
            await LeadDistributionStats.addLeadToBatch(batch.batchId, lead.leadId, true);
            
            successCount++;
            processedCount++;

            sendProgress({
              type: 'lead_processed',
              leadId: lead.leadId,
              status: 'success',
              processed: processedCount,
              total: filteredLeads.length,
              successful: successCount,
              failed: failCount
            });

            return { success: true, leadId: lead.leadId };
          } catch (error) {
            // Update lead with error status
            await Lead.updateByIdNoValidation(lead.leadId, {
              [`pushedTo.${lender}`]: {
                batchId: batch.batchId,
                pushedAt: new Date().toISOString(),
                status: 'failed',
                error: error.message
              }
            });

            // Update batch stats
            await LeadDistributionStats.addLeadToBatch(batch.batchId, lead.leadId, false);
            await LeadDistributionStats.addError(batch.batchId, {
              message: `Lead ${lead.leadId}: ${error.message}`
            });

            failCount++;
            processedCount++;

            sendProgress({
              type: 'lead_processed',
              leadId: lead.leadId,
              status: 'failed',
              error: error.message,
              processed: processedCount,
              total: filteredLeads.length,
              successful: successCount,
              failed: failCount
            });

            return { success: false, leadId: lead.leadId, error: error.message };
          }
        })
      );

      // Small delay between batches
      if (i + batchSize < filteredLeads.length && delayMs > 0) {
        await new Promise(resolve => setTimeout(resolve, delayMs));
      }
    }

    // Update final batch status
    await LeadDistributionStats.updateBatchStats(batch.batchId, {
      status: failCount === 0 ? 'COMPLETED' : (successCount === 0 ? 'FAILED' : 'PARTIAL'),
      completedAt: new Date().toISOString()
    });

    // Send completion message
    sendProgress({
      type: 'batch_completed',
      batchId: batch.batchId,
      totalLeads: filteredLeads.length,
      successful: successCount,
      failed: failCount,
      status: failCount === 0 ? 'COMPLETED' : (successCount === 0 ? 'FAILED' : 'PARTIAL')
    });

    res.end();

  } catch (error) {
    console.error('Error streaming leads:', error);
    
    res.write(`data: ${JSON.stringify({
      type: 'error',
      message: error.message
    })}\n\n`);
    
    res.end();
  }
};

/**
 * Get batch statistics
 */
const getBatchStats = async (req, res) => {
  try {
    const { batchId } = req.params;

    const batch = await LeadDistributionStats.findById(batchId);

    if (!batch) {
      return res.status(404).json({
        success: false,
        message: 'Batch not found'
      });
    }

    res.status(200).json({
      success: true,
      data: batch
    });

  } catch (error) {
    console.error('Error fetching batch stats:', error);
    res.status(500).json({
      success: false,
      message: 'Failed to fetch batch statistics',
      error: error.message
    });
  }
};

/**
 * Get all batches with pagination
 */
const getAllBatches = async (req, res) => {
  try {
    const { limit = 50, lastEvaluatedKey } = req.query;

    const result = await LeadDistributionStats.findAll({
      limit: parseInt(limit),
      lastEvaluatedKey: lastEvaluatedKey ? JSON.parse(lastEvaluatedKey) : undefined
    });

    res.status(200).json({
      success: true,
      data: result.items,
      lastEvaluatedKey: result.lastEvaluatedKey
    });

  } catch (error) {
    console.error('Error fetching batches:', error);
    res.status(500).json({
      success: false,
      message: 'Failed to fetch batches',
      error: error.message
    });
  }
};

/**
 * Get lender summary statistics
 */
const getLenderStats = async (req, res) => {
  try {
    const { lender } = req.params;
    const { startDate, endDate } = req.query;

    const summary = await LeadDistributionStats.getLenderSummary(
      lender,
      startDate,
      endDate
    );

    res.status(200).json({
      success: true,
      data: summary
    });

  } catch (error) {
    console.error('Error fetching lender stats:', error);
    res.status(500).json({
      success: false,
      message: 'Failed to fetch lender statistics',
      error: error.message
    });
  }
};

/**
 * Get leads preview with filters (without sending)
 */
const getLeadsPreview = async (req, res) => {
  try {
    const { filters = {} } = req.body;

    // Query leads based on source if provided
    let allLeads = [];
    if (filters.sources && filters.sources.length > 0) {
      for (const source of filters.sources) {
        const sourceLeads = await Lead.findBySource(source, {
          limit: 100,
          startDate: filters.startDate,
          endDate: filters.endDate
        });
        allLeads = allLeads.concat(sourceLeads);
      }
    } else {
      const result = await Lead.findAll({ limit: 100 });
      allLeads = result.items;
    }

    // Apply filters
    const filteredLeads = allLeads.filter(lead => filterLead(lead, filters));

    res.status(200).json({
      success: true,
      data: {
        totalMatching: filteredLeads.length,
        leads: filteredLeads.slice(0, 10), // Return first 10 as preview
        filters: filters
      }
    });

  } catch (error) {
    console.error('Error getting leads preview:', error);
    res.status(500).json({
      success: false,
      message: 'Failed to get leads preview',
      error: error.message
    });
  }
};

module.exports = {
  streamLeadsToLender,
  getBatchStats,
  getAllBatches,
  getLenderStats,
  getLeadsPreview
};
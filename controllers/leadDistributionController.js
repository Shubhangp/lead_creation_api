const Lead = require('../models/leadModel');
const LeadDistributionStats = require('../models/leadDistributionStatsModel');

// Import your lender-specific sending functions
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
 * Background job processing function
 */
const processLeadsInBackground = async (batchId, lender, filters, batchSize, delayMs, progressCallback) => {
  const sendFunction = getLenderSendFunction(lender);
  
  try {
    // Query ALL leads with pagination
    let allLeads = [];
    
    if (filters.sources && filters.sources.length > 0) {
      // Query each source with pagination
      for (const source of filters.sources) {
        let lastEvaluatedKey = null;
        do {
          const result = await Lead.findBySource(source, {
            limit: 1000,
            startDate: filters.startDate,
            endDate: filters.endDate,
            lastEvaluatedKey
          });
          
          // Handle both array and object response
          const leads = Array.isArray(result) ? result : result.items || [];
          allLeads = allLeads.concat(leads);
          
          lastEvaluatedKey = result.lastEvaluatedKey;
        } while (lastEvaluatedKey);
      }
    } else {
      // Scan ALL leads with pagination
      let lastEvaluatedKey = null;
      do {
        const result = await Lead.findAll({ 
          limit: 1000,
          lastEvaluatedKey 
        });
        allLeads = allLeads.concat(result.items);
        lastEvaluatedKey = result.lastEvaluatedKey;
      } while (lastEvaluatedKey);
    }

    // Apply additional filters
    const filteredLeads = allLeads.filter(lead => filterLead(lead, filters));

    console.log(`[Batch ${batchId}] Found ${filteredLeads.length} leads matching filters`);

    // Update batch with total leads
    await LeadDistributionStats.updateBatchStats(batchId, {
      totalLeads: filteredLeads.length
    });

    if (progressCallback) {
      progressCallback({
        type: 'filtering_complete',
        totalLeads: filteredLeads.length
      });
    }

    // Process leads in batches
    let processedCount = 0;
    let successCount = 0;
    let failCount = 0;

    for (let i = 0; i < filteredLeads.length; i += batchSize) {
      const batchLeads = filteredLeads.slice(i, i + batchSize);
      
      // Process batch in parallel
      await Promise.allSettled(
        batchLeads.map(async (lead) => {
          try {
            await sendFunction(lead);
            
            // Update lead with push status
            await Lead.updateByIdNoValidation(lead.leadId, {
              [`pushedTo.${lender}`]: {
                batchId: batchId,
                pushedAt: new Date().toISOString(),
                status: 'success'
              }
            });

            // Update batch stats
            await LeadDistributionStats.addLeadToBatch(batchId, lead.leadId, true);
            
            successCount++;
            processedCount++;

            if (progressCallback) {
              progressCallback({
                type: 'lead_processed',
                leadId: lead.leadId,
                status: 'success',
                processed: processedCount,
                total: filteredLeads.length,
                successful: successCount,
                failed: failCount
              });
            }

            return { success: true, leadId: lead.leadId };
          } catch (error) {
            // Update lead with error status
            await Lead.updateByIdNoValidation(lead.leadId, {
              [`pushedTo.${lender}`]: {
                batchId: batchId,
                pushedAt: new Date().toISOString(),
                status: 'failed',
                error: error.message
              }
            });

            // Update batch stats
            await LeadDistributionStats.addLeadToBatch(batchId, lead.leadId, false);
            await LeadDistributionStats.addError(batchId, {
              message: `Lead ${lead.leadId}: ${error.message}`
            });

            failCount++;
            processedCount++;

            if (progressCallback) {
              progressCallback({
                type: 'lead_processed',
                leadId: lead.leadId,
                status: 'failed',
                error: error.message,
                processed: processedCount,
                total: filteredLeads.length,
                successful: successCount,
                failed: failCount
              });
            }

            return { success: false, leadId: lead.leadId, error: error.message };
          }
        })
      );

      // Log progress
      console.log(`[Batch ${batchId}] Progress: ${processedCount}/${filteredLeads.length} (Success: ${successCount}, Failed: ${failCount})`);

      // Small delay between batches
      if (i + batchSize < filteredLeads.length && delayMs > 0) {
        await new Promise(resolve => setTimeout(resolve, delayMs));
      }
    }

    // Update final batch status
    await LeadDistributionStats.updateBatchStats(batchId, {
      status: failCount === 0 ? 'COMPLETED' : (successCount === 0 ? 'FAILED' : 'PARTIAL'),
      completedAt: new Date().toISOString()
    });

    console.log(`[Batch ${batchId}] Completed: ${successCount} successful, ${failCount} failed`);

    if (progressCallback) {
      progressCallback({
        type: 'batch_completed',
        batchId: batchId,
        totalLeads: filteredLeads.length,
        successful: successCount,
        failed: failCount,
        status: failCount === 0 ? 'COMPLETED' : (successCount === 0 ? 'FAILED' : 'PARTIAL')
      });
    }

  } catch (error) {
    console.error(`[Batch ${batchId}] Error:`, error);
    
    await LeadDistributionStats.updateBatchStats(batchId, {
      status: 'FAILED',
      completedAt: new Date().toISOString()
    });

    await LeadDistributionStats.addError(batchId, {
      message: `Fatal error: ${error.message}`
    });

    if (progressCallback) {
      progressCallback({
        type: 'error',
        message: error.message
      });
    }

    throw error;
  }
};

/**
 * Start background job (Fire and forget)
 */
const startBackgroundDistribution = async (req, res) => {
  const {
    lender,
    filters = {},
    batchSize = 10,
    delayMs = 100
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

    // Start processing in background (don't await)
    processLeadsInBackground(
      batch.batchId, 
      lender, 
      filters, 
      batchSize, 
      delayMs,
      null // No progress callback
    ).catch(error => {
      console.error(`Background job ${batch.batchId} failed:`, error);
    });

    // Immediately return batch ID
    res.status(200).json({
      success: true,
      message: 'Distribution started in background',
      data: {
        batchId: batch.batchId,
        lender,
        filters
      }
    });

  } catch (error) {
    console.error('Error starting background distribution:', error);
    res.status(500).json({
      success: false,
      message: 'Failed to start distribution',
      error: error.message
    });
  }
};

/**
 * Stream leads with SSE (with progress tracking)
 */
const streamLeadsToLender = async (req, res) => {
  const {
    lender,
    filters = {},
    batchSize = 10,
    delayMs = 100
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

    // Set up SSE
    res.writeHead(200, {
      'Content-Type': 'text/event-stream',
      'Cache-Control': 'no-cache',
      'Connection': 'keep-alive',
      'X-Accel-Buffering': 'no'
    });

    let clientConnected = true;
    req.on('close', () => {
      clientConnected = false;
      console.log(`[Batch ${batch.batchId}] Client disconnected, but job continues in background`);
    });

    const sendProgress = (data) => {
      if (clientConnected) {
        try {
          res.write(`data: ${JSON.stringify(data)}\n\n`);
        } catch (error) {
          clientConnected = false;
        }
      }
    };

    // Send initial batch info
    sendProgress({
      type: 'batch_started',
      batchId: batch.batchId,
      lender,
      filters
    });

    // Start processing (continues even if client disconnects)
    await processLeadsInBackground(
      batch.batchId,
      lender,
      filters,
      batchSize,
      delayMs,
      sendProgress
    );

    if (clientConnected) {
      res.end();
    }

  } catch (error) {
    console.error('Error in streaming distribution:', error);
    
    if (!res.headersSent) {
      res.status(500).json({
        success: false,
        message: error.message
      });
    }
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

    // Query leads with pagination to get accurate count
    let totalCount = 0;
    let sampleLeads = [];
    
    if (filters.sources && filters.sources.length > 0) {
      for (const source of filters.sources) {
        let lastEvaluatedKey = null;
        let sourceCount = 0;
        
        do {
          const result = await Lead.findBySource(source, {
            limit: 1000,
            startDate: filters.startDate,
            endDate: filters.endDate,
            lastEvaluatedKey
          });
          
          const leads = Array.isArray(result) ? result : result.items || [];
          const filtered = leads.filter(lead => filterLead(lead, filters));
          
          sourceCount += filtered.length;
          
          // Collect sample leads (first 10)
          if (sampleLeads.length < 10) {
            sampleLeads = sampleLeads.concat(filtered.slice(0, 10 - sampleLeads.length));
          }
          
          lastEvaluatedKey = result.lastEvaluatedKey;
        } while (lastEvaluatedKey);
        
        totalCount += sourceCount;
      }
    } else {
      let lastEvaluatedKey = null;
      
      do {
        const result = await Lead.findAll({ 
          limit: 1000,
          lastEvaluatedKey 
        });
        
        const filtered = result.items.filter(lead => filterLead(lead, filters));
        totalCount += filtered.length;
        
        if (sampleLeads.length < 10) {
          sampleLeads = sampleLeads.concat(filtered.slice(0, 10 - sampleLeads.length));
        }
        
        lastEvaluatedKey = result.lastEvaluatedKey;
      } while (lastEvaluatedKey);
    }

    res.status(200).json({
      success: true,
      data: {
        totalMatching: totalCount,
        leads: sampleLeads,
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
  startBackgroundDistribution,
  getBatchStats,
  getAllBatches,
  getLenderStats,
  getLeadsPreview
};
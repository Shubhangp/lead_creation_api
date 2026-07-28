// controllers/leadDistributionController.js
const Lead = require('../models/leadModel');
const LeadDistributionStats = require('../models/leadDistributionStatsModel');
const { getRateLimit } = require('../config/lenderRateLimits');

// Import your lender-specific sending functions
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
  sendToMpokket,
  sendToIndiaLends,
  sendToCrmPaisa,
  sendToCreditPulse,
  sendToCreditSea,
  sendToCreditLinks,
  sendToCreditHaat
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
    'FATAKPAYPL': sendToFATAKPAYPL,
    'OVLY': sendToOVLY,
    'RAMFINCROP': sendToRAMFINCROP,
    'MPOKKET': sendToMpokket,
    'INDIALENDS': sendToIndiaLends,
    'CRMPaisa': sendToCrmPaisa,
    "CreditPluse": sendToCreditPulse,
    "CreditSea": sendToCreditSea,
    "CreditLinks": sendToCreditLinks,
    "CreditHaat": sendToCreditHaat,
  };

  return lenderMap[lender];
};

/**
 * Token-bucket rate limiter (per-minute). Same pattern used in processLeadController.js.
 * refillPerMs = ratePerMinute / 60000 → tokens trickle back continuously.
 */
class TokenBucket {
  constructor(ratePerMinute) {
    this.capacity = Math.max(1, ratePerMinute);
    this.tokens = this.capacity;
    this.refillPerMs = ratePerMinute / 60000;
    this.last = Date.now();
  }

  _refill() {
    const now = Date.now();
    const elapsed = now - this.last;
    if (elapsed > 0) {
      this.tokens = Math.min(this.capacity, this.tokens + elapsed * this.refillPerMs);
      this.last = now;
    }
  }

  async take(n = 1) {
    // If the request is bigger than the whole bucket, cap the wait to one full refill.
    const need = Math.min(n, this.capacity);
    while (true) {
      this._refill();
      if (this.tokens >= need) {
        this.tokens -= need;
        return;
      }
      const deficit = need - this.tokens;
      const waitMs = Math.ceil(deficit / this.refillPerMs);
      await new Promise(resolve => setTimeout(resolve, Math.min(waitMs, 1000)));
    }
  }
}

/**
 * Map a lender send-function result (or thrown error) into one of the
 * dashboard status categories: ACCEPT / REJECTED / Failed / other.
 * Mirrors the categorization used by the stats dashboard (responseBody.status).
 */
const categorizeResult = (result, error) => {
  if (error) return 'Failed';
  // Some lenders early-return undefined (e.g. skipped/ineligible) — count as "other".
  if (result === undefined || result === null) return 'other';

  let body = result.responseBody;
  if (typeof body === 'string') {
    try { body = JSON.parse(body); } catch (_) { /* keep as string */ }
  }
  const bodyStatus = (body && typeof body === 'object') ? body.status : undefined;
  const raw = String(bodyStatus || result.responseStatus || '').trim().toUpperCase();

  if (!raw) return 'other';
  if (raw === 'ACCEPT' || raw.includes('ACCEPT') || raw.includes('APPROV') || raw === 'SUCCESS') return 'ACCEPT';
  if (raw.includes('REJECT') || raw.includes('DECLINE')) return 'REJECTED';
  if (raw.includes('FAIL') || raw.includes('ERROR')) return 'Failed';
  return 'other';
};

/**
 * Background job processing function
 *
 * MEMORY-SAFE / STREAMING: instead of collecting every lead into one giant
 * `allLeads` array (and a second `filteredLeads` copy) — which caused OOM crashes
 * on 2L+ datasets — we now process ONE DynamoDB page at a time: fetch → filter →
 * rate-limited send → discard. Peak memory stays bounded to a single page.
 * A per-lender TokenBucket enforces the lender's per-minute rate limit.
 */
const processLeadsInBackground = async (batchId, lender, filters, batchSize, delayMs, progressCallback) => {
  const sendFunction = getLenderSendFunction(lender);

  // Per-lender rate limiter (per minute). Sending is throttled to the lender's cap.
  const rpm = getRateLimit(lender);
  const limiter = new TokenBucket(rpm);
  console.log(`[Batch ${batchId}] Rate limit for ${lender}: ${rpm} leads/min`);

  // Running totals — shared across all pages (kept as plain scalars, not arrays).
  let processedCount = 0;
  let successCount = 0;   // sent to lender API without throwing
  let failCount = 0;      // threw an error
  let matchedCount = 0;   // leads passing the filter (== totalLeads)
  const categoryTotals = { ACCEPT: 0, REJECTED: 0, Failed: 0, other: 0 };
  let processingRecords = [];
  // Pending deltas flushed to DynamoDB periodically (throttle-safe).
  let pending = { processedLeads: 0, successfulLeads: 0, failedLeads: 0 };
  let pendingCats = { ACCEPT: 0, REJECTED: 0, Failed: 0, other: 0 };

  const flushCounters = async (force = false) => {
    if (force || pending.processedLeads >= 50) {
      const delta = pending;
      pending = { processedLeads: 0, successfulLeads: 0, failedLeads: 0 };
      await LeadDistributionStats.incrementCounters(batchId, {
        processedLeads: delta.processedLeads,
        successfulLeads: delta.successfulLeads,
        failedLeads: delta.failedLeads,
        totalLeads: delta.processedLeads // totalLeads == matched == processed in streaming mode
      });
    }
    if (force || (pendingCats.ACCEPT + pendingCats.REJECTED + pendingCats.Failed + pendingCats.other) >= 50) {
      const cats = pendingCats;
      pendingCats = { ACCEPT: 0, REJECTED: 0, Failed: 0, other: 0 };
      await LeadDistributionStats.incrementStatusCategories(batchId, cats);
    }
  };

  const flushProcessingRecords = async (force = false) => {
    if (processingRecords.length >= 25 || (force && processingRecords.length > 0)) {
      const toWrite = processingRecords;
      processingRecords = [];
      await LeadDistributionStats.recordLeadProcessingBatch(toWrite);
    }
  };

  // Process one filtered page (bounded slice) of leads, sub-batched + rate-limited.
  const processPage = async (pageLeads) => {
    const filtered = pageLeads.filter(lead => filterLead(lead, filters));
    if (filtered.length === 0) return;
    matchedCount += filtered.length;

    for (let i = 0; i < filtered.length; i += batchSize) {
      const subBatch = filtered.slice(i, i + batchSize);

      // Enforce the lender's per-minute rate limit before firing this sub-batch.
      await limiter.take(subBatch.length);

      await Promise.allSettled(
        subBatch.map(async (lead) => {
          let category;
          let sendError = null;
          try {
            const result = await sendFunction(lead);
            category = categorizeResult(result, null);

            let retries = 3;
            while (retries > 0) {
              try {
                await Lead.updateByIdNoValidation(lead.leadId, {
                  [`pushedTo.${lender}`]: {
                    batchId,
                    pushedAt: new Date().toISOString(),
                    status: 'success',
                    category
                  }
                });
                break;
              } catch (updateError) {
                if (updateError.name === 'ProvisionedThroughputExceededException' && retries > 1) {
                  retries--;
                  await new Promise(resolve => setTimeout(resolve, 500));
                } else {
                  throw updateError;
                }
              }
            }

            processingRecords.push({ batchId, leadId: lead.leadId, status: 'success', category });
            successCount++;
          } catch (error) {
            sendError = error;
            category = 'Failed';

            let retries = 3;
            while (retries > 0) {
              try {
                await Lead.updateByIdNoValidation(lead.leadId, {
                  [`pushedTo.${lender}`]: {
                    batchId,
                    pushedAt: new Date().toISOString(),
                    status: 'failed',
                    category,
                    error: error.message
                  }
                });
                break;
              } catch (updateError) {
                if (updateError.name === 'ProvisionedThroughputExceededException' && retries > 1) {
                  retries--;
                  await new Promise(resolve => setTimeout(resolve, 500));
                } else {
                  console.error(`Failed to update lead ${lead.leadId}:`, updateError.message);
                  break;
                }
              }
            }

            processingRecords.push({
              batchId,
              leadId: lead.leadId,
              status: 'failed',
              category,
              errorMessage: error.message.substring(0, 500)
            });

            await LeadDistributionStats.addError(batchId, {
              message: `Lead ${lead.leadId}: ${error.message}`
            });

            failCount++;
          }

          // Tally categorized stats (ACCEPT / REJECTED / Failed / other).
          categoryTotals[category] = (categoryTotals[category] || 0) + 1;
          pendingCats[category] = (pendingCats[category] || 0) + 1;
          processedCount++;
          pending.processedLeads++;
          if (sendError) pending.failedLeads++; else pending.successfulLeads++;

          if (progressCallback) {
            progressCallback({
              type: 'lead_processed',
              leadId: lead.leadId,
              status: sendError ? 'failed' : 'success',
              category,
              error: sendError ? sendError.message : undefined,
              processed: processedCount,
              total: matchedCount,
              successful: successCount,
              failed: failCount
            });
          }
        })
      );

      await flushProcessingRecords();
      await flushCounters();

      console.log(`[Batch ${batchId}] Progress: ${processedCount} processed (Success: ${successCount}, Failed: ${failCount}) | ACCEPT ${categoryTotals.ACCEPT} REJECTED ${categoryTotals.REJECTED} Failed ${categoryTotals.Failed} other ${categoryTotals.other}`);

      if (i + batchSize < filtered.length && delayMs > 0) {
        await new Promise(resolve => setTimeout(resolve, delayMs));
      }
    }
  };

  try {
    console.log(`[Batch ${batchId}] Starting streaming distribution...`);

    // Determine which sources to stream over.
    let sourcesToStream = null;
    let useDateRangeFallback = false;
    if (filters.sources && filters.sources.length > 0) {
      sourcesToStream = filters.sources;
    } else {
      const envSources = process.env.LEAD_SOURCES?.split(',').map(s => s.trim()).filter(Boolean) || [];
      if (envSources.length > 0) {
        sourcesToStream = envSources;
      } else if (filters.startDate && filters.endDate) {
        useDateRangeFallback = true;
      } else {
        throw new Error('Either sources or a date range (startDate + endDate) must be provided when LEAD_SOURCES env is not set.');
      }
    }

    if (sourcesToStream) {
      // Stream page-by-page per source: fetch → filter → send → discard.
      for (const source of sourcesToStream) {
        console.log(`[Batch ${batchId}] Streaming source: ${source}`);
        let lastEvaluatedKey = null;
        do {
          const queryOptions = {
            limit: 1000,
            startDate: filters.startDate,
            endDate: filters.endDate
          };
          if (lastEvaluatedKey) queryOptions.lastEvaluatedKey = lastEvaluatedKey;

          const result = await Lead.findBySource(source, queryOptions);

          let pageLeads = [];
          if (Array.isArray(result)) {
            pageLeads = result;
            lastEvaluatedKey = null; // arrays carry no pagination cursor
          } else {
            pageLeads = result.items || [];
            lastEvaluatedKey = result.lastEvaluatedKey || null;
          }

          await processPage(pageLeads);
          // pageLeads goes out of scope on next iteration → eligible for GC.
        } while (lastEvaluatedKey);

        console.log(`[Batch ${batchId}] Source ${source} complete.`);
      }
    } else if (useDateRangeFallback) {
      // Date-range fallback. findByDateRange doesn't page here, so guard memory
      // by processing whatever it returns, then discarding.
      const result = await Lead.findByDateRange(filters.startDate, filters.endDate, { limit: null });
      await processPage(result.items || []);
    }

    // Final flush of any remaining buffered records/counters.
    await flushProcessingRecords(true);
    await flushCounters(true);

    if (matchedCount === 0) {
      await LeadDistributionStats.updateBatchStats(batchId, {
        status: 'COMPLETED',
        totalLeads: 0,
        completedAt: new Date().toISOString()
      });
      console.log(`[Batch ${batchId}] No leads matched criteria.`);
      if (progressCallback) {
        progressCallback({ type: 'batch_completed', batchId, totalLeads: 0, successful: 0, failed: 0, status: 'COMPLETED' });
      }
      return;
    }

    // Final authoritative status + totals (overwrites the incremental values).
    await LeadDistributionStats.updateBatchStats(batchId, {
      status: failCount === 0 ? 'COMPLETED' : (successCount === 0 ? 'FAILED' : 'PARTIAL'),
      completedAt: new Date().toISOString(),
      totalLeads: matchedCount,
      processedLeads: processedCount,
      successfulLeads: successCount,
      failedLeads: failCount
    });

    console.log(`[Batch ${batchId}] ✅ Completed: ${successCount} successful, ${failCount} failed out of ${processedCount} | ACCEPT ${categoryTotals.ACCEPT} REJECTED ${categoryTotals.REJECTED} Failed ${categoryTotals.Failed} other ${categoryTotals.other}`);

    if (progressCallback) {
      progressCallback({
        type: 'batch_completed',
        batchId,
        totalLeads: matchedCount,
        successful: successCount,
        failed: failCount,
        statusCategories: categoryTotals,
        status: failCount === 0 ? 'COMPLETED' : (successCount === 0 ? 'FAILED' : 'PARTIAL')
      });
    }

  } catch (error) {
    console.error(`[Batch ${batchId}] ❌ Fatal error:`, error);

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
      const sources = process.env.LEAD_SOURCES?.split(',').map(s => s.trim()) || [];

      if (sources.length > 0) {
        for (const source of sources) {
          let lastEvaluatedKey = null;
          do {
            const result = await Lead.findBySource(source, {
              limit: 1000,
              startDate: filters.startDate,
              endDate: filters.endDate,
              lastEvaluatedKey
            });
            const leads = result.items || [];
            const filtered = leads.filter(lead => filterLead(lead, filters));
            totalCount += filtered.length;
            if (sampleLeads.length < 10) {
              sampleLeads = sampleLeads.concat(filtered.slice(0, 10 - sampleLeads.length));
            }
            lastEvaluatedKey = result.lastEvaluatedKey || null;
          } while (lastEvaluatedKey);
        }
      } else if (filters.startDate && filters.endDate) {
        const result = await Lead.findByDateRange(filters.startDate, filters.endDate, { limit: null });
        const filtered = (result.items || []).filter(lead => filterLead(lead, filters));
        totalCount = filtered.length;
        sampleLeads = filtered.slice(0, 10);
      } else {
        throw new Error('Either sources or a date range must be provided.');
      }
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
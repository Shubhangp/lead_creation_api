// controllers/leadDistributionController.js
const Lead = require('../models/leadModel');
const LeadDistributionStats = require('../models/leadDistributionStatsModel');
const { getRateLimit } = require('../config/lenderRateLimits');
const { categorizeLenderResult } = require('../config/lenderResultCategorizer');

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
  sendToCreditLinksGold,
  sendToKamakshiMoney,
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
    "CreditLinksGold": sendToCreditLinksGold,
    "kamakshimoney": sendToKamakshiMoney,
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

  async take(n = 1, shouldAbort = null) {
    // If the request is bigger than the whole bucket, cap the wait to one full refill.
    const need = Math.min(n, this.capacity);
    while (true) {
      // Bail out of a long rate-limit wait as soon as the batch is cancelled.
      if (shouldAbort && shouldAbort()) return false;
      this._refill();
      if (this.tokens >= need) {
        this.tokens -= need;
        return true;
      }
      const deficit = need - this.tokens;
      const waitMs = Math.ceil(deficit / this.refillPerMs);
      await new Promise(resolve => setTimeout(resolve, Math.min(waitMs, 1000)));
    }
  }
}

// In-memory guard so the same batchId's background loop is never running
// twice at once in this process — e.g. boot-resume racing a still-running
// job, or resumeIncompleteDistributionBatches() being triggered twice.
const activeDistributionBatchIds = new Set();

// Batches an admin asked to stop (POST /batch/:batchId/cancel). Checked before
// every sub-batch so a stop takes effect within one sub-batch. The DB status
// (CANCELLED) is the durable truth and is re-read once per page as a backstop.
const cancelledDistributionBatchIds = new Set();

/**
 * Background job processing function
 *
 * MEMORY-SAFE / STREAMING: instead of collecting every lead into one giant
 * `allLeads` array (and a second `filteredLeads` copy) — which caused OOM crashes
 * on 2L+ datasets — we now process ONE DynamoDB page at a time: fetch → filter →
 * rate-limited send → discard. Peak memory stays bounded to a single page.
 * A per-lender TokenBucket enforces the lender's per-minute rate limit.
 *
 * CRASH-RESUMABLE: after every page, the current position (source index +
 * DynamoDB lastEvaluatedKey) plus enough of the original request to relaunch
 * (lender/filters/batchSize/delayMs) is checkpointed onto the batch record.
 * `resumeState` lets a restart pick the loop back up from that checkpoint
 * instead of starting the whole batch over.
 */
const processLeadsInBackground = async (batchId, lender, filters, batchSize, delayMs, progressCallback, resumeState = null) => {
  if (activeDistributionBatchIds.has(batchId)) {
    console.log(`[Batch ${batchId}] Already running in this process — skipping duplicate launch`);
    return;
  }
  activeDistributionBatchIds.add(batchId);

  const sendFunction = getLenderSendFunction(lender);

  let cancelled = false;
  const shouldStop = () => cancelled || cancelledDistributionBatchIds.has(batchId);
  const refreshCancelledFromDb = async () => {
    if (shouldStop()) { cancelled = true; return true; }
    try {
      const fresh = await LeadDistributionStats.findById(batchId);
      if (fresh && fresh.status === 'CANCELLED') cancelled = true;
    } catch (e) {
      console.warn(`[Batch ${batchId}] Cancel-status check failed:`, e.message);
    }
    return cancelled;
  };

  // Per-lender rate limiter (per minute). Sending is throttled to the lender's cap.
  const rpm = getRateLimit(lender);
  const limiter = new TokenBucket(rpm);
  console.log(`[Batch ${batchId}] Rate limit for ${lender}: ${rpm} leads/min`);

  // Running totals — shared across all pages (kept as plain scalars, not arrays).
  let processedCount = 0;
  let successCount = 0;   // sent to lender API without throwing
  let failCount = 0;      // threw an error
  let matchedCount = 0;   // leads passing the filter (== totalLeads)
  const categoryTotals = {}; // dynamic per-lender category → running count
  let processingRecords = [];
  // Pending deltas flushed to DynamoDB periodically (throttle-safe).
  let pending = { processedLeads: 0, successfulLeads: 0, failedLeads: 0 };
  let pendingCats = {}; // dynamic per-lender category → pending delta

  const sumValues = (obj) => Object.values(obj).reduce((a, b) => a + b, 0);

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
    if (force || sumValues(pendingCats) >= 50) {
      const cats = pendingCats;
      pendingCats = {};
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
      // Admin pressed Stop — send nothing more.
      if (shouldStop()) { cancelled = true; return; }

      const subBatch = filtered.slice(i, i + batchSize);

      // Enforce the lender's per-minute rate limit before firing this sub-batch.
      await limiter.take(subBatch.length, shouldStop);
      if (shouldStop()) { cancelled = true; return; }

      await Promise.allSettled(
        subBatch.map(async (lead) => {
          let category;
          let sendError = null;
          try {
            const result = await sendFunction(lead);
            category = categorizeLenderResult(lender, result, null);

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
            category = categorizeLenderResult(lender, undefined, error);

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

          // Tally categorized stats (dynamic per-lender category keys).
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

      console.log(`[Batch ${batchId}] Progress: ${processedCount} processed (Success: ${successCount}, Failed: ${failCount}) | categories: ${JSON.stringify(categoryTotals)}`);

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

    // Persist a checkpoint carrying everything needed to relaunch this batch
    // from wherever the loop currently is. Written after every page so a
    // crash/restart never loses more than one in-flight page of progress.
    const persistCheckpoint = async (sourceIndex, lastEvaluatedKey) => {
      try {
        await LeadDistributionStats.updateCheckpoint(batchId, {
          lender,
          filters,
          batchSize,
          delayMs,
          sourceIndex,
          lastEvaluatedKey
        });
      } catch (checkpointError) {
        console.error(`[Batch ${batchId}] Failed to persist checkpoint:`, checkpointError.message);
      }
    };

    const startSourceIndex = resumeState?.sourceIndex || 0;

    if (sourcesToStream) {
      // Stream page-by-page per source: fetch → filter → send → discard.
      for (let srcIdx = startSourceIndex; srcIdx < sourcesToStream.length; srcIdx++) {
        const source = sourcesToStream[srcIdx];
        console.log(`[Batch ${batchId}] Streaming source: ${source}`);
        // Only the source we're resuming into picks up mid-page; every other
        // source (or a fresh, non-resumed run) starts from the beginning.
        let lastEvaluatedKey = (srcIdx === startSourceIndex && resumeState?.lastEvaluatedKey) || null;
        do {
          if (await refreshCancelledFromDb()) break;

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
          if (shouldStop()) { cancelled = true; break; }

          // ── CHECKPOINT — if the process dies here, the next boot resumes
          // this batch from exactly this source + page. Once lastEvaluatedKey
          // comes back null the current source is fully done, so the
          // checkpoint must point at the NEXT source (srcIdx + 1) — pointing
          // it at srcIdx with a null key would make a resume re-stream this
          // already-finished source from page 1 and resend every lead in it.
          await persistCheckpoint(lastEvaluatedKey ? srcIdx : srcIdx + 1, lastEvaluatedKey);
          // pageLeads goes out of scope on next iteration → eligible for GC.
        } while (lastEvaluatedKey);

        if (cancelled) break;
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

    // ── Stopped by admin ─────────────────────────────────────────────────
    // Status is already CANCELLED (set by the cancel endpoint). Record exact
    // totals for what was actually sent and stop — never overwrite the status.
    if (cancelled || shouldStop() || await refreshCancelledFromDb()) {
      await LeadDistributionStats.updateBatchStats(batchId, {
        totalLeads: processedCount,
        processedLeads: processedCount,
        successfulLeads: successCount,
        failedLeads: failCount,
        statusCategories: categoryTotals
      });
      console.log(`[Batch ${batchId}] ⛔ Stopped by admin after ${processedCount} leads (Success: ${successCount}, Failed: ${failCount})`);
      if (progressCallback) {
        progressCallback({
          type: 'batch_cancelled',
          batchId,
          processed: processedCount,
          successful: successCount,
          failed: failCount,
          statusCategories: categoryTotals,
          status: 'CANCELLED'
        });
      }
      return;
    }

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

    // Final authoritative status + totals (overwrites the incremental values,
    // including the full categorized breakdown so the stored map is exact).
    await LeadDistributionStats.updateBatchStats(batchId, {
      status: failCount === 0 ? 'COMPLETED' : (successCount === 0 ? 'FAILED' : 'PARTIAL'),
      completedAt: new Date().toISOString(),
      totalLeads: matchedCount,
      processedLeads: processedCount,
      successfulLeads: successCount,
      failedLeads: failCount,
      statusCategories: categoryTotals
    });

    console.log(`[Batch ${batchId}] ✅ Completed: ${successCount} successful, ${failCount} failed out of ${processedCount} | categories: ${JSON.stringify(categoryTotals)}`);

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

    if (!shouldStop()) {
      await LeadDistributionStats.updateBatchStats(batchId, {
        status: 'FAILED',
        completedAt: new Date().toISOString()
      });
    }

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
  } finally {
    activeDistributionBatchIds.delete(batchId);
    cancelledDistributionBatchIds.delete(batchId);
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

    // Checkpoint the raw inputs immediately, before any page has run, so a
    // crash in the first few seconds still leaves enough on the batch record
    // to auto-resume this job from the start on next boot.
    await LeadDistributionStats.updateCheckpoint(batch.batchId, {
      lender, filters, batchSize, delayMs, sourceIndex: 0, lastEvaluatedKey: null
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

    // Checkpoint the raw inputs immediately, before any page has run, so a
    // crash in the first few seconds still leaves enough on the batch record
    // to auto-resume this job from the start on next boot. (The SSE stream
    // itself doesn't survive a restart, but the underlying send/save work does.)
    await LeadDistributionStats.updateCheckpoint(batch.batchId, {
      lender, filters, batchSize, delayMs, sourceIndex: 0, lastEvaluatedKey: null
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
 * POST /api/v1/distribution/batch/:batchId/cancel
 *
 * Stop a running distribution. Requests already in flight to the lender
 * finish (they can't be recalled), but no further leads are sent.
 */
const cancelDistribution = async (req, res) => {
  try {
    const { batchId } = req.params;
    const batch = await LeadDistributionStats.findById(batchId);
    if (!batch) {
      return res.status(404).json({ success: false, message: 'Batch not found' });
    }
    if (batch.status !== 'PROCESSING') {
      return res.status(409).json({
        success: false,
        message: `Batch is already ${batch.status} — nothing to stop.`,
        data: batch
      });
    }

    // In-memory flag first so the running loop stops at its very next check.
    if (activeDistributionBatchIds.has(batchId)) cancelledDistributionBatchIds.add(batchId);

    const updated = await LeadDistributionStats.markCancelled(batchId);
    if (!updated) {
      cancelledDistributionBatchIds.delete(batchId);
      const latest = await LeadDistributionStats.findById(batchId);
      return res.status(409).json({
        success: false,
        message: `Batch finished (${latest?.status}) before it could be stopped.`,
        data: latest
      });
    }

    console.log(`[Batch ${batchId}] Cancel requested by admin`);
    res.status(200).json({
      success: true,
      message: 'Distribution stopped. No more leads will be sent to the lender for this batch.',
      data: updated
    });
  } catch (error) {
    console.error('Error cancelling distribution:', error);
    res.status(500).json({ success: false, message: 'Failed to stop distribution', error: error.message });
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

/**
 * RESUME INCOMPLETE DISTRIBUTION BATCHES ON SERVER STARTUP
 *
 * Call this once from server.js after the DB connection is confirmed. It
 * finds every batch still marked PROCESSING (interrupted by a crash/restart)
 * and relaunches processLeadsInBackground from its last checkpoint — same
 * shape as resumeIncompletePushJobs() in processLeadController.js.
 */
const resumeIncompleteDistributionBatches = async () => {
  try {
    const activeBatches = await LeadDistributionStats.findActiveBatches();

    if (activeBatches.length === 0) {
      console.log('[LeadDistribution] No interrupted distribution batches found on startup.');
      return;
    }

    console.log(`[LeadDistribution] Found ${activeBatches.length} interrupted batch(es) — auto-resuming from checkpoint...`);

    // Stagger relaunches so a crash that interrupted several batches at once
    // doesn't slam DynamoDB/lender APIs the instant the server boots.
    activeBatches.forEach((batch, i) => {
      if (!batch.checkpoint) {
        console.warn(`[LeadDistribution] Batch ${batch.batchId} has no checkpoint (pre-restore batch?) — skipping auto-resume.`);
        return;
      }

      setTimeout(() => {
        const { lender, filters, batchSize, delayMs, sourceIndex, lastEvaluatedKey } = batch.checkpoint;
        console.log(`[LeadDistribution] Auto-resuming batch ${batch.batchId} (lender=${lender}, sourceIndex=${sourceIndex})`);
        processLeadsInBackground(
          batch.batchId,
          lender,
          filters,
          batchSize,
          delayMs,
          null, // no SSE progress callback on an auto-resumed batch
          { sourceIndex, lastEvaluatedKey }
        ).catch(error => {
          console.error(`[LeadDistribution] Auto-resume failed for batch ${batch.batchId}:`, error.message);
        });
      }, i * 2000);
    });
  } catch (err) {
    console.error('[LeadDistribution] resumeIncompleteDistributionBatches failed:', err.message);
  }
};

module.exports = {
  streamLeadsToLender,
  cancelDistribution,
  startBackgroundDistribution,
  getBatchStats,
  getAllBatches,
  getLenderStats,
  getLeadsPreview,
  resumeIncompleteDistributionBatches
};
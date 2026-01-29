const PendingLead = require('../models/pendingLeadModel');
const Lead = require('../models/leadModel');
const timeUtils = require('../utils/timeutils');
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

class ContinuousLeadScheduler {
  constructor() {
    this.isRunning = false;
    this.isProcessing = false;
    this.batchSize = 500; // Process 500 leads at a time
    this.delayBetweenBatches = 1000; // 1 second between batches
    this.delayBetweenRequests = 100; // 100ms between individual lender requests
    this.maxConcurrentProcessing = 10; // Process 10 leads concurrently
    this.stats = {
      totalProcessed: 0,
      successfulSends: 0,
      failedSends: 0,
      startTime: null
    };
  }

  /**
   * Start continuous processing
   */
  async start() {
    if (this.isRunning) {
      console.log('Continuous scheduler already running');
      return;
    }

    this.isRunning = true;
    this.stats.startTime = new Date();
    console.log('=== Starting Continuous Lead Scheduler ===');
    console.log(`Batch Size: ${this.batchSize}`);
    console.log(`Max Concurrent: ${this.maxConcurrentProcessing}`);
    
    // Start continuous loop
    this.processContinuously();
  }

  /**
   * Stop continuous processing
   */
  stop() {
    this.isRunning = false;
    console.log('Continuous scheduler stopped');
    console.log('Final Stats:', this.stats);
  }

  /**
   * Main continuous processing loop
   */
  async processContinuously() {
    while (this.isRunning) {
      try {
        // Get batch of pending leads
        const result = await PendingLead.getAllPending(this.batchSize);
        
        if (result.items.length === 0) {
          // No pending leads, wait a bit before checking again
          await this.delay(5000); // Wait 5 seconds
          continue;
        }

        console.log(`\n[${new Date().toISOString()}] Processing batch of ${result.items.length} pending leads`);

        // Process the batch
        await this.processBatch(result.items);

        // Small delay between batches to prevent overwhelming the system
        await this.delay(this.delayBetweenBatches);

      } catch (error) {
        console.error('Error in continuous processing loop:', error);
        await this.delay(5000); // Wait before retrying
      }
    }
  }

  /**
   * Process a batch of pending leads with concurrency control
   */
  async processBatch(pendingLeads) {
    // Process in chunks for concurrency control
    const chunks = this.chunkArray(pendingLeads, this.maxConcurrentProcessing);
    
    for (const chunk of chunks) {
      // Process chunk concurrently
      const promises = chunk.map(pendingLead => 
        this.processPendingLead(pendingLead).catch(error => {
          console.error(`Error processing lead ${pendingLead.leadId}:`, error.message);
        })
      );

      await Promise.all(promises);
    }
  }

  /**
   * Process a single pending lead entry
   */
  async processPendingLead(pendingLead) {
    const { pendingLeadId, leadId, lenderNames, leadData } = pendingLead;

    console.log(`Processing lead ${leadId} for ${lenderNames.length} lenders`);

    const lendersToSend = [];
    const lendersOutOfTime = [];

    // Check which lenders are within their time range
    for (const lenderName of lenderNames) {
      const timeRange = await this.getLenderTimeRange(leadData.source, lenderName);
      
      if (timeUtils.isWithinTimeRange(timeRange)) {
        lendersToSend.push(lenderName);
      } else {
        lendersOutOfTime.push(lenderName);
      }
    }

    // Send to lenders that are within time range
    const successfulLenders = [];
    
    for (const lenderName of lendersToSend) {
      try {
        const result = await this.sendToLender(leadData, lenderName);
        
        if (this.isLenderSuccess(result, lenderName)) {
          successfulLenders.push(lenderName);
          this.stats.successfulSends++;
          console.log(`  ✓ ${lenderName} - SUCCESS`);
        } else {
          console.log(`  ✗ ${lenderName} - API returned failure`);
          this.stats.failedSends++;
        }
        
        // Small delay between lender requests
        await this.delay(this.delayBetweenRequests);
        
      } catch (error) {
        console.error(`  ✗ ${lenderName} - ERROR: ${error.message}`);
        this.stats.failedSends++;
      }
    }

    // Update the pending lead entry
    if (successfulLenders.length > 0) {
      // Remove successfully sent lenders
      await PendingLead.removeLenders(leadId, successfulLenders);
      console.log(`  Removed ${successfulLenders.length} successful lenders from pending list`);
    }

    // If there are lenders out of time range, keep the entry but update it
    if (lendersOutOfTime.length > 0 && successfulLenders.length > 0) {
      console.log(`  ${lendersOutOfTime.length} lenders still out of time range`);
    }

    // If all lenders processed (either successful or out of time), mark as processed
    if (lendersOutOfTime.length === 0 && successfulLenders.length === lenderNames.length) {
      // Entry will be automatically deleted by removeLenders
      console.log(`  ✓ All lenders processed for lead ${leadId}`);
    }

    // Update last processed time to prevent immediate reprocessing
    if (lendersOutOfTime.length > 0) {
      await PendingLead.markAsProcessed(pendingLeadId);
    }

    this.stats.totalProcessed++;
  }

  /**
   * Get lender time range from distribution rules
   */
  async getLenderTimeRange(source, lenderName) {
    try {
      const DistributionRule = require('../models/distributionRuleModel');
      const dbRules = await DistributionRule.findActiveBySource(source);
      
      if (!dbRules || !dbRules.rules) {
        return null; // No time restriction
      }

      const rules = dbRules.rules;
      
      // Check immediate lenders
      if (rules.immediate) {
        const lender = rules.immediate.find(l => 
          (typeof l === 'string' ? l : l.lender) === lenderName
        );
        if (lender && typeof lender === 'object' && lender.timeRange) {
          return lender.timeRange;
        }
      }

      // Check delayed lenders
      if (rules.delayed) {
        const lender = rules.delayed.find(l => 
          (typeof l === 'string' ? l : l.lender) === lenderName
        );
        if (lender && typeof lender === 'object' && lender.timeRange) {
          return lender.timeRange;
        }
      }

      return null; // No time restriction for this lender
    } catch (error) {
      console.error(`Error getting time range for ${lenderName}:`, error.message);
      return null;
    }
  }

  /**
   * Send lead to specific lender
   */
  async sendToLender(lead, lender) {
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
   * Check if lender response indicates success
   */
  isLenderSuccess(result, lenderName) {
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
   * Get current statistics
   */
  async getStats() {
    const pendingCount = await PendingLead.getPendingCount();
    const pendingStats = await PendingLead.getStats();
    
    const uptime = this.stats.startTime 
      ? Math.floor((Date.now() - this.stats.startTime.getTime()) / 1000)
      : 0;

    return {
      isRunning: this.isRunning,
      isProcessing: this.isProcessing,
      uptime: `${Math.floor(uptime / 3600)}h ${Math.floor((uptime % 3600) / 60)}m`,
      processing: {
        totalProcessed: this.stats.totalProcessed,
        successfulSends: this.stats.successfulSends,
        failedSends: this.stats.failedSends,
        successRate: this.stats.successfulSends > 0 
          ? ((this.stats.successfulSends / (this.stats.successfulSends + this.stats.failedSends)) * 100).toFixed(2) + '%'
          : 'N/A'
      },
      pending: {
        totalEntries: pendingCount,
        totalPendingLenders: pendingStats.totalPendingLenders,
        byLender: pendingStats.lenderBreakdown
      },
      configuration: {
        batchSize: this.batchSize,
        maxConcurrent: this.maxConcurrentProcessing,
        delayBetweenBatches: this.delayBetweenBatches,
        delayBetweenRequests: this.delayBetweenRequests
      }
    };
  }

  /**
   * Update configuration on the fly
   */
  updateConfig(config) {
    if (config.batchSize) this.batchSize = config.batchSize;
    if (config.maxConcurrent) this.maxConcurrentProcessing = config.maxConcurrent;
    if (config.delayBetweenBatches) this.delayBetweenBatches = config.delayBetweenBatches;
    if (config.delayBetweenRequests) this.delayBetweenRequests = config.delayBetweenRequests;
    
    console.log('Configuration updated:', {
      batchSize: this.batchSize,
      maxConcurrent: this.maxConcurrentProcessing,
      delayBetweenBatches: this.delayBetweenBatches,
      delayBetweenRequests: this.delayBetweenRequests
    });
  }

  /**
   * Utility: Split array into chunks
   */
  chunkArray(array, size) {
    const chunks = [];
    for (let i = 0; i < array.length; i += size) {
      chunks.push(array.slice(i, i + size));
    }
    return chunks;
  }

  /**
   * Utility: Delay execution
   */
  delay(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
  }

  /**
   * Manual trigger to process specific lead
   */
  async processSpecificLead(leadId) {
    const pendingLead = await PendingLead.findByLeadId(leadId);
    
    if (!pendingLead) {
      throw new Error(`No pending lead found with leadId: ${leadId}`);
    }

    await this.processPendingLead(pendingLead);
    return { success: true, leadId };
  }

  /**
   * Force process all pending for a specific lender
   */
  async processLenderNow(lenderName) {
    console.log(`Force processing all pending leads for ${lenderName}`);
    
    let processed = 0;
    let successful = 0;
    let lastKey = null;

    do {
      const result = await PendingLead.getAllPending(this.batchSize, lastKey);
      
      for (const pendingLead of result.items) {
        if (pendingLead.lenderNames.includes(lenderName)) {
          try {
            const sendResult = await this.sendToLender(pendingLead.leadData, lenderName);
            
            if (this.isLenderSuccess(sendResult, lenderName)) {
              await PendingLead.removeLenders(pendingLead.leadId, [lenderName]);
              successful++;
            }
            
            processed++;
            await this.delay(this.delayBetweenRequests);
          } catch (error) {
            console.error(`Error processing ${pendingLead.leadId} for ${lenderName}:`, error.message);
          }
        }
      }

      lastKey = result.lastEvaluatedKey;
    } while (lastKey);

    return {
      lender: lenderName,
      processed,
      successful,
      failed: processed - successful
    };
  }
}

// Singleton instance
const schedulerInstance = new ContinuousLeadScheduler();

module.exports = schedulerInstance;
const cron = require('node-cron');
const rcsService = require('../services/rcsService');

class RCSScheduler {
  constructor() {
    this.isRunning = false;
  }

  /**
   * Initialize RCS scheduler
   * Runs every 5 minutes during operating hours (10 AM to 7 PM IST)
   */
  init() {
    console.log('Initializing RCS Scheduler...');

    // Run every 3 minutes
    cron.schedule('*/3 * * * *', async () => {
      if (this.isRunning) {
        console.log('RCS processing already running, skipping...');
        return;
      }

      // Only process during operating hours
      if (rcsService.isWithinOperatingHours()) {
        this.isRunning = true;
        console.log('Processing pending RCS messages...');
        
        try {
          await rcsService.processPendingRCS();
        } catch (error) {
          console.error('Error in RCS scheduler:', error);
        } finally {
          this.isRunning = false;
        }
      } else {
        console.log('Outside operating hours, skipping RCS processing');
      }
    }, {
      scheduled: true,
      timezone: "Asia/Kolkata"
    });

    console.log('RCS Scheduler initialized successfully');
  }

  /**
   * Manual trigger for testing
   */
  async processPendingMessages() {
    if (this.isRunning) {
      throw new Error('RCS processing already in progress');
    }

    this.isRunning = true;
    try {
      await rcsService.processPendingRCS();
    } finally {
      this.isRunning = false;
    }
  }
}

module.exports = new RCSScheduler();
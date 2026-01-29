const moment = require('moment-timezone');

class TimeUtils {
  /**
   * Check if current time is within the specified time range
   * @param {Object} timeRange - { start: 'HH:mm', end: 'HH:mm', timezone: 'Asia/Kolkata' }
   * @returns {boolean}
   */
  static isWithinTimeRange(timeRange) {
    if (!timeRange || !timeRange.start || !timeRange.end) {
      // If no time range specified, allow sending anytime
      return true;
    }

    const timezone = timeRange.timezone || 'Asia/Kolkata';
    const now = moment.tz(timezone);
    
    const [startHour, startMinute] = timeRange.start.split(':').map(Number);
    const [endHour, endMinute] = timeRange.end.split(':').map(Number);

    const startTime = moment.tz(timezone)
      .hour(startHour)
      .minute(startMinute)
      .second(0)
      .millisecond(0);

    const endTime = moment.tz(timezone)
      .hour(endHour)
      .minute(endMinute)
      .second(0)
      .millisecond(0);

    // Handle case where end time is before start time (crosses midnight)
    if (endTime.isBefore(startTime)) {
      // Time range crosses midnight
      return now.isAfter(startTime) || now.isBefore(endTime);
    }

    return now.isBetween(startTime, endTime, null, '[]');
  }

  /**
   * Get the next available time when leads can be sent
   * @param {Object} timeRange - { start: 'HH:mm', end: 'HH:mm', timezone: 'Asia/Kolkata' }
   * @returns {string} ISO timestamp
   */
  static getNextAvailableTime(timeRange) {
    if (!timeRange || !timeRange.start) {
      // If no time range, return immediate
      return new Date().toISOString();
    }

    const timezone = timeRange.timezone || 'Asia/Kolkata';
    const now = moment.tz(timezone);
    
    const [startHour, startMinute] = timeRange.start.split(':').map(Number);

    let nextAvailable = moment.tz(timezone)
      .hour(startHour)
      .minute(startMinute)
      .second(0)
      .millisecond(0);

    // If we've already passed today's start time, schedule for tomorrow
    if (now.isAfter(nextAvailable)) {
      nextAvailable.add(1, 'day');
    }

    return nextAvailable.toISOString();
  }

  /**
   * Check if it's currently within business hours for a specific day
   * @param {Object} timeRange
   * @param {Date} targetDate - Optional specific date to check
   * @returns {boolean}
   */
  static isBusinessHours(timeRange, targetDate = null) {
    const checkDate = targetDate ? moment(targetDate) : moment();
    
    // Check if it's a weekend (Saturday = 6, Sunday = 0)
    const dayOfWeek = checkDate.day();
    if (dayOfWeek === 0 || dayOfWeek === 6) {
      return false;
    }

    return this.isWithinTimeRange(timeRange);
  }

  /**
   * Calculate delay until next available time window
   * @param {Object} timeRange
   * @returns {number} milliseconds to wait
   */
  static getDelayUntilNextWindow(timeRange) {
    if (!timeRange || !timeRange.start) {
      return 0;
    }

    const nextTime = this.getNextAvailableTime(timeRange);
    const delayMs = new Date(nextTime).getTime() - Date.now();
    
    return Math.max(0, delayMs);
  }

  /**
   * Get a human-readable description of when the lead will be sent
   * @param {Object} timeRange
   * @returns {string}
   */
  static getNextSendDescription(timeRange) {
    if (!timeRange || !timeRange.start) {
      return 'immediately';
    }

    const isWithin = this.isWithinTimeRange(timeRange);
    if (isWithin) {
      return 'immediately (within business hours)';
    }

    const nextTime = this.getNextAvailableTime(timeRange);
    const nextMoment = moment(nextTime);
    const now = moment();

    if (nextMoment.isSame(now, 'day')) {
      return `today at ${timeRange.start}`;
    } else if (nextMoment.isSame(now.add(1, 'day'), 'day')) {
      return `tomorrow at ${timeRange.start}`;
    } else {
      return `on ${nextMoment.format('MMM DD')} at ${timeRange.start}`;
    }
  }

  /**
   * Parse time string to moment object
   * @param {string} timeString - 'HH:mm'
   * @param {string} timezone
   * @returns {moment}
   */
  static parseTime(timeString, timezone = 'Asia/Kolkata') {
    const [hour, minute] = timeString.split(':').map(Number);
    return moment.tz(timezone).hour(hour).minute(minute).second(0).millisecond(0);
  }

  /**
   * Format ISO timestamp to readable string
   * @param {string} isoTimestamp
   * @param {string} timezone
   * @returns {string}
   */
  static formatTimestamp(isoTimestamp, timezone = 'Asia/Kolkata') {
    return moment.tz(isoTimestamp, timezone).format('YYYY-MM-DD HH:mm:ss z');
  }

  /**
   * Check if a scheduled time has passed
   * @param {string} scheduledFor - ISO timestamp
   * @returns {boolean}
   */
  static isPastScheduledTime(scheduledFor) {
    return moment().isAfter(moment(scheduledFor));
  }

  /**
   * Get all leads that should be sent in a specific time window
   * @param {Array} pendingLeads
   * @param {number} windowMinutes - Size of the batch window in minutes
   * @returns {Array}
   */
  static getLeadsInWindow(pendingLeads, windowMinutes = 30) {
    const now = moment();
    const windowEnd = moment().add(windowMinutes, 'minutes');

    return pendingLeads.filter(lead => {
      const scheduledTime = moment(lead.scheduledFor);
      return scheduledTime.isBetween(now, windowEnd, null, '[]');
    });
  }

  /**
   * Group pending leads by lender and scheduled time bucket
   * @param {Array} pendingLeads
   * @param {number} bucketMinutes - Group by this many minutes
   * @returns {Object}
   */
  static groupLeadsByLenderAndTime(pendingLeads, bucketMinutes = 60) {
    const grouped = {};

    pendingLeads.forEach(lead => {
      const lender = lead.lenderName;
      const scheduledMoment = moment(lead.scheduledFor);
      
      // Round down to nearest bucket
      const bucket = scheduledMoment
        .clone()
        .minute(Math.floor(scheduledMoment.minute() / bucketMinutes) * bucketMinutes)
        .second(0)
        .millisecond(0)
        .toISOString();

      if (!grouped[lender]) {
        grouped[lender] = {};
      }

      if (!grouped[lender][bucket]) {
        grouped[lender][bucket] = [];
      }

      grouped[lender][bucket].push(lead);
    });

    return grouped;
  }

  /**
   * Calculate recommended batch size based on time window
   * @param {Object} timeRange
   * @param {number} totalLeads
   * @returns {Object} { batchSize, batchCount, intervalMinutes }
   */
  static calculateBatchStrategy(timeRange, totalLeads) {
    if (!timeRange || !timeRange.start || !timeRange.end) {
      return {
        batchSize: totalLeads,
        batchCount: 1,
        intervalMinutes: 0,
        strategy: 'immediate'
      };
    }

    const [startHour, startMinute] = timeRange.start.split(':').map(Number);
    const [endHour, endMinute] = timeRange.end.split(':').map(Number);

    // Calculate total minutes in the window
    const totalMinutes = (endHour * 60 + endMinute) - (startHour * 60 + startMinute);

    if (totalLeads <= 50) {
      // Small batch - send all at once
      return {
        batchSize: totalLeads,
        batchCount: 1,
        intervalMinutes: 0,
        strategy: 'single-batch'
      };
    } else if (totalLeads <= 200) {
      // Medium batch - split into 4 batches
      const batchCount = 4;
      return {
        batchSize: Math.ceil(totalLeads / batchCount),
        batchCount,
        intervalMinutes: Math.floor(totalMinutes / batchCount),
        strategy: 'medium-batch'
      };
    } else {
      // Large batch - hourly batches
      const batchCount = Math.floor(totalMinutes / 60);
      return {
        batchSize: Math.ceil(totalLeads / batchCount),
        batchCount,
        intervalMinutes: 60,
        strategy: 'hourly-batch'
      };
    }
  }
}

module.exports = TimeUtils;
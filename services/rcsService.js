const axios = require('axios');
const { RCSQueue, RCSLog } = require('../models/rcsModels');
const DistributionRule = require('../models/distributionRuleModel');

class RCSService {
  constructor() {
    this.apiKey = process.env.RCS_API_KEY;
    this.apiUrl = 'https://api.omni.tatatelebusiness.com/rcs/messages';
    this.timezone = 'Asia/Kolkata';
  }

  /**
   * Check if current time is within operating hours (10 AM to 7 PM IST)
   */
  isWithinOperatingHours(startTime = '10:00', endTime = '19:00') {
    const now = new Date();
    const istTime = new Date(now.toLocaleString("en-US", { timeZone: this.timezone }));

    const currentHour = istTime.getHours();
    const currentMinute = istTime.getMinutes();
    const currentTimeInMinutes = currentHour * 60 + currentMinute;

    const [startHour, startMinute] = startTime.split(':').map(Number);
    const [endHour, endMinute] = endTime.split(':').map(Number);

    const startTimeInMinutes = startHour * 60 + startMinute;
    const endTimeInMinutes = endHour * 60 + endMinute;

    return currentTimeInMinutes >= startTimeInMinutes && currentTimeInMinutes <= endTimeInMinutes;
  }

  /**
   * Calculate next business day at start time
   */
  getNextBusinessDayStart(startTime = '10:00') {
    const now = new Date();
    const tomorrow = new Date(now);
    tomorrow.setDate(now.getDate() + 1);

    const [hour, minute] = startTime.split(':').map(Number);
    tomorrow.setHours(hour, minute, 0, 0);

    // Convert to IST
    const istTomorrow = new Date(tomorrow.toLocaleString("en-US", { timeZone: this.timezone }));
    return istTomorrow;
  }

  /**
   * Generate RCS payload for lender success
   */
  generateLenderSuccessPayload(phone, lenderName, leadData) {
    const lenderConfigs = {
      'OVLY': {
        title: 'Loans upto Rs 5 Lakhs in 5 minutes',
        description: 'Hello\n\nTake control of emergencies with a loan of up to ₹5 Lakh💵\n\n✅Interest starting at as low as 1.5%* per month.\n💳EMIs as low as ₹512*.\n\n🤝 Trusted by over 4 crore Indians.\n🏦  Loans via RBI-registered lenders.\n\n📱Sign up now and get started.\n\nT&C Apply.',
        imageUrl: 'https://res.cloudinary.com/dha4otbzk/image/upload/v1758734320/ovly_hxo9w7.jpg',
        actionUrl: 'https://smartcoin.onelink.me/KLIY/ratecutjuly'
      },
      'ZYPE': {
        title: 'Get Instant Personal Loan up to 2 Lakhs',
        description: 'Need salary early, get it now with a loan of up to ₹2 Lakhs💵\n\n✅Interest starting at as low as 1.5%* per month.\n💳EMIs as low as ₹512*.\n🤝 Trusted by over 4 crore Indians.\n🏦  Loans via RBI-registered lenders.\n\n📱Sign up now and get started.\n\nT&C Apply.',
        imageUrl: 'https://res.cloudinary.com/dha4otbzk/image/upload/v1758734323/Zype_v04vki.jpg',
        actionUrl: 'https://zype.sng.link/Ajygt/0bli?_dl=com.zype.mobile&_smtype=3'
      },
      'LendingPlate': {
        title: 'LendingPlate: Best Way to Borrow',
        description: 'Hello\n\nGet an instant personal loan of up to ₹2.5 Lakh💵 with LendingPlate.\n\n✅Interest starting at as low as 1.5%* per month.\n💳EMIs as low as ₹512*.\n\n🤝 Trusted by over 4 crore Indians.\n🏦  Loans via RBI-registered lenders.\n\n📱Sign up now and get started.\n\nT&C Apply.',
        imageUrl: 'https://res.cloudinary.com/dha4otbzk/image/upload/v1758734324/LP_zsqfp8.jpg',
        actionUrl: 'https://lendingplate.com/personal-loan-lead-form?utm_source=RATECUT&utm_campaign=RATECUT#applynow'
      },
      'FATAKPAY': {
        title: 'FatakPay: Get Loan upto ₹2 Lakhs💵',
        description: '✅Interest starting at as low as 1.5%* per month.\n💳EMIs as low as ₹512*.\n🤝 Trusted by over 4 crore Indians.\n🏦  Loans via RBI-registered lenders.\nT&C Apply.',
        imageUrl: 'https://res.cloudinary.com/dha4otbzk/image/upload/v1758734319/fatakpay_giqt2z.jpg',
        actionUrl: 'https://web.fatakpay.com/authentication/login?utm_source=708_8FQLQ&source_caller=api&shortlink=708_8FQLQ&utm_medium='
      },
      'default': {
        title: 'Loan Approved!',
        description: 'Your loan application has been approved✅\n\n🏦  Loans via RBI-registered lenders.\n\n📱Complete your application now!\n\nT&C Apply.',
        imageUrl: 'https://res.cloudinary.com/dha4otbzk/image/upload/v1758734319/zet_exjwcl.jpg',
        actionUrl: 'https://ratecut.com/complete-loan'
      }
    };

    const config = lenderConfigs[lenderName] || lenderConfigs['default'];

    return {
      to: `+91${phone}`,
      content: {
        richCardDetails: {
          standalone: {
            cardOrientation: "VERTICAL",
            content: {
              cardTitle: config.title,
              cardDescription: config.description,
              cardMedia: {
                mediaHeight: "TALL",
                contentInfo: {
                  fileUrl: config.imageUrl
                }
              },
              suggestions: [
                {
                  action: {
                    plainText: "Apply Now",
                    postBack: {
                      data: `APPLY_${lenderName}_${leadData._id}`
                    },
                    openUrl: {
                      url: config.actionUrl
                    }
                  }
                }
              ]
            }
          }
        }
      },
      source: "api"
    };
  }

  /**
   * Generate RCS payload for ZET campaign
   */
  generateZetCampaignPayload(phone, leadData) {
    return {
      to: `+91${phone}`,
      content: {
        richCardDetails: {
          standalone: {
            cardOrientation: "VERTICAL",
            content: {
              cardTitle: "Don't Miss Out on Your Loan!",
              cardDescription: "We have exclusive loan offers waiting for you. Check eligibility in 2 minutes!",
              cardMedia: {
                mediaHeight: "TALL",
                contentInfo: {
                  fileUrl: "https://example.com/zet-campaign.png"
                }
              },
              suggestions: [
                {
                  action: {
                    plainText: "Check Eligibility",
                    postBack: {
                      data: `ZET_ELIGIBILITY_${leadData._id}`
                    },
                    openUrl: {
                      url: "https://ratecut.com/check-eligibility"
                    }
                  }
                }
              ]
            }
          }
        }
      },
      source: "api"
    };
  }

  /**
   * Send RCS message
   */
  async sendRCS(payload) {
    try {
      const response = await axios.post(this.apiUrl, payload, {
        headers: {
          'accept': 'application/json',
          'Authorization': this.apiKey,
          'Content-Type': 'application/json'
        }
      });

      return {
        success: true,
        status: response.status,
        data: response.data
      };
    } catch (error) {
      console.error('RCS API Error:', error.response?.data || error.message);
      return {
        success: false,
        status: error.response?.status || 500,
        error: error.response?.data || error.message
      };
    }
  }

  /**
   * Queue RCS for immediate or delayed sending
   */
  async queueRCS(leadId, phone, rcsType, lenderName = null, priority = null, delayDays = 0) {
    try {
      const distributionRule = await DistributionRule.findOne({
        source: { $exists: true },
        active: true
      });

      const operatingHours = distributionRule?.rcsConfig?.operatingHours || {
        startTime: '10:00',
        endTime: '19:00'
      };

      let scheduledTime = new Date();

      // Add delay days if specified
      if (delayDays > 0) {
        scheduledTime.setDate(scheduledTime.getDate() + delayDays);
      }

      // Check if within operating hours
      if (!this.isWithinOperatingHours(operatingHours.startTime, operatingHours.endTime)) {
        // Schedule for next business day
        scheduledTime = this.getNextBusinessDayStart(operatingHours.startTime);
      }

      const rcsQueueEntry = new RCSQueue({
        leadId,
        phone,
        rcsType,
        lenderName,
        priority,
        scheduledTime,
        status: 'PENDING'
      });

      await rcsQueueEntry.save();
      console.log(`RCS queued for lead ${leadId}, scheduled at ${scheduledTime}`);

      return rcsQueueEntry;
    } catch (error) {
      console.error('Error queuing RCS:', error);
      throw error;
    }
  }

  /**
   * Process pending RCS messages
   */
  /**
 * Process pending RCS messages with atomic locking
 */
  async processPendingRCS() {
    try {
      const now = new Date();

      // Atomic claim: Find and update in one operation to prevent race conditions
      // This works even with multiple app instances
      const claimedMessages = [];

      // Process messages one at a time with atomic updates
      let hasMore = true;
      while (hasMore && claimedMessages.length < 50) { // Limit batch size
        const message = await RCSQueue.findOneAndUpdate(
          {
            status: 'PENDING',
            scheduledTime: { $lte: now },
            attempts: { $lt: 1 }
          },
          {
            $set: {
              status: 'PROCESSING',
              processingStartedAt: new Date()
            },
            $inc: { attempts: 1 }
          },
          {
            new: true,
            sort: { scheduledTime: 1, priority: 1 } // Process oldest/highest priority first
          }
        ).populate('leadId');

        if (message) {
          claimedMessages.push(message);
        } else {
          hasMore = false;
        }
      }

      console.log(`Claimed ${claimedMessages.length} RCS messages for processing`);

      // Now process the claimed messages
      for (const message of claimedMessages) {
        try {
          let payload;

          if (message.rcsType === 'LENDER_SUCCESS') {
            payload = this.generateLenderSuccessPayload(
              message.phone,
              message.lenderName,
              message.leadId
            );
          } else if (message.rcsType === 'ZET_CAMPAIGN') {
            payload = this.generateZetCampaignPayload(
              message.phone,
              message.leadId
            );
          }

          // Update queue entry with payload
          message.rcsPayload = payload;

          const result = await this.sendRCS(payload);

          if (result.success) {
            message.status = 'SENT';
            message.sentAt = new Date();
            message.rcsResponse = result.data;

            // Log successful RCS
            await this.logRCS(message, payload, result.status, result.data, true);

            console.log(`RCS sent successfully for lead ${message.leadId._id}`);
          } else {
            if (message.attempts >= 3) {
              message.status = 'FAILED';
              message.failureReason = result.error;
            } else {
              // Reset to PENDING for retry
              message.status = 'PENDING';
            }
            message.rcsResponse = result.error;

            // Log failed RCS
            await this.logRCS(message, payload, result.status, result.error, false, result.error);

            console.error(`RCS failed for lead ${message.leadId._id}:`, result.error);
          }

          await message.save();
        } catch (error) {
          console.error(`Error processing RCS for lead ${message.leadId._id}:`, error);

          if (message.attempts >= 3) {
            message.status = 'FAILED';
            message.failureReason = error.message;
          } else {
            // Reset to PENDING for retry
            message.status = 'PENDING';
          }
          await message.save();
        }
      }

      console.log(`Processed ${claimedMessages.length} RCS messages`);

      // Clean up any stuck PROCESSING messages (older than 10 minutes)
      const tenMinutesAgo = new Date(Date.now() - 10 * 60 * 1000);
      await RCSQueue.updateMany(
        {
          status: 'PROCESSING',
          processingStartedAt: { $lt: tenMinutesAgo }
        },
        {
          $set: { status: 'PENDING' }
        }
      );

    } catch (error) {
      console.error('Error processing pending RCS:', error);
    }
  }

  /**
   * Log RCS activity
   */
  async logRCS(queueEntry, payload, status, responseBody, success, errorMessage = null) {
    try {
      const rcsLog = new RCSLog({
        leadId: queueEntry.leadId,
        queueId: queueEntry._id,
        phone: queueEntry.phone,
        rcsType: queueEntry.rcsType,
        lenderName: queueEntry.lenderName,
        requestPayload: payload,
        responseStatus: status,
        responseBody,
        success,
        errorMessage
      });

      await rcsLog.save();
    } catch (error) {
      console.error('Error logging RCS activity:', error);
    }
  }

  /**
   * Schedule RCS based on lender success results
   */
  async scheduleRCSForLead(leadId, successfulLenders) {
    try {
      const lead = await require('../models/leadModel').findById(leadId);
      if (!lead) {
        throw new Error('Lead not found');
      }

      const distributionRule = await DistributionRule.findOne({
        source: lead.source,
        active: true
      });

      if (!distributionRule?.rcsConfig?.enabled) {
        console.log('RCS disabled for source:', lead.source);
        return;
      }

      const { lenderPriority, zetCampaign } = distributionRule.rcsConfig;

      if (successfulLenders.length === 0) {
        // No successful lenders - send ZET campaign
        if (zetCampaign.enabled) {
          await this.queueRCS(
            leadId,
            lead.phone,
            'ZET_CAMPAIGN',
            null,
            null,
            zetCampaign.dayDelay
          );
        }
      } else if (successfulLenders.length === 1) {
        // One successful lender - send RCS for that lender
        const lenderConfig = lenderPriority.find(lp => lp.lender === successfulLenders[0]);
        if (lenderConfig) {
          await this.queueRCS(
            leadId,
            lead.phone,
            'LENDER_SUCCESS',
            lenderConfig.lender,
            1,
            0
          );
        }
      } else {
        // Multiple successful lenders - send RCS for top 2 by priority
        const sortedSuccessfulLenders = successfulLenders
          .map(lender => lenderPriority.find(lp => lp.lender === lender))
          .filter(config => config)
          .sort((a, b) => a.priority - b.priority)
          .slice(0, 2);

        // for (const lenderConfig of sortedSuccessfulLenders) {
        //   await this.queueRCS(
        //     leadId,
        //     lead.phone,
        //     'LENDER_SUCCESS',
        //     lenderConfig.lender,
        //     lenderConfig.priority,
        //     lenderConfig.rcsDayDelay
        //   );
        // }
        // Multiple successful lenders - adjust priorities and delays
        for (let i = 0; i < Math.min(sortedSuccessfulLenders.length, 2); i++) {
          const lenderConfig = sortedSuccessfulLenders[i];

          if (i === 0) {
            // First successful lender becomes P1 - send immediately
            await this.queueRCS(
              leadId,
              lead.phone,
              'LENDER_SUCCESS',
              lenderConfig.lender,
              1,
              0 // Send immediately for P1
            );
          } else if (i === 1) {
            // Second successful lender becomes P2 - use P1's original delay
            const p1OriginalDelay = lenderPriority.find(lp => lp.priority === 1)?.rcsDayDelay || 1;
            await this.queueRCS(
              leadId,
              lead.phone,
              'LENDER_SUCCESS',
              lenderConfig.lender,
              2,
              p1OriginalDelay // Use P1's delay for P2
            );
          }
        }
      }
    } catch (error) {
      console.error('Error scheduling RCS for lead:', error);
      throw error;
    }
  }
}

module.exports = new RCSService();
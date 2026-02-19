const axios = require('axios');
const { RCSQueue } = require('../models/rcsModels');
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
        actionUrl: 'https://portal.getzype.com/?utm_campaign=web_Ratecut'
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
      'FATAKPAYPL': {
        title: 'FatakPay: Get Loan upto ₹2 Lakhs💵',
        description: '✅Interest starting at as low as 1.5%* per month.\n💳EMIs as low as ₹512*.\n🤝 Trusted by over 4 crore Indians.\n🏦  Loans via RBI-registered lenders.\nT&C Apply.',
        imageUrl: 'https://res.cloudinary.com/dha4otbzk/image/upload/v1758734319/fatakpay_giqt2z.jpg',
        actionUrl: 'https://web.fatakpay.com/authentication/login?utm_source=708_8FQLQ&source_caller=api&shortlink=708_8FQLQ&utm_medium='
      },
      'RAMFINCROP': {
        title: 'Loans upto Rs 8 Lakhs in 5 minutes',
        description: '✅Interest starting at as low as 1.5%* per month.\n💳EMIs as low as ₹512*.\n🤝 Trusted by over 4 crore Indians.\n🏦  Loans via RBI-registered lenders.\nT&C Apply.',
        imageUrl: 'https://res.cloudinary.com/dha4otbzk/image/upload/v1764403776/Ramfincorp_wfpetc.jpg',
        actionUrl: 'https://applyonline.ramfincorp.com/?utm_source=RateCut'
      },
      'MPOKKET': {
        title: 'mPokket: Instant loans upto 50k',
        description: 'Mpokket Personal Loan Benefits\n\n1. Get Loan upto Rs 50000\n2. 100% Digital Journey\n3. Students can now enjoy instant loans with ease',
        imageUrl: 'https://res.cloudinary.com/dha4otbzk/image/upload/v1766735469/mpokket_ddvhx2.jpg',
        actionUrl: 'https://web.mpokket.in/?utm_source=ratecut&utm_medium=API'
      },
      'CRMPaisa': {
        title: 'Get up to 15Lakh Instant Loan in 10 min',
        description: '• Quick 10-minute disbursal\n• Lightning-fast approval\n• Collateral-free loans\n• Apply from anywhere, anytime\nT&C Apply.',
        imageUrl: 'https://res.cloudinary.com/dha4otbzk/image/upload/v1768536755/RCS_Emergency_Paisa_o3xvcl.jpg',
        actionUrl: 'https://emergencypaisa.com/apply-now?utm_source=RCut&utm_medium=Medium1&utm_campaign=Medium2'
      },
      'INDIALENDS': {
        title: 'InstantLoans with IndiaLends',
        description: 'How It Works?\n\nProvide Basic Details:\nCheck eligibility for pre-qualified offers from 70+ RBI approved lenders\n\nChoose From Best Offers\nCompare and choose from multiple pre-qualified loan offers tailored for your credit profile\n\nComplete Application Online\nApply for your selected loan offer with a 100% online and digital process\n\nGet Money in Your Bank\nGet the approved loan amount disbursed to your bank account',
        imageUrl: 'https://res.cloudinary.com/dha4otbzk/image/upload/v1769841253/RCS_IL_q5njgw.png',
        actionUrl: 'https://partners.indialends.com/RateCut?sub_source=IND&ref_id=77D908AB-7C2E-4E26'
      },
      'default': {
        title: 'Get Instant Loan up to ₹5 lakhs',
        description: 'Apply for an Instant Loan to fulfil all your financial needs.\n\nWe have exclusive loan offers waiting for you. Check eligibility in 2 minutes!',
        imageUrl: 'https://res.cloudinary.com/dha4otbzk/image/upload/v1770264773/RCS_Poonawalla_fezsxk.png',
        actionUrl: 'https://instant-pocket-loan.poonawallafincorp.com/?utm_DSA_Code=PKA00191&UTM_Partner_Name=ZETAPP&UTM_Partner_Medium=sms&UTM_Partner_AgentCode=PFLZETA&UTM_Partner_ReferenceID=ZPRCLC_1234'
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
              cardTitle: "Get Instant Loan up to ₹5 lakhs",
              cardDescription: "Apply for an Instant Loan to fulfil all your financial needs.\n\nWe have exclusive loan offers waiting for you. Check eligibility in 2 minutes!",
              cardMedia: {
                mediaHeight: "TALL",
                contentInfo: {
                  fileUrl: "https://res.cloudinary.com/dha4otbzk/image/upload/v1770264773/RCS_Poonawalla_fezsxk.png"
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
                      url: "https://instant-pocket-loan.poonawallafincorp.com/?utm_DSA_Code=PKA00191&UTM_Partner_Name=ZETAPP&UTM_Partner_Medium=sms&UTM_Partner_AgentCode=PFLZETA&UTM_Partner_ReferenceID=ZPRCLC_1234"
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
      // Find any active distribution rule
      const allRules = await DistributionRule.findAll();
      const distributionRule = allRules.items.find(rule => rule.active);

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

      const rcsQueueEntry = await RCSQueue.create({
        leadId,
        phone,
        rcsType,
        lenderName,
        priority,
        scheduledTime,
        status: 'PENDING'
      });

      console.log(`RCS queued for lead ${leadId}, scheduled at ${scheduledTime}`);

      return rcsQueueEntry;
    } catch (error) {
      console.error('Error queuing RCS:', error);
      throw error;
    }
  }

  /**
   * Process pending RCS messages with DynamoDB
   * Note: DynamoDB doesn't support atomic findAndModify like MongoDB
   * This implementation processes messages in batches
   */
  async processPendingRCS() {
    try {
      const now = new Date();

      // Get pending messages
      const pendingMessages = await RCSQueue.findByStatusAndScheduledTime(
        'PENDING',
        now,
        { limit: 500 }
      );

      console.log(`Found ${pendingMessages.length} pending RCS messages`);

      // Process messages
      for (const message of pendingMessages) {
        try {
          // Check if attempts limit reached
          if (message.attempts >= 1) {
            continue;
          }

          // Update to PROCESSING status
          await RCSQueue.update(message.rcs_queue, {
            status: 'PROCESSING',
            processingStartedAt: new Date().toISOString(),
            attempts: (message.attempts || 0) + 1
          });

          // Get lead data (you'll need to implement Lead model for DynamoDB)
          const Lead = require('../models/leadModel');
          const lead = await Lead.findById(message.leadId);

          if (!lead) {
            await RCSQueue.update(message.rcs_queue, {
              status: 'FAILED',
              failureReason: 'Lead not found'
            });
            continue;
          }

          let payload;

          if (message.rcsType === 'LENDER_SUCCESS') {
            payload = this.generateLenderSuccessPayload(
              message.phone,
              message.lenderName,
              lead
            );
          } else if (message.rcsType === 'ZET_CAMPAIGN') {
            payload = this.generateZetCampaignPayload(
              message.phone,
              lead
            );
          }

          const result = await this.sendRCS(payload);

          if (result.success) {
            await RCSQueue.update(message.rcs_queue, {
              status: 'SENT',
              sentAt: new Date().toISOString(),
              rcsPayload: payload,
              rcsResponse: result.data
            });

            console.log(`RCS sent successfully for lead ${message.leadId}`);
          } else {
            const updateData = {
              rcsPayload: payload,
              rcsResponse: result.error
            };

            if (message.attempts >= 1) {
              updateData.status = 'FAILED';
              updateData.failureReason = JSON.stringify(result.error);
            } else {
              updateData.status = 'PENDING';
            }

            await RCSQueue.update(message.rcs_queue, updateData);

            console.error(`RCS failed for lead ${message.leadId}:`, result.error);
          }
        } catch (error) {
          console.error(`Error processing RCS for message ${message.rcs_queue}:`, error);

          const updateData = {};
          if (message.attempts >= 1) {
            updateData.status = 'FAILED';
            updateData.failureReason = error.message;
          } else {
            updateData.status = 'PENDING';
          }

          await RCSQueue.update(message.rcs_queue, updateData);
        }
      }

      console.log(`Processed ${pendingMessages.length} RCS messages`);

      // Clean up stuck PROCESSING messages (older than 10 minutes)
      // Note: This requires a scan operation in DynamoDB
      const tenMinutesAgo = new Date(Date.now() - 10 * 60 * 1000).toISOString();
      const allMessages = await RCSQueue.findAll({ limit: 1000 });
      
      for (const message of allMessages.items) {
        if (
          message.status === 'PROCESSING' && 
          message.processingStartedAt && 
          message.processingStartedAt < tenMinutesAgo
        ) {
          await RCSQueue.update(message.rcs_queue, {
            status: 'PENDING'
          });
        }
      }

    } catch (error) {
      console.error('Error processing pending RCS:', error);
    }
  }

  /**
   * Schedule RCS based on lender success results
   */
  async scheduleRCSForLead(leadId, successfulLenders) {
    try {
      const Lead = require('../models/leadModel');
      const lead = await Lead.findById(leadId);
      
      if (!lead) {
        throw new Error('Lead not found');
      }

      const distributionRule = await DistributionRule.findActiveBySource(lead.source);

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
            0,
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
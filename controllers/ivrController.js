const IvrCall = require('../models/ivrModel');
const axios = require('axios');

// RCS Configuration - Update these with your actual credentials
const RCS_CONFIG = {
    apiUrl: process.env.RCS_API_URL || 'https://rcs-api-endpoint.com/send',
    apiKey: process.env.RCS_API_KEY || 'your-rcs-api-key',
    senderId: process.env.RCS_SENDER_ID || 'your-sender-id'
};

// Lender RCS Message Templates
const LENDER_MESSAGES = {
    '1': {
        lenderName: 'HDFC Bank',
        message: 'Thank you for your interest in HDFC Bank Personal Loan. Our representative will contact you shortly. Visit: https://hdfc.com/loans',
        rcsTemplate: {
            title: 'HDFC Personal Loan',
            description: 'Get instant approval on personal loans up to ₹40 Lakhs at attractive interest rates.',
            imageUrl: 'https://example.com/hdfc-banner.jpg',
            action: {
                text: 'Apply Now',
                url: 'https://hdfc.com/apply'
            }
        }
    },
    '2': {
        lenderName: 'ICICI Bank',
        message: 'ICICI Bank offers you pre-approved personal loans. Check your eligibility now!',
        rcsTemplate: {
            title: 'ICICI Personal Loan',
            description: 'Pre-approved loans with minimal documentation. Apply in minutes!',
            imageUrl: 'https://example.com/icici-banner.jpg',
            action: {
                text: 'Check Eligibility',
                url: 'https://icici.com/check'
            }
        }
    },
    '3': {
        lenderName: 'Bajaj Finserv',
        message: 'Bajaj Finserv Personal Loan - Quick disbursal, flexible tenure. Apply today!',
        rcsTemplate: {
            title: 'Bajaj Finserv Loan',
            description: 'Get funds in 24 hours with flexible repayment options.',
            imageUrl: 'https://example.com/bajaj-banner.jpg',
            action: {
                text: 'Apply Online',
                url: 'https://bajajfinserv.com/apply'
            }
        }
    }
};

/**
 * Handle IVR Webhook - Main endpoint to receive IVR data
 */
const handleIvrWebhook = async (req, res) => {
    try {
        const ivrData = req.body;
        console.log(ivrData);
        // Validate required fields
        if (!ivrData.call_id || !ivrData.call_to_number || !ivrData.digit_pressed) {
            return res.status(400).json({
                success: false,
                message: 'Missing required fields: callId, phoneNumber, or digitPressed'
            });
        }

        console.log('IVR Webhook Received:', {
            callId: ivrData.call_id,
            phone: ivrData.call_to_number,
            digit: ivrData.digit_pressed
        });

        // Check if call already exists
        const existingCall = await IvrCall.findOne({ callId: ivrData.call_id });
        if (existingCall) {
            return res.status(200).json({
                success: true,
                message: 'Call already processed',
                data: existingCall
            });
        }

        // Create IVR call record
        const ivrCall = new IvrCall({
            uuid: ivrData.uuid,
            phoneNumber: ivrData.call_to_number,
            ourNumber: ivrData.caller_id_number,
            digitPressed: ivrData.digit_pressed,
            callId: ivrData.call_id,
            billingCircle: ivrData.billing_circle,
            phoneNumberWithPrefix: ivrData.customer_no_with_prefix,
            rawIvrData: ivrData,
        });

        await ivrCall.save();

        // If digit pressed is 1, send RCS message
        // if (ivrData.digit_pressed === '1') {
        //     const rcsSent = await sendRcsMessage(ivrCall);

        //     return res.status(200).json({
        //         success: true,
        //         message: 'IVR data processed and RCS message sent',
        //         data: {
        //             callId: ivrCall.callId,
        //             digitPressed: ivrCall.digitPressed,
        //             rcsMessageSent: rcsSent.success,
        //             rcsMessageId: rcsSent.messageId
        //         }
        //     });
        // }

        // For other digits, just acknowledge
        return res.status(200).json({
            success: true,
            message: 'IVR data processed successfully',
            data: {
                callId: ivrCall.callId,
                digitPressed: ivrCall.digitPressed,
                rcsMessageSent: false
            }
        });

    } catch (error) {
        console.error('Error handling IVR webhook:', error);
        return res.status(500).json({
            success: false,
            message: 'Error processing IVR data',
            error: error.message
        });
    }
};

/**
 * Send RCS Message to user
 */
const sendRcsMessage = async (ivrCall) => {
    try {
        const lenderData = LENDER_MESSAGES[ivrCall.digitPressed];

        if (!lenderData) {
            console.log(`No lender data found for digit: ${ivrCall.digitPressed}`);
            return { success: false, messageId: null };
        }

        // Prepare RCS message payload
        const rcsPayload = {
            to: ivrCall.phoneNumber,
            from: RCS_CONFIG.senderId,
            messageType: 'rich_card',
            content: {
                title: lenderData.rcsTemplate.title,
                description: lenderData.rcsTemplate.description,
                media: {
                    url: lenderData.rcsTemplate.imageUrl,
                    contentType: 'image/jpeg'
                },
                suggestions: [
                    {
                        action: {
                            type: 'openUrl',
                            url: lenderData.rcsTemplate.action.url,
                            label: lenderData.rcsTemplate.action.text
                        }
                    },
                    {
                        reply: {
                            text: 'Call Me',
                            postbackData: `call_back_${ivrCall.callId}`
                        }
                    }
                ]
            }
        };

        // Send RCS message via API
        const response = await axios.post(
            RCS_CONFIG.apiUrl,
            rcsPayload,
            {
                headers: {
                    'Authorization': `Bearer ${RCS_CONFIG.apiKey}`,
                    'Content-Type': 'application/json'
                },
                timeout: 10000
            }
        );

        // Update IVR call record with RCS info
        ivrCall.rcsMessageSent = true;
        ivrCall.rcsMessageId = response.data.messageId || 'rcs-msg-' + Date.now();
        await ivrCall.save();

        console.log('RCS message sent successfully:', {
            callId: ivrCall.callId,
            messageId: ivrCall.rcsMessageId
        });

        return {
            success: true,
            messageId: ivrCall.rcsMessageId
        };

    } catch (error) {
        console.error('Error sending RCS message:', error.message);

        // Update call record to indicate RCS failure
        ivrCall.rcsMessageSent = false;
        await ivrCall.save();

        return {
            success: false,
            messageId: null,
            error: error.message
        };
    }
};

/**
 * Get IVR call details by callId
 */
const getIvrCallDetails = async (req, res) => {
    try {
        const { callId } = req.params;

        const ivrCall = await IvrCall.findOne({ callId });

        if (!ivrCall) {
            return res.status(404).json({
                success: false,
                message: 'IVR call not found'
            });
        }

        return res.status(200).json({
            success: true,
            data: ivrCall
        });

    } catch (error) {
        console.error('Error fetching IVR call:', error);
        return res.status(500).json({
            success: false,
            message: 'Error fetching IVR call details',
            error: error.message
        });
    }
};

/**
 * Get all IVR calls with filters
 */
const getAllIvrCalls = async (req, res) => {
    try {
        const {
            page = 1,
            limit = 20,
            digitPressed,
            phoneNumber,
            startDate,
            endDate
        } = req.query;

        const query = {};

        if (digitPressed) query.digitPressed = digitPressed;
        if (phoneNumber) query.phoneNumber = phoneNumber;

        if (startDate || endDate) {
            query.createdAt = {};
            if (startDate) query.createdAt.$gte = new Date(startDate);
            if (endDate) query.createdAt.$lte = new Date(endDate);
        }

        const skip = (parseInt(page) - 1) * parseInt(limit);

        const [calls, total] = await Promise.all([
            IvrCall.find(query)
                .sort({ createdAt: -1 })
                .skip(skip)
                .limit(parseInt(limit))
                .lean(),
            IvrCall.countDocuments(query)
        ]);

        return res.status(200).json({
            success: true,
            data: {
                calls,
                pagination: {
                    total,
                    page: parseInt(page),
                    limit: parseInt(limit),
                    totalPages: Math.ceil(total / parseInt(limit))
                }
            }
        });

    } catch (error) {
        console.error('Error fetching IVR calls:', error);
        return res.status(500).json({
            success: false,
            message: 'Error fetching IVR calls',
            error: error.message
        });
    }
};

/**
 * Get IVR analytics
 */
const getIvrAnalytics = async (req, res) => {
    try {
        const analytics = await IvrCall.aggregate([
            {
                $group: {
                    _id: '$digitPressed',
                    count: { $sum: 1 },
                    rcsMessagesSent: {
                        $sum: { $cond: ['$rcsMessageSent', 1, 0] }
                    }
                }
            },
            {
                $sort: { _id: 1 }
            }
        ]);

        const totalCalls = await IvrCall.countDocuments();
        const totalRcsSent = await IvrCall.countDocuments({ rcsMessageSent: true });

        return res.status(200).json({
            success: true,
            data: {
                totalCalls,
                totalRcsSent,
                digitWiseStats: analytics
            }
        });

    } catch (error) {
        console.error('Error fetching analytics:', error);
        return res.status(500).json({
            success: false,
            message: 'Error fetching analytics',
            error: error.message
        });
    }
};

module.exports = {
    handleIvrWebhook,
    getIvrCallDetails,
    getAllIvrCalls,
    getIvrAnalytics
};
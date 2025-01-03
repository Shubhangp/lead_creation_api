const Lead = require('../models/leadModel');
const smlResponseLog = require('../models/smlResponseLogModel');
const freoResponseLog = require('../models/freoResponseLogModel');
const APIFeatures = require('../utils/apiFeatures');
const axios = require('axios');

// Helper Function to get a random residenceType
const getRandomResidenceType = () => {
  const residenceTypes = ['OWNED', 'RENT', 'LEASE'];
  return residenceTypes[Math.floor(Math.random() * residenceTypes.length)];
};

// Helper function to get an access token
const getAccessToken = async () => {
  const baseUrl = `${process.env.DEV_URL}/oauth/token?grant_type=client_credentials`;
  const username = process.env.FREO_NAME;
  const password = process.env.FREO_KEY;
  const authHeader = Buffer.from(`${username}:${password}`).toString('base64');

  try {
    const response = await axios.post(
      `${baseUrl}`,
      {},
      {
        headers: {
          Accept: 'application/json',
          Authorization: `Basic ${authHeader}`,
        },
      }
    );
    return response.data.access_token;
  } catch (error) {
    console.error('Error fetching access token:', error.response?.data || error.message);
    throw new Error('Failed to generate access token');
  }
};

// Create a lead
exports.createLead = async (req, res) => {
  const { source, fullName, firstName, lastName, phone, email, age, dateOfBirth, gender, panNumber, jobType, businessType, salary, creditScore, address, pincode, consent, } = req.body;

  // Input validation
  if (!source || !fullName || !phone || !email || !panNumber || consent === undefined) {
    return res.status(400).json({ message: 'Source, fullName, phone, email, panNumber, and consent are required.' });
  }

  // Full Name validation
  if (fullName.length < 2 || fullName.length > 100) {
    return res.status(400).json({ message: 'Full name must be between 2 and 100 characters.' });
  }

  // Email validation
  const emailRegex = /[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,4}$/;
  if (!emailRegex.test(email)) {
    return res.status(400).json({ message: 'Invalid email format.' });
  }

  // PAN number validation
  const panRegex = /^[A-Z]{5}[0-9]{4}[A-Z]$/;
  if (!panRegex.test(panNumber)) {
    return res.status(400).json({ message: 'Invalid PAN number format. Must match ABCDE1234F.' });
  }

  // Date of Birth validation
  const isValidDate = /^\d{4}-\d{2}-\d{2}$/.test(dateOfBirth) && !isNaN(Date.parse(dateOfBirth));
  if (dateOfBirth && (!isValidDate || new Date(dateOfBirth) > new Date())) {
    return res.status(400).json({ message: 'Invalid date of birth or date cannot be in the future.' });
  }

  // Conditional defaults for salary and jobType
  const finalSalary = salary || '50000';
  const finalJobType = jobType || 'SALARIED';

  try {
    // Create and save the lead
    const lead = new Lead({
      source, fullName, firstName, lastName, phone, email,
      age,
      dateOfBirth,
      gender,
      panNumber,
      jobType: finalJobType,
      businessType,
      salary: finalSalary,
      creditScore,
      address,
      pincode,
      consent,
    });

    const savedLead = await lead.save();

    // If source is not "SML", send lead to external API
    if (source !== 'SML') {
      const vendorName = process.env.VENDOR_NAME_SML;
      const apiKey = process.env.API_KEY_SML;
      const externalApiUrl = `https://nucleus.switchmyloan.in/vendor/${vendorName}/createLead`;

      const payload = {
        name: fullName,
        phone,
        email,
        panNumber,
        dob: dateOfBirth,
        gender,
        salary: `${finalSalary}`,
        pincode,
        jobType: finalJobType,
      };

      try {
        const apiResponse = await axios.post(externalApiUrl, payload, {
          headers: {
            'x-api-key': apiKey,
            'Content-Type': 'application/json',
          },
        });

        // Save API response to the new collection
        const responseLog = new smlResponseLog({
          leadId: savedLead._id,
          requestPayload: payload,
          responseStatus: apiResponse.status,
          responseBody: apiResponse.data,
        });

        await responseLog.save();
      } catch (error) {
        console.error('Error sending lead to external API:', error.message);
        await smlResponseLog.create({
          leadId: savedLead._id,
          requestPayload: payload,
          responseStatus: error.response?.status || 500,
          responseBody: error.response?.data || { message: 'Unknown error' },
        });
      }
    }

    // If source is not "Freo", send lead to external API
    if (source !== 'Freo') {
      const baseUrl = process.env.DEV_URL;
      const accessToken = await getAccessToken();

      // Construct payload for MoneyTap API
      const payload = {
        emailId: email,
        phone,
        name: fullName,
        panNumber,
        dateOfBirth,
        gender,
        jobType: finalJobType,
        homeAddress: {
          addressLine1: address || '',
          pincode,
        },
        residenceType: getRandomResidenceType(),
        officeAddress: {
          addressLine1: address || '',
          pincode,
        },
        incomeInfo: {
          declared: finalSalary
        },
      };

      try {
        const apiResponse = await axios.post(
          `${baseUrl}/v3/partner/lead/create`,
          payload,
          {
            headers: {
              Accept: 'application/json',
              'Content-Type': 'application/json',
              Authorization: `Bearer ${accessToken}`,
            },
          }
        );

        // Save API response to the new collection
        const responseLog = new freoResponseLog({
          leadId: savedLead._id,
          requestPayload: payload,
          responseStatus: apiResponse.status,
          responseBody: apiResponse.data,
        });

        await responseLog.save();
      } catch (error) {
        console.error('Error sending lead to MoneyTap API:', error.message);
        await freoResponseLog.create({
          leadId: savedLead._id,
          requestPayload: payload,
          responseStatus: error.response?.status || 500,
          responseBody: error.response?.data || { message: 'Unknown error' },
        });
      }
    }

    res.status(201).json({
      status: 'success',
      data: {
        lead: savedLead,
      },
    });
  } catch (error) {
    if (error.code === 11000) {
      return res.status(409).json({ message: 'Duplicate PAN number' });
    }
    res.status(500).json({ message: 'Server error', error: error.message });
  }
};

// Fetch all leads
exports.getLeads = async (req, res) => {
  try {
    // excute query
    const features = new APIFeatures(Lead.find(), req.query)
    .filter()
    .sort()
    .limitFields()
    .paginate();

    const leads = await features.query;

    res.status(200).json({
      status: 'success',
      results: leads.length,
      data: {
          leads
      }
    });
  } catch (error) {
    res.status(500).json({ message: 'Server error', error: error.message });
  }
};

// Fetch lead by ID
exports.getLeadById = async (req, res) => {
  const { id } = req.params;

  try {
    const lead = await Lead.findById(id);
    if (!lead) {
      return res.status(404).json({ message: 'Lead not found' });
    }
    res.status(200).json(lead);
  } catch (error) {
    res.status(500).json({ message: 'Server error', error: error.message });
  }
};
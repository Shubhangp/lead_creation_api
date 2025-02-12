const Lead = require('../models/leadModel');
const smlResponseLog = require('../models/smlResponseLogModel');
const freoResponseLog = require('../models/freoResponseLogModel');
const APIFeatures = require('../utils/apiFeatures');
const axios = require('axios');
const ovlyResponseLog = require('../models/ovlyResponseLog');
const leadUAT = require('../models/leadUATModel');

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
    if (source !== 'FREO') {
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

    // If source is not "OVLY", send lead to external API
    if (source !== 'OVLY') {
      console.log("CALL OVLY");      
      const dedupApiUrl = 'https://leads.smartcoin.co.in/partner/ratecut/lead/dedup';
      const createLeadApiUrl = 'https://leads.smartcoin.co.in/partner/ratecut/lead/create';
      const clientId = process.env.OVLY_CLIENT_ID;
      const clientKey = process.env.OVLY_CLIENT_KEY;

      const dedupPayload = new URLSearchParams({
        phone_number: phone,
        pan: panNumber,
        date_of_birth: dateOfBirth,
        employement_type: finalJobType,
        net_monthly_income: `${finalSalary}`,
        name_as_per_pan: fullName,
      });

      try {
        const dedupResponse = await axios.post(dedupApiUrl, dedupPayload, {
          headers: {
            'admin-api-client-id': clientId,
            'admin-api-client-key': clientKey,
            'Content-Type': 'application/x-www-form-urlencoded',
          },
        });

        const dedupData = dedupResponse.data;
        
        // If lead is fresh (not a duplicate), push to OVLY Lead Create API
        if (dedupData.isDuplicateLead === "false" && dedupData.status === "success") {
          const createLeadPayload = new URLSearchParams({
            phone_number: phone,
            pan: panNumber,
            email,
            employement_type: finalJobType,
            net_monthly_income: `${finalSalary}`,
            mode_of_salary: 'ONLINE',
            bank_name: 'HDFC',
            name_as_per_pan: fullName,
            current_residence_pin_code: pincode,
            date_of_birth: dateOfBirth,
            gender,
          });

          const leadResponse = await axios.post(createLeadApiUrl, createLeadPayload, {
            headers: {
              'admin-api-client-id': clientId,
              'admin-api-client-key': clientKey,
              'Content-Type': 'application/x-www-form-urlencoded',
            },
          });

          console.log('Lead successfully pushed:', leadResponse.data);
          // Save lead response in DB
          const ovlyLeadLog = new ovlyResponseLog({
            leadId: savedLead._id,
            requestPayload: createLeadPayload,
            responseStatus: leadResponse.data.status,
            responseBody: leadResponse.data,
          });

          await ovlyLeadLog.save();

        } else if(dedupData.isDuplicateLead === "true" && dedupData.status === "success") {
          const createLeadPayload = {
            phone_number: phone,
            pan: panNumber,
            email,
            employement_type: finalJobType,
            net_monthly_income: `${finalSalary}`,
            mode_of_salary: 'ONLINE',
            bank_name: 'HDFC',
            name_as_per_pan: fullName,
            current_residence_pin_code: pincode,
            date_of_birth: dateOfBirth,
            gender,
          };

          const ovlyLeadLog = new ovlyResponseLog({
            leadId: savedLead._id,
            requestPayload: createLeadPayload,
            responseStatus: leadResponse.data.status,
            responseBody: leadResponse.data,
          });

          await ovlyLeadLog.save();
        }
      } catch (error) {
        console.error('Error in OVLY API integration:', error.response?.data || error.message);
        await ovlyResponseLog.create({
          leadId: savedLead._id,
          requestPayload: dedupPayload,
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

// Create a lead
exports.createUATLead = async (req, res) => {
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
    const lead = new leadUAT({
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


exports.sendLeadToOvly = async (req, res) => {
  try {
    const {leadId} = req.body;
    const lead = await Lead.findById(leadId);

    if (!lead) {
      console.log(`Lead not found: ${leadId}`);
      return;
    }

    if (lead.source !== 'OVLY') {
      console.log(`Processing lead: ${lead._id}`);

      const dedupApiUrl = 'https://leads.smartcoin.co.in/partner/ratecut/lead/dedup';
      const createLeadApiUrl = 'https://leads.smartcoin.co.in/partner/ratecut/lead/create';
      const clientId = process.env.OVLY_CLIENT_ID;
      const clientKey = process.env.OVLY_CLIENT_KEY;

      const dedupPayload = new URLSearchParams({
        phone_number: lead.phone,
        pan: lead.panNumber,
        date_of_birth: lead.dateOfBirth,
        employement_type: lead.jobType,
        net_monthly_income: lead.salary,
        name_as_per_pan: lead.fullName,
      });

      try {
        const dedupResponse = await axios.post(dedupApiUrl, dedupPayload, {
          headers: {
            'admin-api-client-id': clientId,
            'admin-api-client-key': clientKey,
            'Content-Type': 'application/x-www-form-urlencoded',
          },
        });

        const dedupData = dedupResponse.data;

        if (dedupData.isDuplicateLead === "false" && dedupData.status === "success") {
          const createLeadPayload = new URLSearchParams({
            phone_number: lead.phone,
            pan: lead.panNumber,
            email: lead.email,
            employement_type: lead.jobType,
            net_monthly_income: lead.salary,
            mode_of_salary: 'ONLINE',
            bank_name: 'HDFC',
            name_as_per_pan: lead.fullName,
            current_residence_pin_code: lead.pincode,
            date_of_birth: lead.dateOfBirth,
            gender: lead.gender,
          });

          const leadResponse = await axios.post(createLeadApiUrl, createLeadPayload, {
            headers: {
              'admin-api-client-id': clientId,
              'admin-api-client-key': clientKey,
              'Content-Type': 'application/x-www-form-urlencoded',
            },
          });

          console.log('Lead successfully pushed:', leadResponse.data);

          // Save lead response in DB
          await ovlyResponseLog.create({
            leadId: lead._id,
            requestPayload: createLeadPayload,
            responseStatus: leadResponse.data.status,
            responseBody: leadResponse.data,
          });

        } else if (dedupData.isDuplicateLead === "true" && dedupData.status === "success") {
          await ovlyResponseLog.create({
            leadId: lead._id,
            requestPayload: dedupPayload,
            responseStatus: dedupData.status,
            responseBody: dedupData,
          });
        }
      } catch (error) {
        console.error(`Error processing lead ${lead._id}:`, error.response?.data || error.message);
        await ovlyResponseLog.create({
          leadId: lead._id,
          requestPayload: dedupPayload,
          responseStatus: error.response?.status || 500,
          responseBody: error.response?.data || { message: 'Unknown error' },
        });
      }
    }
  } catch (error) {
    console.error('Error fetching lead:', error);
  }
};

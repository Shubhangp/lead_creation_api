const Lead = require('../models/leadModel');
const { readFile, deleteFile } = require("../utils/readFile");
const smlResponseLog = require('../models/smlResponseLogModel');
const freoResponseLog = require('../models/freoResponseLogModel');
const APIFeatures = require('../utils/apiFeatures');
const axios = require('axios');
const ovlyResponseLog = require('../models/ovlyResponseLog');
const leadUAT = require('../models/leadUATModel');
const LeadingPlateResponseLog = require('../models/leadingPlateResponseLog');
const FintifiResponseLog = require('../models/fintifiResponseLog');
const ZypeResponseLog = require('../models/ZypeResponseLogModel');
const fatakPayResponseLog = require('../models/fatakPayResponseLog');
const ramFinCropLog = require('../models/ramFinCropLogModel');
const vrindaLog = require('../models/VrindaFintechResponseLog');
const DistributionRule = require('../models/distributionRuleModel');
const { sendLeadsToLender } = require('../utils/sendLeads');
const xlsx = require('xlsx');
const path = require('path');
const mmmResponseLog = require('../models/mmmResponseLog');


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

const checkMobileExists = async (phone) => {
  const url = 'https://lms.lendingplate.co.in/api/Api/affiliateApi/checkmobile';
  const headers = {
    'Authorization': `Bearer ${process.env.LP_TOKEN}`,
    'Content-Type': 'application/json'
  }
  const checkPaayload = {
    partner_id: process.env.LP_PARTNER_ID,
    ref_id: phone,
    mobile: phone
  }
  try {
    const response = await axios.post(url, checkPaayload, { headers });

    return response.data.status === 'S';
  } catch (error) {
    console.error('Error in mobile check API:', error.response?.data || error.message);
    return false;
  }
};

const processLoanApplication = async (payload) => {
  const url = 'https://lms.lendingplate.co.in/api/Api/affiliateApi/loanprocess';
  const headers = {
    'Authorization': `Bearer ${process.env.LP_TOKEN}`,
    'Content-Type': 'application/json'
  }
  try {
    const response = await axios.post(url, payload, { headers });
    console.log(response.data);
    return response.data;
  } catch (error) {
    console.error('Error in loan process API:', error.response?.data || error.message);
    return false;
  }
};

const checkZypeEligibility = async (mobileNumber, panNumber) => {
  try {
    const response = await axios.post(
      "https://prod.zype.co.in/attribution-service/api/v1/underwriting/customerEligibility",
      {
        mobileNumber,
        panNumber,
        partnerId: process.env.ZYPE_PARTNER_ID,
      },
      {
        headers: { "Content-Type": "application/json" },
      }
    );
    return response.data
  } catch (error) {
    console.error("ZYPE Eligibility Check Failed:", error.response?.data || error.message);
    return false;
  }
}

const processZypeApplication = async (payload) => {
  try {

    const response = await axios.post(
      "https://prod.zype.co.in/attribution-service/api/v1/underwriting/preApprovalOffer",
      payload,
      {
        headers: { "Content-Type": "application/json" },
      }
    );
    // console.log(response);

    return response.data;
  } catch (error) {
    console.error("Error sending lead to ZYPE:", error.response?.data || error.message);
    return { status: "Failed", message: "ZYPE API Error" };
  }
}

const formatDate = (dateString) => {
  const date = new Date(dateString);
  return date.toLocaleDateString('en-GB');
};

const formatDatewithdash = (dateString) => {
  const date = new Date(dateString);
  const day = String(date.getDate()).padStart(2, '0');
  const month = String(date.getMonth() + 1).padStart(2, '0');
  const year = date.getFullYear();

  return `${day}-${month}-${year}`;
};

function formatToYYYYMMDD(dateString) {
  const date = new Date(dateString);
  return date.toISOString().split('T')[0];
}

function readExcelFile() {
  const workbook = xlsx.readFile(path.join(__dirname, './FatakPay_PL_Serviceable_pincode_list.xlsx'));
  const sheetName = workbook.SheetNames[1];
  return xlsx.utils.sheet_to_json(workbook.Sheets[sheetName]);
}

const pinCodeData = readExcelFile();


// Create a lead
exports.createLead = async (req, res) => {
  const {
    source, fullName, firstName, lastName, phone, email,
    age, dateOfBirth, gender, panNumber, jobType, businessType,
    salary, creditScore, cibilScore, address, pincode, consent
  } = req.body;

  if (!source || !fullName || !phone || !email || !panNumber || consent === undefined) {
    return res.status(400).json({ message: 'Source, fullName, phone, email, panNumber, and consent are required.' });
  }

  if (fullName.length < 2 || fullName.length > 100) {
    return res.status(400).json({ message: 'Full name must be between 2 and 100 characters.' });
  }

  const emailRegex = /[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,4}$/;
  if (!emailRegex.test(email)) {
    return res.status(400).json({ message: 'Invalid email format.' });
  }

  const panRegex = /^[A-Z]{5}[0-9]{4}[A-Z]$/;
  if (!panRegex.test(panNumber)) {
    return res.status(400).json({ message: 'Invalid PAN number format. Must match ABCDE1234F.' });
  }

  const isValidDate = /^\d{4}-\d{2}-\d{2}$/.test(dateOfBirth) && !isNaN(Date.parse(dateOfBirth));
  if (dateOfBirth && (!isValidDate || new Date(dateOfBirth) > new Date())) {
    return res.status(400).json({ message: 'Invalid date of birth or date cannot be in the future.' });
  }

  const finalSalary = salary || '50000';
  const finalJobType = jobType || 'SALARIED';

  try {
    const lead = new Lead({
      source, fullName, firstName, lastName, phone, email,
      age, dateOfBirth, gender, panNumber, jobType: finalJobType,
      businessType, salary: finalSalary, creditScore, cibilScore,
      address, pincode, consent,
    });

    const savedLead = await lead.save();

    const distributionRules = await getDistributionRules(source);

    await processLenders(savedLead, distributionRules.immediate, 'immediate');

    scheduleDelayedLenders(savedLead, distributionRules.delayed);

    res.status(201).json({
      status: 'success',
      data: {
        lead: savedLead,
      },
    });
  } catch (error) {
    console.log(error);
    if (error.code === 11000) {
      return res.status(409).json({ message: 'Duplicate PAN number' });
    }
    res.status(500).json({ message: 'Server error', error: error.message });
  }
};

// Process lenders based on group (immediate or delayed)
async function processLenders(lead, lenders, type) {
  for (const lender of lenders) {
    const lenderName = typeof lender === 'string' ? lender : lender.lender;

    // Don't send to the same lender that created the lead
    if (lenderName !== lead.source) {
      try {
        await sendToLender(lead, lenderName);
        // Log successful distribution
        console.log(`Lead ${lead._id} sent to ${lenderName} (${type})`);
      } catch (error) {
        console.error(`Error sending lead to ${lenderName}:`, error.message);
      }
    }
  }
}

// Schedule delayed lenders using a job queue
function scheduleDelayedLenders(lead, delayedLenders) {
  for (const lenderConfig of delayedLenders) {
    if (lenderConfig.lender !== lead.source) {
      const delayMs = lenderConfig.delayMinutes * 60 * 1000;

      setTimeout(async () => {
        try {
          await sendToLender(lead, lenderConfig.lender);
          console.log(`Delayed lead ${lead._id} sent to ${lenderConfig.lender} after ${lenderConfig.delayMinutes} minutes`);
        } catch (error) {
          console.error(`Error sending delayed lead to ${lenderConfig.lender}:`, error.message);
        }
      }, delayMs);

      // Log scheduled job
      console.log(`Scheduled lead ${lead._id} to be sent to ${lenderConfig.lender} after ${lenderConfig.delayMinutes} minutes`);
    }
  }
}

// Send lead to specific lender
async function sendToLender(lead, lender) {

  // Create handler map for all lenders
  const lenderHandlers = {
    'SML': sendToSML,
    'FREO': sendToFreo,
    'OVLY': sendToOVLY,
    'LendingPlate': sendToLendingPlate,
    'ZYPE': sendToZYPE,
    'FINTIFI': sendToFINTIFI,
    'FATAKPAY': sendToFATAKPAY,
    'RAMFINCROP': sendToRAMFINCROP,
    "MyMoneyMantra": sendToMyMoneyMantra
  };

  // Call the appropriate handler for the lender
  if (lenderHandlers[lender]) {
    return await lenderHandlers[lender](lead);
  } else {
    throw new Error(`No handler found for lender: ${lender}`);
  }
}

async function getDistributionRules(source) {
  try {
    const dbRules = await DistributionRule.findOne({ source, active: true });
    console.log(dbRules);

    if (dbRules) {
      return dbRules.rules;
    }

    const defaultRules = {
      FREO: {
        immediate: ['ZYPE', 'OVLY', 'LendingPlate', 'FATAKPAY', 'MyMoneyMantra'],
        delayed: [
          { lender: 'SML', delayMinutes: 1440 }
        ]
      },
      MyMoneyMantra: {
        immediate: ['ZYPE', 'OVLY', 'LendingPlate', 'FATAKPAY',],
        delayed: [
          { lender: 'SML', delayMinutes: 1440 }
        ]
      },
      SML: {
        immediate: ['FREO', 'OVLY', 'MyMoneyMantra'],
        delayed: [
          { lender: 'LendingPlate', delayMinutes: 1 },
          { lender: 'ZYPE', delayMinutes: 1 },
          { lender: 'FINTIFI', delayMinutes: 1 },
          { lender: 'RAMFINCROP', delayMinutes: 1 },
          { lender: 'FATAKPAY', delayMinutes: 1 }
        ]
      },
      OVLY: {
        immediate: ['FREO', 'SML', 'MyMoneyMantra'],
        delayed: [
          { lender: 'LendingPlate', delayMinutes: 1 },
          { lender: 'ZYPE', delayMinutes: 1 },
          { lender: 'FINTIFI', delayMinutes: 1 },
          { lender: 'RAMFINCROP', delayMinutes: 1 },
          { lender: 'FATAKPAY', delayMinutes: 1 }
        ]
      },
      default: {
        immediate: ['FREO', 'SML', 'OVLY', 'MyMoneyMantra'],
        delayed: [
          { lender: 'LendingPlate', delayMinutes: 1 },
          { lender: 'ZYPE', delayMinutes: 1 },
          { lender: 'FINTIFI', delayMinutes: 1 },
          { lender: 'RAMFINCROP', delayMinutes: 1 },
          { lender: 'FATAKPAY', delayMinutes: 1 }
        ]
      }
    };

    return defaultRules[source] || defaultRules.default;
  } catch (error) {
    console.error('Error fetching distribution rules:', error);

    return {
      immediate: ['ZYPE', 'OVLY', 'LendingPlate', 'FATAKPAY', 'RAMFINCROP', 'MyMoneyMantra'],
      delayed: [
        { lender: 'SML', delayMinutes: 1440 },
        { lender: 'FINTIFI', delayMinutes: 1440 }
      ]
    };
  }
}

// Individual lender handlers
async function sendToSML(lead) {
  const {
    _id, fullName, phone, email, dateOfBirth,
    gender, panNumber, jobType, salary, pincode, source
  } = lead;

  const externalApiUrl = `https://dedupe.switchmyloan.in/api/method/lead_management.custom_method.create_lead_entry`;

  const payload = {
    mobile_number: String(phone),
    first_name: fullName.split(' ')[0],
    last_name: fullName.split(' ')[1] ? fullName.split(' ')[1] : fullName.split(' ')[0],
    gender: String(gender),
    pan_number: String(panNumber),
    dob: formatToYYYYMMDD(dateOfBirth),
    net_monthly_income: String(salary),
    email: String(email),
    pin_code: String(pincode),
    profession: String(jobType),
    channel_partner: 'Ratecut',
  };

  try {
    const apiResponse = await axios.post(externalApiUrl, payload, {
      headers: {
        'Content-Type': 'application/json',
        'Authorization': 'Basic NzYwNmE3ODI2M2RmNGY1OmMwNDUxNWY4OTBiNjhhNQ==',
        'Cookie': 'full_name=Guest; sid=Guest; system_user=no; user_id=Guest; user_image='
      },
    });

    console.log("SML response:", apiResponse.data);

    // Save API response to the new collection
    const responseLog = new smlResponseLog({
      leadId: _id,
      source: source,
      requestPayload: payload,
      responseStatus: apiResponse.status,
      responseBody: apiResponse.data,
    });

    await responseLog.save();
    return responseLog;
  } catch (error) {
    console.error('Error sending lead to SML API:', error);
    const errorLog = await smlResponseLog.create({
      leadId: _id,
      source: source,
      requestPayload: payload,
      responseStatus: error.response?.status || 500,
      responseBody: error.response?.data || { message: 'Unknown error' },
    });
    return errorLog;
  }
}

async function sendToFreo(lead) {
  console.log("FREO", lead);
  const {
    _id, fullName, phone, email, dateOfBirth,
    gender, panNumber, jobType, salary, address, pincode, source
  } = lead;

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
    jobType,
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
      declared: salary
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
      leadId: _id,
      source: source,
      requestPayload: payload,
      responseStatus: apiResponse.status,
      responseBody: apiResponse.data,
    });

    await responseLog.save();
    return responseLog;
  } catch (error) {
    console.error('Error sending lead to Freo API:', error.message);
    const errorLog = await freoResponseLog.create({
      leadId: _id,
      source: source,
      requestPayload: payload,
      responseStatus: error.response?.status || 500,
      responseBody: error.response?.data || { message: 'Unknown error' },
    });
    return errorLog;
  }
}

async function sendToOVLY(lead) {
  console.log("OVLY", lead);
  const {
    _id, fullName, phone, email, dateOfBirth,
    gender, panNumber, jobType, salary, pincode, source
  } = lead;

  const dedupApiUrl = 'https://leads.smartcoin.co.in/partner/ratecut/lead/dedup';
  const createLeadApiUrl = 'https://leads.smartcoin.co.in/partner/ratecut/lead/create';
  const clientId = process.env.OVLY_CLIENT_ID;
  const clientKey = process.env.OVLY_CLIENT_KEY;

  const dedupPayload = new URLSearchParams({
    phone_number: phone,
    pan: panNumber,
    // date_of_birth: formatToYYYYMMDD(dateOfBirth),
    employement_type: jobType,
    net_monthly_income: `${salary}`,
    name_as_per_pan: fullName,
  });

  const dedupPayloadDB = {
    phone_number: phone,
    pan: panNumber,
    date_of_birth: dateOfBirth,
    employement_type: jobType,
    net_monthly_income: `${salary}`,
    name_as_per_pan: fullName,
  };

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
        employement_type: jobType,
        net_monthly_income: `${salary}`,
        mode_of_salary: 'ONLINE',
        bank_name: 'HDFC',
        name_as_per_pan: fullName,
        current_residence_pin_code: pincode,
        // date_of_birth: null,
        gender,
      });

      const createLeadPayloadDB = {
        phone_number: phone,
        pan: panNumber,
        email,
        employement_type: jobType,
        net_monthly_income: `${salary}`,
        mode_of_salary: 'ONLINE',
        bank_name: 'HDFC',
        name_as_per_pan: fullName,
        current_residence_pin_code: pincode,
        date_of_birth: null,
        gender,
      };

      const leadResponse = await axios.post(createLeadApiUrl, createLeadPayload, {
        headers: {
          'admin-api-client-id': clientId,
          'admin-api-client-key': clientKey,
          'Content-Type': 'application/x-www-form-urlencoded',
        },
      });

      // Save lead response in DB
      const ovlyLeadLog = new ovlyResponseLog({
        leadId: _id,
        source: source,
        requestPayload: createLeadPayloadDB,
        responseStatus: leadResponse.data.status,
        responseBody: leadResponse.data,
      });

      await ovlyLeadLog.save();
      return ovlyLeadLog;
    } else if (dedupData.isDuplicateLead === "true" && dedupData.status === "success") {
      const ovlyLeadLog = new ovlyResponseLog({
        leadId: _id,
        source: source,
        requestPayload: dedupPayloadDB,
        responseStatus: 'duplicate',
        responseBody: dedupData,
      });

      await ovlyLeadLog.save();
      return ovlyLeadLog;
    }
  } catch (error) {
    console.error('Error in OVLY API integration:', error.response?.data || error.message);
    const errorLog = await ovlyResponseLog.create({
      leadId: _id,
      source: source,
      requestPayload: dedupPayloadDB || {},
      responseStatus: error.response?.status || 500,
      responseBody: error.response?.data || { message: 'Unknown error' },
    });
    return errorLog;
  }
}

async function sendToLendingPlate(lead) {
  console.log("LP", lead);
  const {
    _id, fullName, phone, panNumber, dateOfBirth, pincode, salary, source
  } = lead;

  const isMobileValid = await checkMobileExists(phone);

  const loanPayload = {
    partner_id: process.env.LP_PARTNER_ID,
    ref_id: phone,
    mobile: phone,
    customer_name: fullName,
    pancard: panNumber,
    dob: formatDate(dateOfBirth),
    pincode,
    profession: "SAL",
    net_mothlyincome: salary,
  };

  if (!isMobileValid) {
    const responseLog = await LeadingPlateResponseLog.create({
      leadId: _id,
      source: source,
      requestPayload: loanPayload,
      responseStatus: "Fail",
      responseBody: { "status": "Failed" }
    });
    return responseLog;
  } else {
    const loanSuccess = await processLoanApplication(loanPayload);
    const responseLog = await LeadingPlateResponseLog.create({
      leadId: _id,
      source: source,
      requestPayload: loanPayload,
      responseStatus: loanSuccess.Message,
      responseBody: loanSuccess
    });
    return responseLog;
  }
}

async function sendToZYPE(lead) {
  console.log("ZYPE", lead);
  const {
    _id, fullName, phone, email, dateOfBirth, panNumber, jobType, businessType, salary, source
  } = lead;

  if (jobType === "SELF_EMPLOYED") {
    return;
  }

  const isEligible = await checkZypeEligibility(phone, panNumber);

  if (isEligible.message === 'REJECT') {
    const responseLog = await ZypeResponseLog.create({
      leadId: _id,
      source: source,
      requestPayload: {
        mobileNumber: phone,
        panNumber,
        partnerId: process.env.ZYPE_PARTNER_ID,
      },
      responseStatus: "REJECTED",
      responseBody: { status: "REJECTED" },
    });
    return responseLog;
  } else if (isEligible.status === 'ACCEPT') {
    const zypePayload = {
      mobileNumber: phone,
      email,
      panNumber,
      name: fullName,
      dob: formatToYYYYMMDD(dateOfBirth),
      income: parseInt(salary, 10),
      employmentType: 'salaried',
      orgName: businessType || "",
      partnerId: process.env.ZYPE_PARTNER_ID,
      bureauType: 3,
    };

    const zypeResponse = await processZypeApplication(zypePayload);
    const responseLog = await ZypeResponseLog.create({
      leadId: _id,
      source: source,
      requestPayload: zypePayload,
      responseStatus: zypeResponse?.status || "Unknown",
      responseBody: zypeResponse,
    });
    return responseLog;
  }
}

async function sendToFINTIFI(lead) {
  console.log("FINTIFI", lead);
  const {
    _id, fullName, phone, email, dateOfBirth, gender, panNumber, jobType, salary, pincode, source
  } = lead;

  const apiKey = process.env.API_KEY_FINTIFI;
  const externalApiUrl = `https://nucleus.fintifi.in/api/lead/ratecut`;

  const payload = {
    firstName: fullName.split(' ')[0],
    lastName: fullName.split(' ')[1] ? fullName.split(' ')[1] : fullName.split(' ')[0],
    phone,
    email,
    panNumber,
    dob: formatToYYYYMMDD(dateOfBirth),
    gender,
    salary: `${salary}`,
    pincode,
    jobType,
  };

  try {
    const apiResponse = await axios.post(externalApiUrl, payload, {
      headers: {
        'x-api-key': apiKey,
        'Content-Type': 'application/json',
      },
    });

    // Save API response to the new collection
    const responseLog = new FintifiResponseLog({
      leadId: _id,
      source: source,
      requestPayload: payload,
      responseStatus: apiResponse.data.success,
      responseBody: apiResponse.data,
    });

    await responseLog.save();
    return responseLog;
  } catch (error) {
    console.error('Error sending lead to FINTIFI API:', error);
    const errorLog = await FintifiResponseLog.create({
      leadId: _id,
      source: source,
      requestPayload: payload,
      responseStatus: error.success || 500,
      responseBody: error.error || { message: 'Unknown error' },
    });
    return errorLog;
  }
}

async function sendToFATAKPAY(lead) {
  console.log("FATAKPAY", lead);
  const {
    _id, fullName, firstName, lastName, phone, email, dateOfBirth,
    gender, address, pincode, jobType, panNumber, source
  } = lead;

  // Check if pincode is valid for FATAKPAY
  const validPincodes = pinCodeData.map((row) => parseInt(row.Pincode, 10));
  if (!validPincodes.includes(parseInt(pincode))) {
    console.log(`Pincode ${pincode} not valid for FATAKPAY. Skipping.`);
    return null;
  }

  try {
    const tokenResponse = await axios.post(
      'https://onboardingapi.fatakpay.com/external-api/v1/create-user-token',
      {
        username: process.env.FATAKPAY_USERNAME,
        password: process.env.FATAKPAY_PASSWORD,
      }
    );

    const accessToken = tokenResponse.data.data.token;

    const eligibilityPayload = {
      mobile: phone,
      first_name: firstName || fullName.split(' ')[0],
      last_name: lastName || fullName.split(' ')[1] || fullName.split(' ')[0],
      email,
      employment_type_id: jobType,
      pan: panNumber,
      dob: formatToYYYYMMDD(dateOfBirth),
      pincode,
      home_address: address || '',
      office_address: address || '',
      consent: true,
      consent_timestamp: new Date().toISOString().replace('T', ' ').split('.')[0],
    };

    const eligibilityResponse = await axios.post(
      'https://onboardingapi.fatakpay.com/external-api/v1/emi-insurance-eligibility',
      eligibilityPayload,
      {
        headers: {
          Authorization: `Token ${accessToken}`,
          'Content-Type': 'application/json',
        },
      }
    );

    // Save Response in Database
    const responseLog = await fatakPayResponseLog.create({
      leadId: _id,
      source: source,
      requestPayload: eligibilityPayload,
      responseStatus: eligibilityResponse.data.status_code,
      responseBody: eligibilityResponse.data,
    });
    return responseLog;
  } catch (error) {
    console.error('Error in FatakPay Eligibility API:', error.response?.data || error.message);
    const eligibilityPayload = {
      mobile: phone,
      first_name: firstName || fullName.split(' ')[0],
      last_name: lastName || fullName.split(' ')[1] || '',
      email,
      employment_type_id: jobType,
      pan: panNumber,
      dob: dateOfBirth,
      pincode,
      home_address: address || '',
      office_address: address || '',
      consent: true,
      consent_timestamp: new Date().toISOString().replace('T', ' ').split('.')[0],
    };

    const errorLog = await fatakPayResponseLog.create({
      leadId: _id,
      source: source,
      requestPayload: eligibilityPayload || {},
      responseStatus: error.response?.status || 500,
      responseBody: error.response?.data || { message: 'Unknown error' },
    });
    return errorLog;
  }
}

async function sendToRAMFINCROP(lead) {
  const {
    _id, fullName, phone, email, dateOfBirth, panNumber, jobType, salary, source
  } = lead;

  const payload = {
    mobile: phone,
    name: fullName,
    loanAmount: salary,
    email: email,
    employeeType: jobType,
    dob: formatToYYYYMMDD(dateOfBirth),
    pancard: panNumber
  };

  try {
    const response = await axios.post(
      'https://www.ramfincorp.com/loanapply/ramfincorp_api/lead_gen/api/v1/create_lead',
      payload,
      {
        headers: {
          'Content-Type': 'application/json',
          'Authorization': 'Basic cmFtZmluXzQ3YTVjZDcyNWNmYTMwNjA5NGY0MWM2MzNlMWZjNDE2OjRjNzBlYzc1NTc1OGYwMTYxOTVmODM5NzgxMDRhNjAzM2ZhNGExYTU='
        }
      }
    );
    console.log(response);

    const responseLog = await ramFinCropLog.create({
      leadId: _id,
      source: source,
      requestPayload: payload,
      responseStatus: response.status,
      responseBody: response.data,
    });
    return responseLog;
  } catch (error) {
    console.error('Error creating lead for RAMFINCROP:', error.response ? error.response.data : error.message);
    const errorLog = await ramFinCropLog.create({
      leadId: _id,
      source: source,
      requestPayload: payload,
      responseStatus: error.response?.status || 500,
      responseBody: error.response?.data || { message: 'Unknown error' },
    });
    return errorLog;
  }
}

async function sendToVrindaFintech(lead) {
  const {
    _id, fullName, phone, email, panNumber, pincode,
    jobType, salary,
  } = lead;

  const payload = {
    full_name: fullName,
    mobile: phone,
    mobile_verification_flag: "0",
    email,
    pancard: panNumber,
    pincode,
    income_type: jobType,
    purpose_of_loan: 'Purchase',
    monthly_salary: salary,
    loan_amount: salary,
    customer_lead_id: _id.toString(),
  };

  try {
    const response = await axios.post(
      'https://preprod-api.vrindafintech.com/marketing-push-data/',
      payload,
      {
        headers: {
          'Auth': 'd95af2f34ac136f6941307556bda40f881c6349cfc380c07b010a068474f40e8',
          'Username': 'RATECUT_30042025',
          'Accept': 'application/json',
          'Content-Type': 'application/json'
        }
      }
    );

    const responseLog = await vrindaLog.create({
      leadId: _id,
      requestPayload: payload,
      responseStatus: response.status,
      responseBody: response.data,
    });

    return responseLog;
  } catch (error) {
    console.error('Error creating lead for VrindaFintech:', error.response?.data || error.message);
    const errorLog = await vrindaLog.create({
      leadId: _id,
      requestPayload: payload,
      responseStatus: error.response?.status || 500,
      responseBody: error.response?.data || { message: 'Unknown error' },
    });

    return errorLog;
  }
}

async function sendToMyMoneyMantra(lead) {
  console.log("MyMoneyMantra API Call899", lead);

  const {
    _id, fullName, phone, email, dateOfBirth,
    gender, pincode, jobType, panNumber, salary, source
  } = lead;

  try {
    // Step 1: Get Access Token
    const correlationId = `MMM_${Date.now()}`;
    console.log("correlationId:909", correlationId);

    const authResponse = await axios.post(
      'https://api2.mymoneymantra.com/api/jwt/v1/authenticate',
      {
        clientId: process.env.MMM_CLIENT_ID || 'RateCut',
        clientSecret: process.env.MMM_CLIENT_SECRET || 'mmm@2025#rateCut@Pr0d'
      },
      {
        headers: {
          'Content-Type': 'application/json',
          'correlationId': correlationId,
          'appId': 'MMMWEBAPP'
        }
      }
    );
    console.log("925", authResponse.data);

    const { accessToken } = authResponse.data;

    // Step 2: Prepare Lead Payload
    const leadPayload = {
      personal: {
        fullName: fullName,
        gender: mapGenderToMMM(gender),
        dob: formatDatewithdash(dateOfBirth),
        bankConsent: true,
        pan: panNumber,
        whatsappConsent: true,
        mmmConsent: true
      },
      contact: {
        mobile: [
          {
            addressTypeMasterId: "1",
            mobile: phone,
            isDefault: "Y"
          }
        ],
        email: [
          {
            addressTypeMasterId: "5",
            email: email,
            isDefault: "Y"
          }
        ]
      },
      work: {
        applicantType: mapJobTypeToMMM(jobType),
        netMonthlyIncome: salary
      },
      productId: 17,
      utmMedium: process.env.MMM_UTM_MEDIUM || "cpd",
      utmSource: process.env.MMM_UTM_SOURCE || "affliate_ratecut",
      utmCampaign: process.env.MMM_UTM_CAMPAIGN || "affliate_ratecut",
      address: [
        {
          addressTypeMasterId: "1000000001",
          pincode: pincode
        }
      ],
    };

    // Step 3: Send Lead to MMM    

    const leadResponse = await axios.post(
      'https://api2.mymoneymantra.com/orchestration/api/affiliate/lead',
      leadPayload,
      {
        headers: {
          'Content-Type': 'application/json',
          'Authorization': `Bearer ${accessToken}`,
          'correlationId': correlationId,
          'channelName': process.env.MMM_CHANNEL_NAME || 'MMM_API_WEB',
          'channelSource': process.env.MMM_CHANNEL_SOURCE || 'RateCut_B2C',
          'appId': 'MMMWEBAPP',
          'sync': 'false',
          'documentAsync': 'true',
          'affiliate': true
        }
      }
    );

    // Step 4: Save Response in Database
    const responseLog = await mmmResponseLog.create({
      leadId: _id,
      source: source,
      correlationId: correlationId,
      requestPayload: leadPayload,
      responseStatus: leadResponse.status,
      responseBody: leadResponse.data
    });

    console.log('MyMoneyMantra Lead Created:', leadResponse.data);
    return responseLog;

  } catch (error) {
    console.error('Error in MyMoneyMantra API:', error.response?.data || error.message);

    // Prepare payload for error logging
    const leadPayload = {
      personal: {
        fullName: fullName,
        mobile: phone,
        email: email,
        pan: panNumber || '',
        dob: formatDatewithdash(dateOfBirth)
      },
      pincode: pincode
    };

    // Save error log
    const errorLog = await mmmResponseLog.create({
      leadId: _id,
      source: source,
      requestPayload: leadPayload,
      responseStatus: error.response?.status || 500,
      responseBody: error.response?.data || {
        message: error.message || 'Unknown error',
        error: true
      },
      errorDetails: {
        message: error.message,
        code: error.code,
        stack: error.stack
      }
    });

    return errorLog;
  }
}

// Map gender to MMM master values
function mapGenderToMMM(gender) {
  const genderMap = {
    'male': '1000000001',
    'm': '1000000001',
    'female': '1000000002',
    'f': '1000000002'
  };
  return genderMap[gender?.toLowerCase()] || '1000000001';
}

// Map job type to MMM applicant type
function mapJobTypeToMMM(jobType) {
  const jobTypeMap = {
    'salaried': '1000000004',
    'self-employed': '1000000001',
    'self employed': '1000000001',
    'professional': '1000000002',
    'defence': '1000000008'
  };
  return jobTypeMap[jobType?.toLowerCase()] || '1000000004';
}

// exports.createLead = async (req, res) => {
//   const { source, fullName, firstName, lastName, phone, email, age, dateOfBirth, gender, panNumber, jobType, businessType, salary, creditScore, cibilScore, address, pincode, consent, } = req.body;

//   // Input validation
//   if (!source || !fullName || !phone || !email || !panNumber || consent === undefined) {
//     return res.status(400).json({ message: 'Source, fullName, phone, email, panNumber, and consent are required.' });
//   }

//   // Full Name validation
//   if (fullName.length < 2 || fullName.length > 100) {
//     return res.status(400).json({ message: 'Full name must be between 2 and 100 characters.' });
//   }

//   // Email validation
//   const emailRegex = /[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,4}$/;
//   if (!emailRegex.test(email)) {
//     return res.status(400).json({ message: 'Invalid email format.' });
//   }

//   // PAN number validation
//   const panRegex = /^[A-Z]{5}[0-9]{4}[A-Z]$/;
//   if (!panRegex.test(panNumber)) {
//     return res.status(400).json({ message: 'Invalid PAN number format. Must match ABCDE1234F.' });
//   }

//   // Date of Birth validation
//   const isValidDate = /^\d{4}-\d{2}-\d{2}$/.test(dateOfBirth) && !isNaN(Date.parse(dateOfBirth));
//   if (dateOfBirth && (!isValidDate || new Date(dateOfBirth) > new Date())) {
//     return res.status(400).json({ message: 'Invalid date of birth or date cannot be in the future.' });
//   }

//   // Conditional defaults for salary and jobType
//   const finalSalary = salary || '50000';
//   const finalJobType = jobType || 'SALARIED';

//   try {
//     // Create and save the lead
//     const lead = new Lead({
//       source, fullName, firstName, lastName, phone, email,
//       age,
//       dateOfBirth,
//       gender,
//       panNumber,
//       jobType: finalJobType,
//       businessType,
//       salary: finalSalary,
//       creditScore,
//       cibilScore,
//       address,
//       pincode,
//       consent,
//     });

//     const savedLead = await lead.save();

//     // If source is not "SML", send lead to external API
//     if (source !== 'SML') {
//       const vendorName = process.env.VENDOR_NAME_SML;
//       const apiKey = process.env.API_KEY_SML;
//       const externalApiUrl = `https://nucleus.switchmyloan.in/vendor/${vendorName}/createLead`;

//       const payload = {
//         name: fullName,
//         phone,
//         email,
//         panNumber,
//         dob: dateOfBirth,
//         gender,
//         salary: `${finalSalary}`,
//         pincode,
//         jobType: finalJobType,
//       };

//       try {
//         const apiResponse = await axios.post(externalApiUrl, payload, {
//           headers: {
//             'x-api-key': apiKey,
//             'Content-Type': 'application/json',
//           },
//         });

//         // Save API response to the new collection
//         const responseLog = new smlResponseLog({
//           leadId: savedLead._id,
//           requestPayload: payload,
//           responseStatus: apiResponse.status,
//           responseBody: apiResponse.data,
//         });

//         await responseLog.save();
//       } catch (error) {
//         console.error('Error sending lead to external API:', error.message);
//         await smlResponseLog.create({
//           leadId: savedLead._id,
//           requestPayload: payload,
//           responseStatus: error.response?.status || 500,
//           responseBody: error.response?.data || { message: 'Unknown error' },
//         });
//       }
//     }

//     // If source is not "Freo", send lead to external API
//     if (source !== 'FREO') {
//       const baseUrl = process.env.DEV_URL;
//       const accessToken = await getAccessToken();

//       // Construct payload for MoneyTap API
//       const payload = {
//         emailId: email,
//         phone,
//         name: fullName,
//         panNumber,
//         dateOfBirth,
//         gender,
//         jobType: finalJobType,
//         homeAddress: {
//           addressLine1: address || '',
//           pincode,
//         },
//         residenceType: getRandomResidenceType(),
//         officeAddress: {
//           addressLine1: address || '',
//           pincode,
//         },
//         incomeInfo: {
//           declared: finalSalary
//         },
//       };

//       try {
//         const apiResponse = await axios.post(
//           `${baseUrl}/v3/partner/lead/create`,
//           payload,
//           {
//             headers: {
//               Accept: 'application/json',
//               'Content-Type': 'application/json',
//               Authorization: `Bearer ${accessToken}`,
//             },
//           }
//         );

//         // Save API response to the new collection
//         const responseLog = new freoResponseLog({
//           leadId: savedLead._id,
//           requestPayload: payload,
//           responseStatus: apiResponse.status,
//           responseBody: apiResponse.data,
//         });

//         await responseLog.save();
//       } catch (error) {
//         console.error('Error sending lead to MoneyTap API:', error.message);
//         await freoResponseLog.create({
//           leadId: savedLead._id,
//           requestPayload: payload,
//           responseStatus: error.response?.status || 500,
//           responseBody: error.response?.data || { message: 'Unknown error' },
//         });
//       }
//     }

//     // If source is not "OVLY", send lead to external API
//     if (source !== 'OVLY') {
//       const dedupApiUrl = 'https://leads.smartcoin.co.in/partner/ratecut/lead/dedup';
//       const createLeadApiUrl = 'https://leads.smartcoin.co.in/partner/ratecut/lead/create';
//       const clientId = process.env.OVLY_CLIENT_ID;
//       const clientKey = process.env.OVLY_CLIENT_KEY;

//       const dedupPayload = new URLSearchParams({
//         phone_number: phone,
//         pan: panNumber,
//         date_of_birth: dateOfBirth,
//         employement_type: finalJobType,
//         net_monthly_income: `${finalSalary}`,
//         name_as_per_pan: fullName,
//       });

//       const dedupPayloadDB = {
//         phone_number: phone,
//         pan: panNumber,
//         date_of_birth: dateOfBirth,
//         employement_type: finalJobType,
//         net_monthly_income: `${finalSalary}`,
//         name_as_per_pan: fullName,
//       };

//       try {
//         const dedupResponse = await axios.post(dedupApiUrl, dedupPayload, {
//           headers: {
//             'admin-api-client-id': clientId,
//             'admin-api-client-key': clientKey,
//             'Content-Type': 'application/x-www-form-urlencoded',
//           },
//         });

//         const dedupData = dedupResponse.data;
//         console.log("Ovly:", dedupData);
//         // If lead is fresh (not a duplicate), push to OVLY Lead Create API
//         if (dedupData.isDuplicateLead === "false" && dedupData.status === "success") {
//           console.log("Ovly:", dedupData.isDuplicateLead, dedupData.status);
//           const createLeadPayload = new URLSearchParams({
//             phone_number: phone,
//             pan: panNumber,
//             email,
//             employement_type: finalJobType,
//             net_monthly_income: `${finalSalary}`,
//             mode_of_salary: 'ONLINE',
//             bank_name: 'HDFC',
//             name_as_per_pan: fullName,
//             current_residence_pin_code: pincode,
//             date_of_birth: dateOfBirth,
//             gender,
//           });

//           const createLeadPayloadDB = {
//             phone_number: phone,
//             pan: panNumber,
//             email,
//             employement_type: finalJobType,
//             net_monthly_income: `${finalSalary}`,
//             mode_of_salary: 'ONLINE',
//             bank_name: 'HDFC',
//             name_as_per_pan: fullName,
//             current_residence_pin_code: pincode,
//             date_of_birth: dateOfBirth,
//             gender,
//           };

//           const leadResponse = await axios.post(createLeadApiUrl, createLeadPayload, {
//             headers: {
//               'admin-api-client-id': clientId,
//               'admin-api-client-key': clientKey,
//               'Content-Type': 'application/x-www-form-urlencoded',
//             },
//           });

//           console.log('Lead successfully pushed:', leadResponse.data);
//           // Save lead response in DB
//           const ovlyLeadLog = new ovlyResponseLog({
//             leadId: savedLead._id,
//             requestPayload: createLeadPayloadDB,
//             responseStatus: leadResponse.data.status,
//             responseBody: leadResponse.data,
//           });

//           await ovlyLeadLog.save();

//         } else if (dedupData.isDuplicateLead === "true" && dedupData.status === "success") {
//           console.log("Ovly:", dedupData.isDuplicateLead, dedupData.status);
//           const ovlyLeadLog = new ovlyResponseLog({
//             leadId: savedLead._id,
//             requestPayload: dedupPayloadDB,
//             responseStatus: 'duplicate',
//             responseBody: dedupData,
//           });

//           await ovlyLeadLog.save();
//         }
//       } catch (error) {
//         console.error('Error in OVLY API integration:', error.response?.data || error.message);
//         await ovlyResponseLog.create({
//           leadId: savedLead._id,
//           requestPayload: dedupPayloadDB,
//           responseStatus: error.response?.status,
//           responseBody: error.response?.data || { message: 'Unknown error' },
//         });
//       }
//     }

//     // If source is not "LendingPlate", send lead to external API
//     if (source !== 'LendingPlate') {
//       const isMobileValid = await checkMobileExists(phone);
//       if (!isMobileValid) {
//         const loanPayload = {
//           partner_id: process.env.LP_PARTNER_ID,
//           ref_id: phone,
//           mobile: phone,
//           customer_name: fullName,
//           pancard: panNumber,
//           dob: formatDate(dateOfBirth),
//           pincode,
//           profession: "SAL",
//           net_mothlyincome: finalSalary,
//         };

//         await LeadingPlateResponseLog.create({
//           leadId: savedLead._id,
//           requestPayload: loanPayload,
//           responseStatus: "Fail",
//           responseBody: { "status": "Failed" }
//         });
//       } else if (isMobileValid) {
//         const loanPayload = {
//           partner_id: process.env.LP_PARTNER_ID,
//           ref_id: phone,
//           mobile: phone,
//           customer_name: fullName,
//           pancard: panNumber,
//           dob: formatDate(dateOfBirth),
//           pincode,
//           profession: "SAL",
//           net_mothlyincome: finalSalary,
//         };

//         const loanSuccess = await processLoanApplication(loanPayload);

//         await LeadingPlateResponseLog.create({
//           leadId: savedLead._id,
//           requestPayload: loanPayload,
//           responseStatus: loanSuccess.Message,
//           responseBody: loanSuccess
//         });
//       }
//     }

//     // If source is not "ZYPE", send lead to external API
//     if (source !== 'ZYPE') {
//       const isEligible = await checkZypeEligibility(phone, panNumber);
//       console.log(isEligible);
//       if (isEligible.message === 'REJECT') {
//         await ZypeResponseLog.create({
//           leadId: savedLead._id,
//           requestPayload: {
//             mobileNumber: phone,
//             panNumber,
//             partnerId: process.env.ZYPE_PARTNER_ID,
//           },
//           responseStatus: "REJECTED",
//           responseBody: { status: "REJECTED" },
//         });
//       } else if (isEligible.status === 'ACCEPT') {
//         const zypePayload = {
//           mobileNumber: phone,
//           email,
//           panNumber,
//           name: fullName,
//           dob: dateOfBirth,
//           income: parseInt(finalSalary, 10),
//           employmentType: 'salaried',
//           orgName: businessType || "",
//           partnerId: process.env.ZYPE_PARTNER_ID,
//           bureauType: 3,
//         };

//         const zypeResponse = await processZypeApplication(zypePayload);
//         // console.log(zypeResponse);


//         await ZypeResponseLog.create({
//           leadId: savedLead._id,
//           requestPayload: zypePayload,
//           responseStatus: zypeResponse?.status || "Unknown",
//           responseBody: zypeResponse,
//         });
//       }
//     }

//     // If source is not "FINTIFI", send lead to external API
//     if (source !== 'FINTIFI') {
//       const apiKey = process.env.API_KEY_FINTIFI;
//       const externalApiUrl = `https://nucleus.fintifi.in/api/lead/ratecut`;

//       const payload = {
//         firstName: fullName.split(' ')[0],
//         lastName: fullName.split(' ')[1] ? fullName.split(' ')[1] : fullName.split(' ')[0],
//         phone,
//         email,
//         panNumber,
//         dob: dateOfBirth,
//         gender,
//         salary: `${finalSalary}`,
//         pincode,
//         jobType: finalJobType,
//       };

//       try {
//         const apiResponse = await axios.post(externalApiUrl, payload, {
//           headers: {
//             'x-api-key': apiKey,
//             'Content-Type': 'application/json',
//           },
//         });

//         // Save API response to the new collection
//         const responseLog = new FintifiResponseLog({
//           leadId: savedLead._id,
//           requestPayload: payload,
//           responseStatus: apiResponse.data.success,
//           responseBody: apiResponse.data,
//         });

//         await responseLog.save();
//       } catch (error) {
//         console.error('Error sending lead to external API:', error);
//         await FintifiResponseLog.create({
//           leadId: savedLead._id,
//           requestPayload: payload,
//           responseStatus: error.success || 500,
//           responseBody: error.error || { message: 'Unknown error' },
//         });
//       }
//     }

//     // If source is not "FATAKPAY", send lead to external API
//     const validPincodes = pinCodeData.map((row) => parseInt(row.Pincode, 10));
//     // console.log(validPincodes);

//     if (source !== 'FATAKPAY' && validPincodes.includes(parseInt(pincode))) {
//       try {
//         const tokenResponse = await axios.post(
//           'https://onboardingapi.fatakpay.com/external-api/v1/create-user-token',
//           {
//             username: process.env.FATAKPAY_USERNAME,
//             password: process.env.FATAKPAY_PASSWORD,
//           }
//         );

//         const accessToken = tokenResponse.data.data.token;

//         const eligibilityPayload = {
//           mobile: phone,
//           first_name: firstName || fullName.split(' ')[0],
//           last_name: lastName || fullName.split(' ')[1] || '',
//           email,
//           employment_type_id: finalJobType,
//           pan: panNumber,
//           dob: dateOfBirth,
//           pincode,
//           home_address: address || '',
//           office_address: address || '',
//           consent: true,
//           consent_timestamp: new Date().toISOString().replace('T', ' ').split('.')[0],
//         };

//         const eligibilityResponse = await axios.post(
//           'https://onboardingapi.fatakpay.com/external-api/v1/emi-insurance-eligibility',
//           eligibilityPayload,
//           {
//             headers: {
//               Authorization: `Token ${accessToken}`,
//               'Content-Type': 'application/json',
//             },
//           }
//         );

//         // Step 3: Save Response in Database
//         await fatakPayResponseLog.create({
//           leadId: savedLead._id,
//           requestPayload: eligibilityPayload,
//           responseStatus: eligibilityResponse.data.status_code,
//           responseBody: eligibilityResponse.data,
//         });

//       } catch (error) {
//         console.error('Error in FatakPay Eligibility API:', error.response?.data || error.message);

//         await fatakPayResponseLog.create({
//           leadId: savedLead._id,
//           requestPayload: eligibilityPayload,
//           responseStatus: error.response?.status || 500,
//           responseBody: error.response?.data || { message: 'Unknown error' },
//         });
//       }
//     }

//     if (source !== 'RAMFINCROP') {
//       const payload = {
//         mobile: phone,
//         name: fullName,
//         loanAmount: finalSalary,
//         email: email,
//         employeeType: jobType,
//         dob: dateOfBirth,
//         pancard: panNumber
//       }

//       try {
//         const response = await axios.post(
//           'https://www.ramfincorp.com/loanapply/ramfincorp_api/lead_gen/api/v1/create_lead',
//           payload,
//           {
//             headers: {
//               'Content-Type': 'application/json',
//               'Authorization': 'Basic cmFtZmluXzQ3YTVjZDcyNWNmYTMwNjA5NGY0MWM2MzNlMWZjNDE2OjRjNzBlYzc1NTc1OGYwMTYxOTVmODM5NzgxMDRhNjAzM2ZhNGExYTU='
//             }
//           }
//         );

//         await ramFinCropLog.create({
//           leadId: savedLead._id,
//           requestPayload: payload,
//           responseStatus: response.data.status,
//           responseBody: response.data,
//         });

//         console.log(response.data);
//       } catch (error) {
//         console.error('Error creating lead:', error.response ? error.response.data : error.message);
//         await ramFinCropLog.create({
//           leadId: savedLead._id,
//           requestPayload: payload,
//           responseStatus: error.response?.data.status || 500,
//           responseBody: error.response?.data || { message: 'Unknown error' },
//         });
//       }
//     }

//     res.status(201).json({
//       status: 'success',
//       data: {
//         lead: savedLead,
//       },
//     });
//   } catch (error) {
//     console.log(error);
//     if (error.code === 11000) {
//       return res.status(409).json({ message: 'Duplicate PAN number' });
//     }
//     res.status(500).json({ message: 'Server error', error: error.message });
//   }
// };

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
      return res.status(409).json({ message: 'Duplicate PAN number or phone' });
    }
    res.status(500).json({ message: 'Server error', error: error.message });
  }
};


// Function to process file
const convertExcelDateToJSDate = (excelDate, PAN) => {
  console.log(excelDate, PAN);
  const jsDate = new Date((excelDate - 25569) * 86400 * 1000);
  return jsDate.toISOString().split("T")[0];
};

exports.processFile = async (req, res) => {
  try {
    if (!req.file) {
      return res.status(400).json({ message: "No file uploaded." });
    }

    const lenders = req.body.lenders;
    if (!lenders || lenders.length === 0) {
      return res.status(400).json({ message: "Select at least one lender." });
    }

    const filePath = req.file.path;
    const leads = readFile(filePath);

    console.log(`📊 Total leads found: ${leads.length}`);
    console.log(leads[0]);
    // Save leads to MongoDB
    const savedLeads = await Lead.insertMany(
      leads.map((lead) => ({
        source: "FREO_FEB",
        fullName: `${lead.fullName}`,
        phone: `${lead.phone}`,
        email: lead.email,
        dateOfBirth: lead.dateOfBirth,
        gender: lead.gender,
        panNumber: lead.panNumber,
        jobType: lead.jobType,
        salary: `${lead.salary}`,
        address: `${lead.address}`,
        pincode: `${lead.pincode}`,
        consent: true
      }))
    );

    // const leadsWithIds = savedLeads.map((savedLead, index) => ({
    //   ...leads[index],
    //   _id: savedLead._id,
    // }));

    // Send to selected lenders
    const sendLeadsPromises = savedLeads.map((lead) => sendToSML(lead));
    const responses = await Promise.all(sendLeadsPromises);

    deleteFile(filePath);

    res.status(200).json({ message: "Leads processed successfully", responses });
  } catch (error) {
    console.error("Error processing leads:", error);
    res.status(500).json({ message: "Internal server error" });
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
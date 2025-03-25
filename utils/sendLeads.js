const axios = require("axios");
const xlsx = require('xlsx');
const path = require('path');
const ovlyResponseLog = require('../models/ovlyResponseLog');
const smlResponseLog = require('../models/smlResponseLogModel');
const freoResponseLog = require('../models/freoResponseLogModel');
const leadingPlateResponseLog = require('../models/leadingPlateResponseLog');
const FintifiResponseLog = require('../models/fintifiResponseLog');
const ZypeResponseLog = require('../models/ZypeResponseLogModel');
const fatakPayResponseLog = require('../models/fatakPayResponseLog');

const sendLeadsToLender = async (lender, leads) => {
  switch (lender) {
    case "SML":
      return sendToSML(leads);
    case "FREO":
      return sendToFreo(leads);
    case "ZYPE":
      return sendToZype(leads);
    case "LendingPlate":
      return sendToLP(leads);
    case "FINTIFI":
      return sendToFintifi(leads);
    case "OVLY":
      return sendToOvly(leads);
    case "FATAKPAY":
      return sendToFatakpay(leads);
    default:
      return { lender, status: "Failed", message: "Lender not configured" };
  }
};

// Send to FREO
const getAccessToken = async () => {
  const baseUrl = `https://app.moneytap.com/oauth/token?grant_type=client_credentials`;
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
    console.error('XLSX Error fetching access token:', error.response?.data || error.message);
    throw new Error('Failed to generate access token');
  }
};

const convertExcelDateToJSDate = (excelDate) => {
  const jsDate = new Date((excelDate - 25569) * 86400 * 1000);
  console.log(jsDate.toISOString().split("T")[0], excelDate);
  return jsDate.toISOString().split("T")[0];
};

const getRandomResidenceType = () => {
  const residenceTypes = ['OWNED', 'RENT', 'LEASE'];
  return residenceTypes[Math.floor(Math.random() * residenceTypes.length)];
};

const sendToFreo = async (leads) => {
  const accessToken = await getAccessToken();
  // Construct payload for MoneyTap API
  const payload = leads.map(lead => ({
    emailId: lead.Email,
    phone: `${lead.Phone}`,
    name: `${lead["First Name"]} ${lead["Last Name"]}`,
    panNumber: lead.PAN,
    dateOfBirth: convertExcelDateToJSDate(lead.DOB),
    gender: lead.Gender,
    jobType: lead.EmploymentType,
    homeAddress: {
      addressLine1: `${lead.Pincode}`,
      addressLine2: `${lead.Pincode}`,
      pincode: `${lead.Pincode}`,
    },
    residenceType: getRandomResidenceType(),
    officeAddress: {
      addressLine1: `${lead.Pincode}`,
      addressLine2: `${lead.Pincode}`,
      pincode: `${lead.Pincode}`,
    },
    incomeInfo: {
      declared: lead.Salary,
      mode: 'ONLINE'
    },
  }));

  try {
    const apiResponse = await axios.post(
      `https://app.moneytap.com/v3/partner/lead/create`,
      { payload },
      {
        headers: {
          Accept: 'application/json',
          'Content-Type': 'application/json',
          Authorization: `Bearer ${accessToken}`,
        },
      }
    );

    // Save API response to the new collection
    const responseLogs = payload.map((lead, index) => ({
      leadId: lead._id,
      requestPayload: lead,
      responseStatus: apiResponse.status,
      responseBody: apiResponse.data[index] || apiResponse.data,
    }));

    await freoResponseLog.insertMany(responseLogs);
  } catch (error) {
    console.error('XLSX Error sending lead to MoneyTap API:', error);
    const errorLogs = payload.map(lead => ({
      leadId: lead._id,
      requestPayload: lead,
      responseStatus: error.response?.status || 500,
      responseBody: error.response?.data || { message: 'Unknown error' },
    }));

    await freoResponseLog.insertMany(errorLogs);
  }
}


// Function to send lead to SML
const sendToSML = async (leads) => {  
  const vendorName = "ratecut";
  const apiKey = "td3gH20O6OjccEadCa8+9g==";
  const externalApiUrl = `https://nucleus.switchmyloan.in/vendor/${vendorName}/createLead`;

  const formattedLeads = leads.map(lead => ({
    name: `${lead["First Name"]} ${lead["Last Name"]}`,
    phone: `${lead.Phone}`,
    email: lead.Email,
    panNumber: lead.PAN,
    dob: convertExcelDateToJSDate(lead.DOB),
    gender: lead.Gender,
    salary: `${lead.Salary}`,
    pincode: `${lead.Pincode}`,
    jobType: lead.EmploymentType,
  }));

  try {
    const apiResponse = await axios.post(
      externalApiUrl,
      { formattedLeads },
      {
        headers: {
          'x-api-key': apiKey,
          'Content-Type': 'application/json',
        },
      }
    );

    // Save response logs in bulk
    const responseLogs = formattedLeads.map((lead, index) => ({
      leadId: lead._id,
      requestPayload: lead,
      responseStatus: apiResponse.status,
      responseBody: apiResponse.data[index] || apiResponse.data,
    }));

    await smlResponseLog.insertMany(responseLogs);
  } catch (error) {
    console.error('XLSX Error sending leads to SML API:', error.message);

    const errorLogs = formattedLeads.map(lead => ({
      leadId: lead._id,
      requestPayload: lead,
      responseStatus: error.response?.status || 500,
      responseBody: error.response?.data || { message: 'Unknown error' },
    }));

    await smlResponseLog.insertMany(errorLogs);
  }
};

// Function to send lead to LP
const checkMobileExists = async (phone) => {
  const url = 'https://lms.lendingplate.co.in/api/Api/affiliateApi/checkmobile';
  const headers = {
    'Authorization': `Bearer b8e7f2c30c8e10d25da52329d164f4801f4e5dbb253895c98d343c75030d45aa`,
    'Content-Type': 'application/json'
  }
  const checkPaayload = {
    partner_id: "RATECUT",
    ref_id: phone,
    mobile: phone
  }
  try {
    const response = await axios.post(url, checkPaayload, { headers });

    return response.data.status === 'S';
  } catch (error) {
    console.error('XLSX Error in mobile check API:', error.response?.data || error.message);
    return false;
  }
};

const processLoanApplication = async (payload) => {
  const url = 'https://lms.lendingplate.co.in/api/Api/affiliateApi/loanprocess';
  const headers = {
    'Authorization': `Bearer b8e7f2c30c8e10d25da52329d164f4801f4e5dbb253895c98d343c75030d45aa`,
    'Content-Type': 'application/json'
  }
  try {
    const response = await axios.post(url, payload, { headers });
    console.log("XLSX LP", response);
    return response.data;
  } catch (error) {
    console.error('XLSX Error in loan process API:', error.response?.data || error.message);
    return false;
  }
};

const formatDate = (dateString) => {
  const date = new Date(dateString);
  return date.toLocaleDateString('en-GB');
};

const sendToLP = async (leads) => { 
  const logs = [];

  for (const lead of leads) {   
    const isMobileValid = await checkMobileExists(lead.Phone);
    console.log("XLSX LP mob", isMobileValid);    
    const loanPayload = {
      partner_id: "RATECUT",
      ref_id: `${lead.Phone}`,
      mobile: `${lead.Phone}`,
      customer_name: `${lead["First Name"]} ${lead["Last Name"]}`,
      pancard: lead.PAN,
      dob: formatDate(convertExcelDateToJSDate(lead.DOB)),
      pincode: `${lead.Pincode}`,
      profession: "SAL",
      net_mothlyincome: `${lead.Salary}`,
    };
    if (!isMobileValid) {

      logs.push({
        leadId: lead._id,
        requestPayload: loanPayload,
        responseStatus: "Fail",
        responseBody: { status: "Failed" }
      });
    } else if (isMobileValid) {
      const loanSuccess = await processLoanApplication(loanPayload);
      
      logs.push({
        leadId: lead._id,
        requestPayload: loanPayload,
        responseStatus: loanSuccess.Message,
        responseBody: loanSuccess
      });
    }
  }

  if (logs.length > 0) {
    await leadingPlateResponseLog.insertMany(logs);
  }
}

// Function to send lead to Zype
const checkZypeEligibility = async (mobileNumber, panNumber) => {
  try {
    const response = await axios.post(
      "https://prod.zype.co.in/attribution-service/api/v1/underwriting/customerEligibility",
      {
        mobileNumber,
        panNumber,
        partnerId: "d8fc589d-1428-4358-ad99-a9c960e8e7d4",
      },
      {
        headers: { "Content-Type": "application/json" },
      }
    );
    return response.data
  } catch (error) {
    console.error("XLSX ZYPE Eligibility Check Failed:", error.response?.data || error.message);
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
    console.log("XLSX Zype", response);

    return response.data;
  } catch (error) {
    console.error("XLSX Error sending lead to ZYPE:", error.response?.data || error.message);
    return { status: "Failed", message: "ZYPE API Error" };
  }
}

const sendToZype = async (leads) => {
  const logs = [];

  for (const lead of leads) {
    const isEligible = await checkZypeEligibility(`${lead.Phone}`, lead.PAN);
    if (isEligible.message === 'REJECT') {
      logs.push({
        leadId: lead._id,
        requestPayload: {
          mobileNumber: `${lead.Phone}`,
          panNumber: lead.PAN,
          partnerId: "d8fc589d-1428-4358-ad99-a9c960e8e7d4",
        },
        responseStatus: "REJECTED",
        responseBody: { status: "REJECTED" },
      });
    } else if (isEligible.status === 'ACCEPT') {
      const zypePayload = {
        mobileNumber: `${lead.Phone}`,
        email: lead.Email,
        panNumber: lead.PAN,
        name: `${lead["First Name"]} ${lead["Last Name"]}`,
        dob: convertExcelDateToJSDate(lead.DOB),
        income: parseInt(lead.Salary, 10),
        employmentType: 'salaried',
        orgName: "",
        partnerId: "d8fc589d-1428-4358-ad99-a9c960e8e7d4",
        bureauType: 3,
      };

      const zypeResponse = await processZypeApplication(zypePayload);

      logs.push({
        leadId: lead._id,
        requestPayload: zypePayload,
        responseStatus: zypeResponse?.status || "Unknown",
        responseBody: zypeResponse,
      });
    }
  }

  if (logs.length > 0) {
    await ZypeResponseLog.insertMany(logs);
  }
}

// Function to send lead to Fintifi
const sendToFintifi = async (leads) => {
  const apiKey = "pWNkzEYws48qzUqVmE0sKPmxiV0dDYYLiTOI6Ck9qyY=";
  const externalApiUrl = `https://nucleus.fintifi.in/api/lead/ratecut`;
  const logs = [];

  for (const lead of leads) {
    const payload = {
      firstName: lead["First Name"],
      lastName: lead["Last Name"],
      phone: `${lead.Phone}`,
      email: lead.Email,
      panNumber: lead.PAN,
      dob: convertExcelDateToJSDate(lead.DOB),
      gender: lead.Gender,
      salary: `${lead.Salary}`,
      pincode: `${lead.Pincode}`,
      jobType: lead.EmploymentType,
    };

    try {
      const apiResponse = await axios.post(externalApiUrl, payload, {
        headers: {
          'x-api-key': apiKey,
          'Content-Type': 'application/json',
        },
      });

      // Save API response to the new collection
      logs.push({
        leadId: lead._id,
        requestPayload: payload,
        responseStatus: apiResponse.data.success,
        responseBody: apiResponse.data,
      });
    } catch (error) {
      console.error('XLSX Error sending lead to Fintifi API:', error);
      logs.push({
        leadId: lead._id,
        requestPayload: payload,
        responseStatus: error.success || 500,
        responseBody: error.error || { message: 'Unknown error' },
      });
    }
  }

  if (logs.length > 0) {
    await FintifiResponseLog.insertMany(logs);
  }
}

// Function to send lead to Ovly
const sendToOvly = async (leads) => {
  const dedupApiUrl = 'https://leads.smartcoin.co.in/partner/ratecut/lead/dedup';
  const createLeadApiUrl = 'https://leads.smartcoin.co.in/partner/ratecut/lead/create';
  const clientId = process.env.OVLY_CLIENT_ID;
  const clientKey = process.env.OVLY_CLIENT_KEY;
  const logs = [];

  for (const lead of leads) {
    const dedupPayload = new URLSearchParams({
      phone_number: `${lead.Phone}`,
      pan: lead.PAN,
      date_of_birth: convertExcelDateToJSDate(lead.DOB),
      employement_type: lead.EmploymentType,
      net_monthly_income: `${lead.Salary}`,
      name_as_per_pan: `${lead["First Name"]} ${lead["Last Name"]}`,
    });

    const dedupPayloadDB = {
      phone_number: `${lead.Phone}`,
      pan: lead.PAN,
      date_of_birth: convertExcelDateToJSDate(lead.DOB),
      employement_type: lead.EmploymentType,
      net_monthly_income: `${lead.Salary}`,
      name_as_per_pan: `${lead["First Name"]} ${lead["Last Name"]}`,
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
      console.log("XLSX Ovly:", dedupData);
      // If lead is fresh (not a duplicate), push to OVLY Lead Create API
      if (dedupData.isDuplicateLead === "false" && dedupData.status === "success") {
        console.log("XLSX Ovly:", dedupData.isDuplicateLead, dedupData.status);
        const createLeadPayload = new URLSearchParams({
          phone_number: `${lead.Phone}`,
          pan: lead.PAN,
          email: lead.Email,
          employement_type: lead.EmploymentType,
          net_monthly_income: `${lead.Salary}`,
          mode_of_salary: 'ONLINE',
          bank_name: 'HDFC',
          name_as_per_pan: `${lead["First Name"]} ${lead["Last Name"]}`,
          current_residence_pin_code: lead.PAN,
          date_of_birth: convertExcelDateToJSDate(lead.DOB),
          gender: lead.Gender,
        });

        const createLeadPayloadDB = {
          phone_number: `${lead.Phone}`,
          pan: lead.PAN,
          email: lead.Email,
          employement_type: lead.EmploymentType,
          net_monthly_income: `${lead.Salary}`,
          mode_of_salary: 'ONLINE',
          bank_name: 'HDFC',
          name_as_per_pan: `${lead["First Name"]} ${lead["Last Name"]}`,
          current_residence_pin_code: lead.PAN,
          date_of_birth: convertExcelDateToJSDate(lead.DOB),
          gender: lead.Gender,
        };

        const leadResponse = await axios.post(createLeadApiUrl, createLeadPayload, {
          headers: {
            'admin-api-client-id': clientId,
            'admin-api-client-key': clientKey,
            'Content-Type': 'application/x-www-form-urlencoded',
          },
        });

        console.log('XLSX Lead successfully pushed:', leadResponse.data);
        // Save lead response in DB
        logs.push({
          leadId: lead._id,
          requestPayload: createLeadPayloadDB,
          responseStatus: leadResponse.data.status,
          responseBody: leadResponse.data,
        });

      } else if (dedupData.isDuplicateLead === "true" && dedupData.status === "success") {
        console.log("XLSX Ovly:", dedupData.isDuplicateLead, dedupData.status);
        logs.push({
          leadId: lead._id,
          requestPayload: dedupPayloadDB,
          responseStatus: 'duplicate',
          responseBody: dedupData,
        });

      }
    } catch (error) {
      console.error('XLSX Error in OVLY API integration:', error.response?.data || error.message);
      logs.push({
        leadId: lead._id,
        requestPayload: dedupPayloadDB,
        responseStatus: error.response?.status,
        responseBody: error.response?.data || { message: 'Unknown error' },
      });
    }
  }

  if (logs.length > 0) {
    await ovlyResponseLog.insertMany(logs);
  }
}


// Function to send lead to Fatakpay
function readExcelFile() {
  const workbook = xlsx.readFile(path.join(__dirname, './FatakPay_PL_Serviceable_pincode_list.xlsx'));
  const sheetName = workbook.SheetNames[1];
  return xlsx.utils.sheet_to_json(workbook.Sheets[sheetName]);
}

const pinCodeData = readExcelFile();

const validPincodes = pinCodeData.map((row) => parseInt(row.Pincode, 10));

const sendToFatakpay = async (leads) => {
  const logEntries = [];

  for (const lead of leads) {
    if (validPincodes.includes(parseInt(lead.Pincode))) {
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
          mobile: `${lead.Phone}`,
          first_name: lead["First Name"],
          last_name: lead["Last Name"],
          email: lead.Email,
          employment_type_id: lead.EmploymentType,
          pan: lead.PAN,
          dob: convertExcelDateToJSDate(lead.DOB),
          pincode: `${lead.Pincode}`,
          home_address: `${lead.Pincode}`,
          office_address: `${lead.Pincode}`,
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

        logEntries.push({
          leadId: lead._id,
          requestPayload: eligibilityPayload,
          responseStatus: eligibilityResponse.data.status_code,
          responseBody: eligibilityResponse.data,
        });

      } catch (error) {
        console.error('XLSX Error in FatakPay Eligibility API:', error.response?.data || error.message);

        logEntries.push({
          leadId: lead._id,
          requestPayload: eligibilityPayload,
          responseStatus: error.response?.status || 500,
          responseBody: error.response?.data || { message: 'Unknown error' },
        });
      }
    }
  }

  if (logEntries.length > 0) {
    await fatakPayResponseLog.insertMany(logEntries);
  }
}

module.exports = { sendLeadsToLender };
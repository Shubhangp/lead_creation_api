const axios = require('axios');
const xlsx = require('xlsx');
const path = require('path');
const crypto = require('crypto');

const SMLResponseLog = require('../models/smlResponseLogModel');
const FreoResponseLog = require('../models/freoResponseLogModel');
const OvlyResponseLog = require('../models/ovlyResponseLog');
const LendingPlateResponseLog = require('../models/leadingPlateResponseLog');
const FintifiResponseLog = require('../models/fintifiResponseLog');
const ZypeResponseLog = require('../models/ZypeResponseLogModel');
const FatakPayResponseLog = require('../models/fatakPayResponseLog');
const RamFinCropLog = require('../models/ramFinCropLogModel');
// const vrindaLog = require('../models/VrindaFintechResponseLog');
const IndiaLendsResponseLog = require('../models/indiaLendsResponseLog');
const MpokketResponseLog = require('../models/mpokketResponseLog');
const CrmPaisaResponseLog = require('../models/crmPaisaResponseLogModel');
const MMMResponseLog = require('../models/mmmResponseLog');

async function sendToSML(lead) {
  const {
    leadId, fullName, phone, email, dateOfBirth,
    gender, panNumber, jobType, salary, pincode, source
  } = lead;

  // Use whichever ID field exists
  const leadIdValue = leadId;

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

    // Save API response using DynamoDB
    const responseLog = await SMLResponseLog.create({
      leadId: leadIdValue,
      source: source,
      requestPayload: payload,
      responseStatus: apiResponse.status,
      responseBody: apiResponse.data,
    });

    return responseLog;
  } catch (error) {
    console.error('Error sending lead to SML API:', error);

    const errorLog = await SMLResponseLog.create({
      leadId: leadIdValue,
      source: source,
      requestPayload: payload,
      responseStatus: error.response?.status || 500,
      responseBody: error.response?.data || { message: 'Unknown error' },
    });
    return errorLog;
  }
}

function formatToYYYYMMDD(dateString) {
  const date = new Date(dateString);
  return date.toISOString().split('T')[0];
}

async function sendToFreo(lead) {
  console.log("FREO", lead);
  const {
    leadId, fullName, phone, email, dateOfBirth,
    gender, panNumber, jobType, salary, address, pincode, source
  } = lead;
  const baseUrl = process.env.DEV_URL;
  const accessToken = await getAccessToken();
  
  // Construct payload for Freo API
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

    // Save API response using DynamoDB model
    const responseLog = await FreoResponseLog.create({
      leadId: leadId,
      source: source,
      requestPayload: payload,
      responseStatus: apiResponse.status,
      responseBody: apiResponse.data,
    });
    
    return responseLog;
  } catch (error) {
    console.error('Error sending lead to Freo API:', error.message);
    const errorLog = await FreoResponseLog.create({
      leadId: leadId,
      source: source,
      requestPayload: payload,
      responseStatus: error.response?.status || 500,
      responseBody: error.response?.data || { message: 'Unknown error' },
    });
    
    return errorLog;
  }
}

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

// Helper Function to get a random residenceType
const getRandomResidenceType = () => {
  const residenceTypes = ['OWNED', 'RENT', 'LEASE'];
  return residenceTypes[Math.floor(Math.random() * residenceTypes.length)];
};

async function sendToOVLY(lead) {
  console.log("OVLY", lead);
  const {
    leadId, fullName, phone, email, dateOfBirth,
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
      // Save lead response using DynamoDB model
      const ovlyLeadLog = await OvlyResponseLog.create({
        leadId: leadId,
        source: source,
        requestPayload: createLeadPayloadDB,
        responseStatus: leadResponse.data.status,
        responseBody: leadResponse.data,
      });
      return ovlyLeadLog;
    } else if (dedupData.isDuplicateLead === "true" && dedupData.status === "success") {
      const ovlyLeadLog = await OvlyResponseLog.create({
        leadId: leadId,
        source: source,
        requestPayload: dedupPayloadDB,
        responseStatus: 'duplicate',
        responseBody: dedupData,
      });
      return ovlyLeadLog;
    }
  } catch (error) {
    console.error('Error in OVLY API integration:', error.response?.data || error.message);
    const errorLog = await OvlyResponseLog.create({
      leadId: leadId,
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
    leadId, fullName, phone, panNumber, dateOfBirth, pincode, salary, source
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
    const responseLog = await LendingPlateResponseLog.create({
      leadId: leadId,
      source: source,
      requestPayload: loanPayload,
      responseStatus: "Fail",
      responseBody: { "status": "Failed" }
    });
    return responseLog;
  } else {
    const loanSuccess = await processLoanApplication(loanPayload);
    const responseLog = await LendingPlateResponseLog.create({
      leadId: leadId,
      source: source,
      requestPayload: loanPayload,
      responseStatus: loanSuccess.Message,
      responseBody: loanSuccess
    });
    return responseLog;
  }
}

const formatDate = (dateString) => {
  const date = new Date(dateString);
  return date.toLocaleDateString('en-GB');
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

async function sendToZYPE(lead) {
  console.log("ZYPE", lead);
  const {
    leadId, fullName, phone, email, dateOfBirth, panNumber, jobType, businessType, salary, source
  } = lead;
  if (jobType === "SELF_EMPLOYED") {
    return;
  }
  const isEligible = await checkZypeEligibility(phone, panNumber);
  if (isEligible.message === 'REJECT') {
    const responseLog = await ZypeResponseLog.create({
      leadId: leadId,
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
      leadId: leadId,
      source: source,
      requestPayload: zypePayload,
      responseStatus: zypeResponse?.status || "Unknown",
      responseBody: zypeResponse,
    });
    return responseLog;
  }
}

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

async function sendToFINTIFI(lead) {
  console.log("FINTIFI", lead);
  const {
    leadId, fullName, phone, email, dateOfBirth, gender, panNumber, jobType, salary, pincode, source
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
    // Save API response using DynamoDB model
    const responseLog = await FintifiResponseLog.create({
      leadId: leadId,
      source: source,
      requestPayload: payload,
      responseStatus: apiResponse.data.success,
      responseBody: apiResponse.data,
    });
    return responseLog;
  } catch (error) {
    console.error('Error sending lead to FINTIFI API:', error);
    const errorLog = await FintifiResponseLog.create({
      leadId: leadId,
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
    leadId, fullName, firstName, lastName, phone, email, dateOfBirth,
    gender, address, pincode, jobType, panNumber, source
  } = lead;
  // Check if pincode is valid for FATAKPAY
  // const validPincodes = pinCodeData.map((row) => parseInt(row.Pincode, 10));
  // if (!validPincodes.includes(parseInt(pincode))) {
  //   console.log(`Pincode ${pincode} not valid for FATAKPAY. Skipping.`);
  //   return null;
  // }
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
    // Save Response using DynamoDB model
    const responseLog = await FatakPayResponseLog.create({
      leadId: leadId,
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
    const errorLog = await FatakPayResponseLog.create({
      leadId: leadId,
      source: source,
      requestPayload: eligibilityPayload || {},
      responseStatus: error.response?.status || 500,
      responseBody: error.response?.data || { message: 'Unknown error' },
    });
    return errorLog;
  }
}

function readExcelFile() {
  const workbook = xlsx.readFile(path.join(__dirname, './FatakPay_PL_Serviceable_pincode_list.xlsx'));
  const sheetName = workbook.SheetNames[1];
  return xlsx.utils.sheet_to_json(workbook.Sheets[sheetName]);
}

const pinCodeData = readExcelFile();

async function sendToRAMFINCROP(lead) {
  const {
    leadId, fullName, phone, email, dateOfBirth, panNumber, jobType, salary, source
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
    console.log(response.data, response.status);
    const responseLog = await RamFinCropLog.create({
      leadId: leadId,
      source: source,
      requestPayload: payload,
      responseStatus: response.status,
      responseBody: response.data,
    });
    return responseLog;
  } catch (error) {
    console.error('Error creating lead for RAMFINCROP:', error.response ? error.response.data : error.message);
    const errorLog = await RamFinCropLog.create({
      leadId: leadId,
      source: source,
      requestPayload: payload,
      responseStatus: error.response?.status || 500,
      responseBody: error.response?.data || { message: 'Unknown error' },
    });
    return errorLog;
  }
}

// async function sendToVrindaFintech(lead) {
//   const {
//     _id, fullName, phone, email, panNumber, pincode,
//     jobType, salary,
//   } = lead;

//   const payload = {
//     full_name: fullName,
//     mobile: phone,
//     mobile_verification_flag: "0",
//     email,
//     pancard: panNumber,
//     pincode,
//     income_type: jobType,
//     purpose_of_loan: 'Purchase',
//     monthly_salary: salary,
//     loan_amount: salary,
//     customer_lead_id: _id.toString(),
//   };

//   try {
//     const response = await axios.post(
//       'https://preprod-api.vrindafintech.com/marketing-push-data/',
//       payload,
//       {
//         headers: {
//           'Auth': 'd95af2f34ac136f6941307556bda40f881c6349cfc380c07b010a068474f40e8',
//           'Username': 'RATECUT_30042025',
//           'Accept': 'application/json',
//           'Content-Type': 'application/json'
//         }
//       }
//     );

//     const responseLog = await vrindaLog.create({
//       leadId: _id,
//       requestPayload: payload,
//       responseStatus: response.status,
//       responseBody: response.data,
//     });

//     return responseLog;
//   } catch (error) {
//     console.error('Error creating lead for VrindaFintech:', error.response?.data || error.message);
//     const errorLog = await vrindaLog.create({
//       leadId: _id,
//       requestPayload: payload,
//       responseStatus: error.response?.status || 500,
//       responseBody: error.response?.data || { message: 'Unknown error' },
//     });

//     return errorLog;
//   }
// }

async function sendToMyMoneyMantra(lead) {
  console.log("MyMoneyMantra API Call899", lead);
  const {
    leadId, fullName, phone, email, dateOfBirth,
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
    // Step 4: Save Response using DynamoDB model
    const responseLog = await MMMResponseLog.create({
      leadId: leadId,
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
    const errorLog = await MMMResponseLog.create({
      leadId: leadId,
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

const formatDatewithdash = (dateString) => {
  const date = new Date(dateString);
  const day = String(date.getDate()).padStart(2, '0');
  const month = String(date.getMonth() + 1).padStart(2, '0');
  const year = date.getFullYear();

  return `${day}-${month}-${year}`;
};

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

async function sendToIndiaLends(lead) {
  console.log("IndiaLends API Call", lead);

  const {
    leadId,
    fullName,
    phone,
    email,
    dateOfBirth,
    gender,
    pincode,
    jobType,
    panNumber,
    salary,
    source,
    companyName
  } = lead;

  try {
    // Step 1: Authenticate and Get Access Token
    const authResponse = await axios.post(
      'https://ilauthenticationlive.azurewebsites.net/api/Login/PostImplicitLogin',
      null,
      {
        params: {
          email: 'RateCut@indialends.com',
          password: 'raiNdL@211125$#'
        }
      }
    );

    console.log("Authentication Response:", authResponse.data);

    if (authResponse.data.info.status !== "100") {
      throw new Error(`Authentication failed: ${authResponse.data.info.message}`);
    }

    const accessToken = authResponse.data.data.access_token;

    // Step 2: Check for Duplicate Lead (Dedup Check)
    const mobileHash = hashMobileNumber(phone);
    
    const dedupResponse = await axios.post(
      'https://apimgmtlive.indialends.com/LoanOffers/LaasDedupV1',
      {
        MobileNumber: mobileHash
      },
      {
        headers: {
          'Authorization': `Bearer ${accessToken}`,
          'Ocp-Apim-Subscription-Key': process.env.INDIALENDS_SUBSCRIPTION_KEY || 'c24c7706d8e84bd7be5f661aa1622082',
          'Content-Type': 'application/json'
        }
      }
    );

    console.log("Dedup Response:", dedupResponse.data);

    const isDedupe = dedupResponse.data.data.IsDedupe;

    // If IsDedupe is "1" or "2", save as duplicate and return
    if (isDedupe === "1" || isDedupe === "2") {
      const duplicateLog = await IndiaLendsResponseLog.create({
        leadId: leadId,
        source: source,
        requestPayload: { MobileNumber: mobileHash },
        responseStatus: dedupResponse.status,
        responseBody: dedupResponse.data,
        isDuplicate: true,
        duplicateStatus: isDedupe
      });

      console.log('IndiaLends Duplicate Lead Found:', dedupResponse.data);
      return duplicateLog;
    }

    // Step 3: Submit Credit Report Details (Only if not duplicate)
    const [firstName, ...lastNameParts] = fullName.split(' ');
    const lastName = lastNameParts.join(' ') || firstName;

    const creditReportPayload = {
      EmailID: email,
      reference_id: phone,
      PanNumber: panNumber,
      MobileNumber: phone,
      EmployementType: mapJobTypeToEmploymentType(jobType),
      MonthlyIncome: salary.toString(),
      FirstName: firstName,
      LastName: lastName,
      ResidencePinCode: pincode,
      CompanyName: companyName || "Not Provided",
      DOB: formatDateForIndiaLends(dateOfBirth),
      Gender: mapGenderToIndiaLends(gender),
      FlowByPass: 1,
      SalaryMode: "cheque"
    };

    const creditReportResponse = await axios.post(
      'https://apimgmtlive.indialends.com/LoanOffers/api/CreditReport/POSTCreditReportDetailsv1',
      creditReportPayload,
      {
        headers: {
          'Authorization': `Bearer ${accessToken}`,
          'Ocp-Apim-Subscription-Key': process.env.INDIALENDS_SUBSCRIPTION_KEY || 'c24c7706d8e84bd7be5f661aa1622082',
          'Content-Type': 'application/json'
        }
      }
    );

    console.log("Credit Report Response:", creditReportResponse.data);

    // Step 4: Save Response in Database
    const responseLog = await IndiaLendsResponseLog.create({
      leadId: leadId,
      source: source,
      accessToken: accessToken,
      dedupCheck: dedupResponse.data,
      requestPayload: creditReportPayload,
      responseStatus: creditReportResponse.status,
      responseBody: creditReportResponse.data,
      isDuplicate: false
    });

    console.log('IndiaLends Lead Created:', creditReportResponse.data);
    return responseLog;

  } catch (error) {
    console.error('Error in IndiaLends API:', error.response?.data || error.message);

    // Prepare payload for error logging
    const errorPayload = {
      EmailID: email,
      MobileNumber: phone,
      PanNumber: panNumber || '',
      FirstName: fullName,
      DOB: formatDateForIndiaLends(dateOfBirth),
      ResidencePinCode: pincode
    };

    // Save error log
    const errorLog = await IndiaLendsResponseLog.create({
      leadId: leadId,
      source: source,
      requestPayload: errorPayload,
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

// Helper function to hash mobile number
function hashMobileNumber(mobile) {
  return crypto.createHash('sha256').update(mobile).digest('hex');
}

// Helper function to format date (MM-DD-YYYY)
function formatDateForIndiaLends(date) {
  const d = new Date(date);
  const month = String(d.getMonth() + 1).padStart(2, '0');
  const day = String(d.getDate()).padStart(2, '0');
  const year = d.getFullYear();
  return `${month}-${day}-${year}`;
}

// Helper function to map gender
function mapGenderToIndiaLends(gender) {
  const genderMap = {
    'male': 'male',
    'female': 'female',
    'm': 'male',
    'f': 'female'
  };
  return genderMap[gender?.toLowerCase()] || 'male';
}

// Helper function to map job type to employment type
function mapJobTypeToEmploymentType(jobType) {
  const jobTypeMap = {
    'salaried': 'salaried',
    'self-employed': 'self_employed',
    'selfemployed': 'self_employed',
    'business': 'self_employed'
  };
  return jobTypeMap[jobType?.toLowerCase()] || 'salaried';
}

async function sendToMpokket(lead) {
  const {
    leadId,
    fullName, firstName, lastName, phone, email, dateOfBirth,
    gender, pincode, jobType, businessType, panNumber, salary, 
    address, consent, createdAt, source
  } = lead;

  // Use whichever ID field exists
  const leadIdValue = leadId;

  try {

    const dedupePayload = {
      email_id: Buffer.from(email).toString("base64"),
      mobile_number: Buffer.from(phone).toString("base64")
    };

    const dedupeResponse = await axios.post(
      'https://api.mpkt.in/acquisition-affiliate/v1/dedupe/check',
      dedupePayload,
      {
        headers: {
          'api-key': process.env.MPOKKET_API_KEY || 'DF29C53A361F42FFABCD776A7EFD2',
          'Content-Type': 'application/json'
        }
      }
    );

    console.log("Mpokket Dedupe Response:", dedupeResponse.data);

    // Check if dedupe passed
    if (!dedupeResponse.data.success) {
      console.log("Mpokket Dedupe check failed - Lead already exists");
      
      // Save dedupe failure log
      const dedupeFailLog = await MpokketResponseLog.create({
        leadId: leadIdValue,
        source: source,
        requestPayload: dedupePayload,
        responseStatus: dedupeResponse.status,
        responseBody: dedupeResponse.data,
        step: 'dedupe_check',
        status: 'duplicate'
      });

      return dedupeFailLog;
    }

    // Step 2: Send Lead to Mpokket (if dedupe passed)
    const leadPayload = {
      email_id: email,
      mobile_no: phone,
      Full_name: fullName,
      first_name: firstName || fullName.split(' ')[0],
      middle_name: "", // Optional - extract if needed
      last_name: lastName || fullName.split(' ').slice(1).join(' '),
      date_of_birth: formatDateToDDMMYYYY(dateOfBirth),
      gender: mapGenderToMpokket(gender),
      profession: mapJobTypeToMpokket(jobType),
      additional_info: {
        consent_timestamp: formatConsentTimestamp(createdAt),
        api_consent: "Yes",
        loan_amount: "", // Add if available in lead data
        loan_tenure: "", // Add if available in lead data
        company_type: businessType || "",
        industry_type: "", // Add if available
        company_name: "", // Add if available
        current_designation: "", // Add if available
        company_address: address || "",
        company_pincode: pincode || "",
        company_city: "", // Extract from pincode if needed
        company_state: "", // Extract from pincode if needed
        current_company_working_years: "", // Add if available
        net_monthly_income: salary || "",
        salary_mode: "", // Add if available
        bank_name: "", // Add if available
        pancard: panNumber,
        enter_fname_as_per_pancard: firstName || "",
        enter_lname_as_per_pancard: lastName || "",
        current_address: address || "",
        current_pincode: pincode || "",
        current_city: "", // Extract if needed
        current_state: "", // Extract if needed
        current_residence_type: "", // Add if available
        years_stayed_in_current_address: "", // Add if available
        education_qualification: "", // Add if available
        marital_status: "", // Add if available
        father_name: "", // Add if available
        mother_name: "", // Add if available
        current_total_emi_paid_per_month: "", // Add if available
        active_creditcard_holder: "", // Add if available
        offical_email_id: email,
        college_name: "", // Add if available
        college_pincode: "", // Add if available
        college_city: "", // Add if available
        college_state: "", // Add if available
        college_strength: "", // Add if available
        degree_type: "", // Add if available
        degree_name: "", // Add if available
        degree_specialisation: "", // Add if available
        degree_attendance_type: "", // Add if available
        degree_start_date: "", // Add if available
        degree_end_date: "" // Add if available
      }
    };

    const leadResponse = await axios.post(
      'https://api.mpkt.in/acquisition-affiliate/v1/user',
      leadPayload,
      {
        headers: {
          'api-key': process.env.MPOKKET_API_KEY || 'DF29C53A361F42FFABCD776A7EFD2',
          'Content-Type': 'application/json'
        }
      }
    );

    console.log("Mpokket Lead Response:", leadResponse.data);

    // Step 3: Save Success Response in Database
    const responseLog = await MpokketResponseLog.create({
      leadId: leadIdValue,
      source: source,
      requestPayload: {
        dedupePayload,
        leadPayload
      },
      responseStatus: leadResponse.status,
      responseBody: leadResponse.data,
      step: 'lead_submission',
      status: 'success'
    });

    console.log('Mpokket Lead Created:', leadResponse.data);
    return responseLog;

  } catch (error) {
    console.error('Error in Mpokket API:', error.response?.data || error.message);

    // Prepare payload for error logging
    const errorPayload = {
      email_id: email,
      mobile_no: phone,
      Full_name: fullName,
      pancard: panNumber
    };

    // Save error log
    const errorLog = await MpokketResponseLog.create({
      leadId: leadIdValue,
      source: source,
      requestPayload: errorPayload,
      responseStatus: error.response?.status || 500,
      responseBody: error.response?.data || {
        message: error.message || 'Unknown error',
        error: true
      },
      errorDetails: {
        message: error.message,
        code: error.code,
        stack: error.stack
      },
      step: error.config?.url?.includes('dedupe') ? 'dedupe_check' : 'lead_submission',
      status: 'error'
    });

    return errorLog;
  }
}

// Helper Functions
function formatDateToDDMMYYYY(date) {
  if (!date) return "";
  const d = new Date(date);
  const day = String(d.getDate()).padStart(2, '0');
  const month = String(d.getMonth() + 1).padStart(2, '0');
  const year = d.getFullYear();
  return `${day}-${month}-${year}`;
}

function formatConsentTimestamp(date) {
  if (!date) return "";
  const d = new Date(date);
  const year = d.getFullYear();
  const month = String(d.getMonth() + 1).padStart(2, '0');
  const day = String(d.getDate()).padStart(2, '0');
  const hours = String(d.getHours()).padStart(2, '0');
  const minutes = String(d.getMinutes()).padStart(2, '0');
  const seconds = String(d.getSeconds()).padStart(2, '0');
  const milliseconds = String(d.getMilliseconds()).padStart(3, '0');
  return `${year}-${month}-${day} ${hours}:${minutes}:${seconds}.${milliseconds}`;
}

function mapGenderToMpokket(gender) {
  if (!gender) return "";
  const g = gender.toLowerCase();
  if (g === 'male' || g === 'm') return 'male';
  if (g === 'female' || g === 'f') return 'female';
  return gender;
}

function mapJobTypeToMpokket(jobType) {
  if (!jobType) return "";
  const j = jobType.toLowerCase();
  if (j.includes('salaried') || j.includes('employee')) return 'salaried';
  if (j.includes('self') || j.includes('business')) return 'self-employed';
  if (j.includes('student')) return 'student';
  return jobType;
}

async function sendToCrmPaisa(lead) {
  const {
    leadId, fullName, phone, email, dateOfBirth,
    panNumber, jobType, salary, pincode, source
  } = lead;

  // Validation 1: Pincode check
  const validPincodes = pinCodeDataCRMPaisa.map((row) => parseInt(row.pincode, 10));
  if (!validPincodes.includes(parseInt(pincode))) {
    console.log(`Pincode ${pincode} not valid for CrmPaisa. Skipping.`);
    
    const validationLog = await CrmPaisaResponseLog.create({
      leadId: leadId,
      source: source,
      requestPayload: {
        mobile: String(phone),
        first_name: fullName.split(' ')[0],
        last_name: fullName.split(' ')[1] || fullName.split(' ')[0],
        email: String(email),
        employment_type: String(jobType),
        pan: String(panNumber),
        dob: formatToYYYYMMDD(dateOfBirth),
        pincode: String(pincode),
        monthly_income: String(salary),
      },
      responseStatus: 'not-valid',
      responseBody: { message: 'Invalid pincode', reason: 'Pincode not in approved list' },
    });
    
    return validationLog;
  }

  // Validation 2: Job type must be salaried
  const normalizedJobType = String(jobType).toLowerCase().trim();
  if (normalizedJobType !== 'salaried') {
    console.log(`Job type "${jobType}" is not salaried. Skipping.`);
    
    const validationLog = await CrmPaisaResponseLog.create({
      leadId: leadId,
      source: source,
      requestPayload: {
        mobile: String(phone),
        first_name: fullName.split(' ')[0],
        last_name: fullName.split(' ')[1] || fullName.split(' ')[0],
        email: String(email),
        employment_type: String(jobType),
        pan: String(panNumber),
        dob: formatToYYYYMMDD(dateOfBirth),
        pincode: String(pincode),
        monthly_income: String(salary),
      },
      responseStatus: 'not-valid',
      responseBody: { message: 'Invalid employment type', reason: 'Only salaried individuals are accepted' },
    });
    
    return validationLog;
  }

  // Validation 3: Minimum salary requirement (₹30,000)
  const monthlySalary = parseFloat(salary);
  if (isNaN(monthlySalary) || monthlySalary < 30000) {
    console.log(`Salary ${salary} does not meet minimum requirement of ₹30,000. Skipping.`);
    
    const validationLog = await CrmPaisaResponseLog.create({
      leadId: leadId,
      source: source,
      requestPayload: {
        mobile: String(phone),
        first_name: fullName.split(' ')[0],
        last_name: fullName.split(' ')[1] || fullName.split(' ')[0],
        email: String(email),
        employment_type: String(jobType),
        pan: String(panNumber),
        dob: formatToYYYYMMDD(dateOfBirth),
        pincode: String(pincode),
        monthly_income: String(salary),
      },
      responseStatus: 'not-valid',
      responseBody: { message: 'Insufficient salary', reason: 'Minimum monthly salary requirement is ₹30,000' },
    });
    
    return validationLog;
  }

  // Validation 4: Age requirement (25-50 years)
  const dob = new Date(dateOfBirth);
  const today = new Date();
  let age = today.getFullYear() - dob.getFullYear();
  const monthDiff = today.getMonth() - dob.getMonth();
  
  if (monthDiff < 0 || (monthDiff === 0 && today.getDate() < dob.getDate())) {
    age--;
  }

  if (age < 25 || age > 50) {
    console.log(`Age ${age} is outside the allowed range (25-50 years). Skipping.`);
    
    const validationLog = await CrmPaisaResponseLog.create({
      leadId: leadId,
      source: source,
      requestPayload: {
        mobile: String(phone),
        first_name: fullName.split(' ')[0],
        last_name: fullName.split(' ')[1] || fullName.split(' ')[0],
        email: String(email),
        employment_type: String(jobType),
        pan: String(panNumber),
        dob: formatToYYYYMMDD(dateOfBirth),
        pincode: String(pincode),
        monthly_income: String(salary),
      },
      responseStatus: 'not-valid',
      responseBody: { message: 'Invalid age', reason: 'Age must be between 25 and 50 years', calculatedAge: age },
    });
    
    return validationLog;
  }

  // All validations passed - proceed with API call
  const leadIdValue = leadId;
  const externalApiUrl = `https://api.crmpaisa.com/affiliates`;
  const payload = {
    mobile: String(phone),
    first_name: fullName.split(' ')[0],
    last_name: fullName.split(' ')[1] ? fullName.split(' ')[1] : fullName.split(' ')[0],
    email: String(email),
    employment_type: String(jobType),
    pan: String(panNumber),
    dob: formatToYYYYMMDD(dateOfBirth),
    pincode: String(pincode),
    monthly_income: String(salary),
    utm_source: "Ratecut",
    utm_campaign: "Ratecut",
    utm_medium: "Ratecut",
    utm_term: ""
  };

  try {
    const apiResponse = await axios.post(externalApiUrl, payload, {
      headers: { 
        'Auth': 'ZTI4MTU1MzE4NWQ2MGQyZTFhNWM0NGU3M2UzMmM3MDM=', 
        'Content-Type': 'application/json'
      },
    });
    
    console.log("CrmPaisa response:", apiResponse.data);

    // Save API response using DynamoDB
    const responseLog = await CrmPaisaResponseLog.create({
      leadId: leadIdValue,
      source: source,
      requestPayload: payload,
      responseStatus: apiResponse.status,
      responseBody: apiResponse.data,
    });

    return responseLog;
  } catch (error) {
    console.error('Error sending lead to CrmPaisa API:', error);
    
    const errorLog = await CrmPaisaResponseLog.create({
      leadId: leadIdValue,
      source: source,
      requestPayload: payload,
      responseStatus: error.response?.status || 500,
      responseBody: error.response?.data || { message: 'Unknown error' },
    });
    
    return errorLog;
  }
}

function readExcelFileCRMPaisa() {
  const workbook = xlsx.readFile(path.join(__dirname, './EP_pincode.xlsx'));
  const sheetName = workbook.SheetNames[0];
  return xlsx.utils.sheet_to_json(workbook.Sheets[sheetName]);
}

const pinCodeDataCRMPaisa = readExcelFileCRMPaisa();

module.exports = {
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
};
const fs = require('fs');
const path = require('path');
const csv = require('csv-parse/sync');
const XLSX = require('xlsx');

const Lead = require('../models/leadModel');
const { docClient } = require('../dynamodb');
const { QueryCommand } = require('@aws-sdk/lib-dynamodb');

const RESPONSELOG_SOURCES = require('../config/registry').RESPONSELOG_SOURCES;

const LENDER_CONFIGS = {

  ovly: {
    displayName: 'Ovly (SmartCoin)',
    lenderKey: 'OVLY',
    allowedExtensions: ['.csv', '.xlsx', '.xls'],
    sheetName: null,
    idType: 'responselog',
    tableName: 'ovly_response_logs',
    // Only query logs where OVLY accepted the lead
    successFilterExpr: {
      FilterExpression: '#rs = :rs',
      ExpressionAttributeNames: { '#rs': 'responseStatus' },
      ExpressionAttributeValues: { ':rs': 'success' },
    },
    // OVLY echoes their own lead_id in responseBody — extract it to match against MIS
    extractLenderIdFromLog: (log) => {
      const body = tryParseJSON(log.responseBody);
      return body?.lead_id || body?.leadId?.S || body?.leadId || null;
    },
    extractId:      (row) => pick(row, 'lead_id', 'leadId', 'LeadId', 'LEAD_ID'),
    extractStatus:  (row) => pick(row, 'rejection_reason', 'status', 'Status') || 'Unknown',
    successStatuses: ['Unlocked', 'disbursed', 'Disbursed'],
    extractDetails: (row) => ({
      unlockAmount:        pick(row, 'unlock_amount'),
      appliedDate:         pick(row, 'applied_date'),
      kycCompletedDate:    pick(row, 'kyc_completed_date'),
      approvedDate:        pick(row, 'approved_date'),
      emandateDoneAt:      pick(row, 'emandate_done_at'),
      agreementSignedDate: pick(row, 'agreement_signed_date'),
      loanAmount:          pick(row, 'loan_amount'),
      loanDisbursedDate:   pick(row, 'loan_disbursed_date'),
    }),
  },

  lendingplate: {
    displayName: 'Lending Plate',
    lenderKey: 'LendingPlate',
    allowedExtensions: ['.csv', '.xlsx', '.xls'],
    sheetName: null,
    idType: 'responselog',
    tableName: 'lending_plate_response_logs',
    // Only query logs where LP accepted/disbursed
    successFilterExpr: {
      FilterExpression: '#rs = :rs',
      ExpressionAttributeNames: { '#rs': 'responseStatus' },
      ExpressionAttributeValues: { ':rs': 'Success' },
    },
    // LP stores our ref_id (= our leadId) in the requestPayload we sent
    extractLenderIdFromLog: (log) => {
      const payload = tryParseJSON(log.requestPayload);
      return payload?.ref_id?.S || payload?.ref_id || payload?.reference_id || null;
    },
    extractId:      (row) => pick(row, 'Reference ID', 'reference_id', 'referenceId', 'ref_id'),
    extractStatus:  (row) => pick(row, 'LP Status', 'lp_status', 'lpStatus') || 'Unknown',
    successStatuses: ['DISBURSED', 'SANCTION', 'SANCTION-ACCEPTED'],
    extractDetails: (row) => ({
      lpLeadId:         pick(row, 'LP Lead ID', 'lp_lead_id'),
      lpLeadDate:       pick(row, 'LP Lead Date', 'lp_lead_date'),
      lpIncomeType:     pick(row, 'LP Income type', 'lp_income_type'),
      lpRejectReason:   pick(row, 'LP Reject Reason', 'lp_reject_reason'),
      sanctionedAmount: pick(row, 'Sanctioned Amount', 'sanctioned_amount'),
      sanctionedDate:   pick(row, 'Sanctioned Date', 'sanctioned_date'),
      disbursedAmount:  pick(row, 'Disbursed Amount', 'disbursed_amount'),
      disbursedDate:    pick(row, 'Disbursed Date', 'disbursed_date'),
    }),
  },

  // ── New MIS-only lenders (match via leads table) ──────────────────────────

  zype: {
    displayName: 'Zype',
    lenderKey: 'ZYPE',
    allowedExtensions: ['.csv', '.xlsx', '.xls'],
    sheetName: null,
    idType: 'phone',
    successStatuses: ['approved', 'Approved', 'ACCEPT', 'disbursed', 'Disbursed'],
    extractId: (row) => normalizePhone(String(pick(row, 'mobile_number', 'phone', 'Mobile') || '')),
    extractStatus: (row) => pick(row, 'approval_status', 'l2_status', 'l1_status') || 'Unknown',
    extractDetails: (row) => ({
      l1Status:        pick(row, 'l1_status'),
      l2Status:        pick(row, 'l2_status'),
      creditLimit:     pick(row, 'credit_limit'),
      principalAmount: pick(row, 'principal_amount'),
      disbursedOn:     pick(row, 'disbursedon_date'),
      rejectionReason: pick(row, 'rejection_reason'),
      dropOffStep:     pick(row, 'drop_off_step'),
      customerName:    pick(row, 'customer_name'),
      employmentType:  pick(row, 'employment_type'),
      apiPushDate:     pick(row, 'api_push_date'),
    }),
  },

  cashvia: {
    displayName: 'Cashvia',
    lenderKey: 'CASHVIA',
    allowedExtensions: ['.xlsx', '.xls'],
    sheetName: 'Sheet1',
    idType: 'leadId',             // MIS has our UUID in "Lead ID" column
    successStatuses: ['Approved', 'Disbursed'],
    extractId:      (row) => pick(row, 'Lead ID', 'lead_id', 'LeadId', 'leadId'),
    extractStatus:  (row) => pick(row, 'Status', 'status') || 'Unknown',
    extractDetails: (row) => ({
      approvalAmount:  pick(row, 'Approval Amount', 'approval_amount'),
      disbursedAmount: pick(row, 'Disbursal Amount', 'disbursal_amount'),
      cibilScore:      pick(row, 'CIBIL Score', 'cibil_score'),
      decision:        pick(row, 'Decision', 'decision'),
      remark:          pick(row, 'Remark', 'remark'),
      agentName:       pick(row, 'Agent Name', 'agent_name'),
    }),
  },

  digicredit: {
    displayName: 'Digicredit',
    lenderKey: 'DIGICREDIT',
    allowedExtensions: ['.xlsx', '.xls'],
    sheetName: 'Sheet1',
    idType: 'leadId',             // MIS has our UUID in "lead_id" column
    successStatuses: ['Approved', 'Disbursed'],
    extractId:      (row) => pick(row, 'lead_id', 'Lead ID', 'leadId'),
    extractStatus:  (row) => pick(row, 'status', 'Status') || 'Unknown',
    extractDetails: (row) => ({
      approvalAmount:  pick(row, 'approval_amount', 'Approval Amount'),
      disbursedAmount: pick(row, 'disbursal_amount', 'Disbursal Amount'),
      disbursedDate:   pick(row, 'disbursal_date', 'Disbursal Date'),
      remark:          pick(row, 'latest_call_remark', 'Latest Call Remark'),
      agentName:       pick(row, 'agent_name', 'Agent Name'),
      score:           pick(row, 'score', 'Score'),
    }),
  },

  tap4credit: {
    displayName: 'Tap4Credit',
    lenderKey: 'TAP4CREDIT',
    allowedExtensions: ['.xlsx', '.xls'],
    sheetName: 'Sheet1',
    idType: 'leadId',             // MIS has our UUID in "leadId" column
    successStatuses: ['Approved', 'Disbursed'],
    extractId:      (row) => pick(row, 'leadId', 'lead_id', 'Lead ID'),
    extractStatus:  (row) => pick(row, 'status', 'Status') || 'Unknown',
    extractDetails: (row) => ({
      approvalAmount:  pick(row, 'approvalAmount', 'approval_amount'),
      disbursedAmount: pick(row, 'disbursalAmount', 'disbursal_amount'),
      disbursedDate:   pick(row, 'disbursalDate', 'disbursal_date'),
      cibilScore:      pick(row, 'cibil_score'),
      remark:          pick(row, 'remarks', 'Remarks'),
      agentName:       pick(row, 'AgentName', 'agent_name'),
    }),
  },

  speedoloan: {
    displayName: 'SpeedoLoan',
    lenderKey: 'SPEEDOLOAN',
    allowedExtensions: ['.csv', '.xlsx', '.xls'],
    sheetName: null,
    idType: 'phone',
    successStatuses: ['Disbursed', 'Approved', 'Sanctioned'],
    extractId: (row) => normalizePhone(String(pick(row, 'PhoneNumber', 'phone', 'Mobile') || '')),
    extractStatus:  (row) => pick(row, 'user_status', 'Status', 'status') || 'Unknown',
    extractDetails: (row) => ({
      approvalAmount:  pick(row, 'ApprovalAmount', 'approval_amount'),
      disbursedAmount: pick(row, 'DisbursalAmount', 'disbursal_amount'),
      disbursedDate:   pick(row, 'DisbursedAt', 'disbursed_at'),
      rejectReason:    pick(row, 'RejectionReason', 'rejection_reason'),
      empType:         pick(row, 'EmpType', 'emp_type'),
    }),
  },

  creditsea: {
    displayName: 'CreditSea',
    lenderKey: 'CreditSea',        // same key as existing API push lender
    allowedExtensions: ['.xlsx', '.xls'],
    sheetName: 'MIS Reports',      // use MIS Reports sheet only
    idType: 'phone',
    successStatuses: ['Disbursed', 'Approved'],
    extractId: (row) => normalizePhone(String(pick(row, 'phoneNumber', 'phone', 'Phone') || '')),
    extractStatus:  (row) => pick(row, 'loanStatus', 'loan_status', 'status') || 'Unknown',
    extractDetails: (row) => ({
      disbursedAmount:   pick(row, 'disbursedAmount', 'disbursed_amount'),
      disbursedAt:       pick(row, 'disbursedAt', 'disbursed_at'),
      rejectedAt:        pick(row, 'rejectedAt', 'rejected_at'),
      rejectionReason:   pick(row, 'rejectionReason', 'rejection_reason'),
      lastStage:         pick(row, 'LastStage', 'last_stage'),
      applicationNumber: pick(row, 'applicationNumber', 'application_number'),
    }),
  },

  paisaboxx: {
    displayName: 'Paisaboxx',
    lenderKey: 'PAISABOXX',
    allowedExtensions: ['.xlsx', '.xls'],
    sheetName: 'LeadData',         // use LeadData sheet only (ignore DisbursalSummary, DisbursalData)
    idType: 'phone',
    successStatuses: ['Disbursed', 'Approve', 'Proceed to Bank'],
    extractId: (row) => normalizePhone(String(pick(row, 'Mobile Number', 'mobile_number', 'mobile', 'Phone') || '')),
    extractStatus:  (row) => pick(row, 'Status', 'status') || 'Unknown',
    extractDetails: (row) => ({
      loanAmount:    pick(row, 'Loan Amount', 'loan_amount'),
      disbursedDate: pick(row, 'Disbursed Date', 'disbursed_date'),
      rejectDate:    pick(row, 'Reject Date', 'reject_date'),
      rejectReason:  pick(row, 'Reject Reason', 'reject_reason'),
      profession:    pick(row, 'Profession', 'profession'),
      salary:        pick(row, 'Salary', 'salary'),
    }),
  },

  fatakpay_dcl: {
    displayName: 'FatakPay DCL',
    lenderKey: 'FATAKPAY',
    allowedExtensions: ['.xlsx', '.xls'],
    sheetName: 'Raw_Data',
    idType: 'responselog',
    tableName: 'fatakpay_response_logs',
    // Only query logs where FatakPay accepted the lead
    successFilterExpr: {
      FilterExpression: 'contains(#rb, :msg)',
      ExpressionAttributeNames: { '#rb': 'responseBody' },
      ExpressionAttributeValues: { ':msg': 'You are eligible.' },
    },
    // FatakPay returns lapp_id in their response body
    extractLenderIdFromLog: (log) => {
      const body = tryParseJSON(log.responseBody);
      return body?.lapp_id || body?.data?.lapp_id || null;
    },
    extractId:      (row) => pick(row, 'lapp_id'),
    extractStatus:  (row) => pick(row, 'stage_name', 'Stage Name') || 'Unknown',
    successStatuses: ['Disbursement', 'Disbursed'],
    extractDetails: (row) => ({
      lappId:    pick(row, 'lapp_id'),
      remarks:   pick(row, 'remarks'),
      payable:   pick(row, 'payable'),
      leadMonth: pick(row, 'lead_month'),
    }),
  },

  fatakpay_pl: {
    displayName: 'FatakPay PL',
    lenderKey: 'FATAKPAYPL',
    allowedExtensions: ['.xlsx', '.xls'],
    sheetName: 'Affiliate Data',
    idType: 'responselog',
    tableName: 'fatakpay_pl__response_logs',
    // Only query logs where FatakPay PL accepted the lead
    successFilterExpr: {
      FilterExpression: 'contains(#rb, :msg)',
      ExpressionAttributeNames: { '#rb': 'responseBody' },
      ExpressionAttributeValues: { ':msg': 'You are eligible.' },
    },
    // FatakPay PL also returns lapp_id in their response body
    extractLenderIdFromLog: (log) => {
      const body = tryParseJSON(log.responseBody);
      return body?.lapp_id || body?.data?.lapp_id || null;
    },
    extractId:      (row) => pick(row, 'lapp_id'),
    extractStatus:  (row) => pick(row, 'latest_emi_stage_name') || 'Unknown',
    successStatuses: ['Disbursement', 'Disbursed'],
    extractDetails: (row) => ({
      lappId:        pick(row, 'lapp_id'),
      fullName:      pick(row, 'full_name'),
      disbursedDate: pick(row, 'disb_dt'),
      loanProposed:  pick(row, 'loan_amount_proposed'),
      loanProvided:  pick(row, 'loan_amount_provided'),
      rejectReason:  pick(row, 'final_reject_reason'),
      city:          pick(row, 'Final_City'),
      state:         pick(row, 'Final_State'),
    }),
  },

  herofincorp: {
    displayName: 'Hero Fincorp',
    lenderKey: 'HEROFINCORP',
    allowedExtensions: ['.xlsx', '.xls'],
    sheetName: 'Sheet1',
    idType: 'none',
    // ⚠️ Hero Fincorp MIS has no phone column; customer_id is a hashed string.
    // To enable matching: store Hero Fincorp's AppID / customer_id during the API push.
    successStatuses: ['DISBURSED', 'Disbursed'],
    extractId:      (row) => null,
    extractStatus:  (row) => pick(row, 'Stage', 'stage') || 'Unknown',
    extractDetails: (row) => ({
      appId:            pick(row, 'AppID', 'app_id'),
      customerId:       pick(row, 'customer_id'),
      disbursedAmount:  pick(row, 'Disbursed_Amount', 'disbursed_amount'),
      disbursedDate:    pick(row, 'Disbursement_Date', 'disbursement_date'),
      sanctionedAmount: pick(row, 'sanctioned_loan_amount'),
      bureauDecision:   pick(row, 'Bureau_Decision'),
      stage:            pick(row, 'Stage'),
      substage:         pick(row, 'substage'),
    }),
  },

  prefr: {
    displayName: 'Prefr',
    lenderKey: 'PREFR',
    allowedExtensions: ['.xlsx', '.xls'],
    sheetName: 'Sheet1',
    idType: 'phone',
    successStatuses: ['disbursed', 'Disbursed', 'sanctioned', 'Sanctioned'],
    extractId: (row) => normalizePhone(String(pick(row, 'mobilenumber', 'mobile', 'phone', 'Phone') || '')),
    extractStatus:  (row) => pick(row, 'loanstatus', 'loan_status', 'status') || 'Unknown',
    extractDetails: (row) => ({
      disbursedAmount:  pick(row, 'disbursalamount', 'disbursed_amount'),
      disbursedDate:    pick(row, 'disbursementdate', 'disbursed_date'),
      offerAmount:      pick(row, 'offeramount', 'offer_amount'),
      rejectReason:     pick(row, 'reject_reason'),
      stageTag:         pick(row, 'stage_tag'),
      goodQualityLead:  pick(row, 'good_quality_lead'),
      income:           pick(row, 'income'),
      employmentType:   pick(row, 'employmenttype'),
    }),
  },
};

// ─── Utilities ────────────────────────────────────────────────────────────────

function pick(row, ...keys) {
  for (const key of keys) {
    const val = row[key];
    if (val !== undefined && val !== null && val !== '' && val !== 'No Input' && val !== 'NULL') {
      return String(val).trim();
    }
    // case-insensitive fallback
    const found = Object.keys(row).find(k => k.toLowerCase() === key.toLowerCase());
    if (found) {
      const v = row[found];
      if (v !== undefined && v !== null && v !== '' && v !== 'No Input' && v !== 'NULL') {
        return String(v).trim();
      }
    }
  }
  return null;
}


function normalizePhone(phone) {
  if (!phone) return null;
  const digits = phone.replace(/\D/g, '');
  if (digits.length === 12 && digits.startsWith('91')) return digits.slice(2);
  if (digits.length === 11 && digits.startsWith('0'))  return digits.slice(1);
  if (digits.length === 10) return digits;
  return null;
}

function parseFile(filePath, originalFilename, config) {
  const ext = path.extname(originalFilename).toLowerCase();
  let rows = [];

  if (ext === '.csv') {
    const content = fs.readFileSync(filePath, 'utf-8');
    rows = csv.parse(content, { columns: true, skip_empty_lines: true, trim: true });
  } else {
    const workbook = XLSX.readFile(filePath, { cellDates: true });
    let sheetName = config.sheetName;
    if (!sheetName || !workbook.SheetNames.includes(sheetName)) {
      sheetName = workbook.SheetNames[0];
    }
    rows = XLSX.utils.sheet_to_json(workbook.Sheets[sheetName], { defval: null });
  }

  return rows;
}

// ─── Utilities ────────────────────────────────────────────────────────────────

function tryParseJSON(val) {
  if (!val) return null;
  if (typeof val === 'object') return val;
  try { return JSON.parse(val); } catch { return null; }
}

// ─── Build lenderLeadId → ourLeadId map from successful response logs only ────
//
// Queries each source via source-createdAt-index GSI.
// FilterExpression pushes success filtering to DynamoDB server-side:
//   - RCUs stay the same (DynamoDB charges before filter) but returned payload
//     is much smaller → less data transfer + faster in-memory processing.
//
async function buildResponseLogMap(config) {
  const map = new Map(); // lenderInternalId → ourLeadId

  for (const source of RESPONSELOG_SOURCES) {
    let lastKey = null;
    do {
      const params = {
        TableName: config.tableName,
        IndexName: 'source-createdAt-index',
        KeyConditionExpression: '#src = :src',
        ExpressionAttributeNames: {
          '#src': 'source',
          ...config.successFilterExpr.ExpressionAttributeNames,
        },
        ExpressionAttributeValues: {
          ':src': source,
          ...config.successFilterExpr.ExpressionAttributeValues,
        },
        FilterExpression: config.successFilterExpr.FilterExpression,
        // Only fetch the fields we actually need → saves bandwidth
        ProjectionExpression: 'leadId, responseBody, requestPayload',
      };
      if (lastKey) params.ExclusiveStartKey = lastKey;

      const res = await docClient.send(new QueryCommand(params));

      for (const log of (res.Items || [])) {
        if (!log.leadId) continue;
        const lenderLeadId = config.extractLenderIdFromLog(log);
        if (lenderLeadId) {
          map.set(String(lenderLeadId), log.leadId);
        }
      }

      lastKey = res.LastEvaluatedKey;
    } while (lastKey);
  }

  return map;
}

// ─── MIS → leads table sync ───────────────────────────────────────────────────

async function syncMISToLeads(rows, config) {
  const result = { totalInFile: rows.length, matched: 0, updated: 0, successful: 0, unmatched: 0, errors: [] };

  // Build lenderLeadId → ourLeadId map upfront (one-time query, success-only)
  let responseLogMap = null;
  if (config.idType === 'responselog') {
    console.log(`[${config.lenderKey}] Building response log map from successful entries...`);
    responseLogMap = await buildResponseLogMap(config);
    console.log(`[${config.lenderKey}] Response log map built: ${responseLogMap.size} entries`);
  }

  for (const row of rows) {
    try {
      const identifier = config.extractId(row);
      if (!identifier) { result.unmatched++; continue; }

      let lead;
      if (config.idType === 'responselog') {
        const ourLeadId = responseLogMap.get(String(identifier));
        if (!ourLeadId) { result.unmatched++; continue; }
        lead = await Lead.findById(ourLeadId);
      } else if (config.idType === 'phone') {
        lead = await Lead.findByPhone(identifier);
      } else {
        lead = await Lead.findById(identifier);
      }

      if (!lead) { result.unmatched++; continue; }
      result.matched++;

      const status    = config.extractStatus(row);
      const details   = config.extractDetails(row);
      const isSuccess = config.successStatuses.some(s => s.toLowerCase() === (status || '').toLowerCase());

      // Build lenderStatuses entry — stored as flat DynamoDB attribute "lenderStatuses.CASHVIA" etc.
      const statusEntry = { status, ...details, isSuccess, updatedAt: new Date().toISOString() };
      // Strip nulls
      Object.keys(statusEntry).forEach(k => {
        if (statusEntry[k] === null || statusEntry[k] === undefined) delete statusEntry[k];
      });

      const updates = { [`lenderStatuses.${config.lenderKey}`]: statusEntry };

      // Append to successfulLenders list if this lender confirmed the lead
      if (isSuccess) {
        const existing = Array.isArray(lead.successfulLenders) ? lead.successfulLenders : [];
        if (!existing.includes(config.lenderKey)) {
          updates.successfulLenders = [...existing, config.lenderKey];
        }
        result.successful++;
      }

      await Lead.updateByIdNoValidation(lead.leadId, updates);
      result.updated++;
    } catch (err) {
      console.error(`[${config.lenderKey}] Row processing error:`, err.message);
      result.errors.push({ identifier: config.extractId ? config.extractId(row) : '?', error: err.message });
    }
  }

  return result;
}

// ─── Route handlers ───────────────────────────────────────────────────────────

exports.getLenders = (req, res) => {
  const lenders = Object.keys(LENDER_CONFIGS).map(key => {
    const c = LENDER_CONFIGS[key];
    return {
      key,
      displayName:       c.displayName,
      lenderKey:         c.lenderKey || null,
      idType:            c.idType,
      sheetName:         c.sheetName || 'first sheet',
      allowedExtensions: c.allowedExtensions,
      successStatuses:   c.successStatuses || [],
      note: c.idType === 'none'
        ? 'Auto-matching not available — MIS file has no phone/leadId column'
        : null,
    };
  });
  res.json({ success: true, lenders });
};

exports.uploadAndSync = async (req, res) => {
  let filePath = null;

  try {
    const { lender } = req.body;
    const file = req.file;

    if (!file)   return res.status(400).json({ success: false, error: 'No file provided' });
    filePath = file.path;

    if (!lender) return res.status(400).json({ success: false, error: 'Lender parameter is required' });

    const config = LENDER_CONFIGS[lender.toLowerCase()];
    if (!config) {
      return res.status(400).json({
        success: false,
        error: `Unknown lender: ${lender}. Available: ${Object.keys(LENDER_CONFIGS).join(', ')}`,
      });
    }

    const ext = path.extname(file.originalname).toLowerCase();
    if (!config.allowedExtensions.includes(ext)) {
      return res.status(400).json({
        success: false,
        error: `File type ${ext} not allowed for ${config.displayName}. Allowed: ${config.allowedExtensions.join(', ')}`,
      });
    }

    const rows = parseFile(filePath, file.originalname, config);
    console.log(`[${lender}] Parsed ${rows.length} rows from "${file.originalname}" (sheet: ${config.sheetName || 'first'})`);

    if (fs.existsSync(filePath)) { fs.unlinkSync(filePath); filePath = null; }

    const syncResult = await syncMISToLeads(rows, config);
    syncResult.lender = config.displayName;
    syncResult.tableName = 'leads';

    res.json({ success: true, ...syncResult });

  } catch (error) {
    console.error('[uploadAndSync] Error:', error);
    if (filePath && fs.existsSync(filePath)) fs.unlinkSync(filePath);
    res.status(500).json({ success: false, error: error.message || 'Internal server error' });
  }
};


exports.LENDER_CONFIGS = LENDER_CONFIGS;

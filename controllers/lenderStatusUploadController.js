const fs = require('fs');
const path = require('path');
const csv = require('csv-parse/sync');
const XLSX = require('xlsx');

const Lead = require('../models/leadModel');
const LeadSuccess = require('../models/leadSuccessModel');

// ─── Lender Configs ──────────────────────────────────────────────────────────
//
// idType:
//   'responselog' → legacy: match via lender response log table (Ovly, LendingPlate)
//   'phone'       → look up lead in leads table by phone number
//   'leadId'      → look up lead in leads table by our UUID (leadId)
//   'none'        → cannot auto-match (no phone/leadId in MIS file)
//
// sheetName: which Excel sheet to use (null = first sheet / CSV)
//
// successStatuses: array of status strings that mean the lender accepted the lead
// extractId(row): returns the phone or leadId string from a parsed row
// extractStatus(row): returns status string from a row
// extractDetails(row): returns extra detail fields to store alongside status
// ─────────────────────────────────────────────────────────────────────────────

const LENDER_CONFIGS = {

  // ── Legacy response-log lenders ───────────────────────────────────────────

  ovly: {
    displayName: 'Ovly (SmartCoin)',
    tableName: 'ovly_response_logs',
    model: require('../models/ovlyResponseLog'),
    allowedExtensions: ['.csv', '.xlsx', '.xls'],
    sheetName: null,
    idType: 'responselog',
    columnMapping: {
      leadId: ['lead_id', 'leadId', 'LeadId', 'LEAD_ID'],
      rejectionReason: ['rejection_reason', 'rejectionReason', 'RejectionReason', 'REJECTION_REASON'],
      unlockAmount: ['unlock_amount', 'unlockAmount', 'UnlockAmount'],
      appliedDate: ['applied_date', 'appliedDate', 'AppliedDate'],
      kycCompletedDate: ['kyc_completed_date', 'kycCompletedDate', 'KycCompletedDate'],
      approvedDate: ['approved_date', 'approvedDate', 'ApprovedDate'],
      emandateDoneAt: ['emandate_done_at', 'emandateDoneAt', 'EmandateDoneAt'],
      agreementSignedDate: ['agreement_signed_date', 'agreementSignedDate', 'AgreementSignedDate'],
      loanAmount: ['loan_amount', 'loanAmount', 'LoanAmount'],
      loanDisbursedDate: ['loan_disbursed_date', 'loanDisbursedDate', 'LoanDisbursedDate'],
    },
    successStatuses: ['Unlocked', 'disbursed'],
    extractLeadId: (responseBody) => {
      if (!responseBody) return null;
      let parsed = responseBody;
      if (typeof responseBody === 'string') {
        try { parsed = JSON.parse(responseBody); } catch { return null; }
      }
      if (parsed.leadId && typeof parsed.leadId === 'object' && parsed.leadId.S) return parsed.leadId.S;
      if (typeof parsed.leadId === 'string') return parsed.leadId;
      return null;
    },
    filterUpdateData: (rowData) => {
      const filtered = { lead_id: rowData.leadId, rejection_reason: rowData.rejectionReason };
      if (rowData.rejectionReason === 'Unlocked') {
        if (rowData.unlockAmount)        filtered.unlock_amount = rowData.unlockAmount;
        if (rowData.appliedDate)         filtered.applied_date = rowData.appliedDate;
        if (rowData.kycCompletedDate)    filtered.kyc_completed_date = rowData.kycCompletedDate;
        if (rowData.approvedDate)        filtered.approved_date = rowData.approvedDate;
        if (rowData.emandateDoneAt)      filtered.emandate_done_at = rowData.emandateDoneAt;
        if (rowData.agreementSignedDate) filtered.agreement_signed_date = rowData.agreementSignedDate;
        if (rowData.loanAmount)          filtered.loan_amount = rowData.loanAmount;
        if (rowData.loanDisbursedDate)   filtered.loan_disbursed_date = rowData.loanDisbursedDate;
      }
      return filtered;
    },
    updateMethod: 'updateStatusWithData',
  },

  lendingplate: {
    displayName: 'Lending Plate',
    tableName: 'lending_plate_response_logs',
    model: require('../models/leadingPlateResponseLog'),
    allowedExtensions: ['.csv', '.xlsx', '.xls'],
    sheetName: null,
    idType: 'responselog',
    columnMapping: {
      referenceId: ['Reference ID', 'reference_id', 'referenceId', 'ref_id', 'RefId'],
      lpLeadId: ['LP Lead ID', 'lp_lead_id', 'lpLeadId', 'LPLeadID'],
      lpLeadDate: ['LP Lead Date', 'lp_lead_date', 'lpLeadDate', 'LPLeadDate'],
      lpStatus: ['LP Status', 'lp_status', 'lpStatus', 'LPStatus'],
      lpIncomeType: ['LP Income type', 'lp_income_type', 'lpIncomeType', 'LPIncomeType'],
      lpRejectReason: ['LP Reject Reason', 'lp_reject_reason', 'lpRejectReason', 'LPRejectReason'],
      api1HitDate: ['API 1 Hit Date', 'api1_hit_date', 'api1HitDate', 'API1HitDate'],
      api1Response: ['API 1 Response', 'api1_response', 'api1Response', 'API1Response'],
      api1Reason: ['API 1 Reason', 'api1_reason', 'api1Reason', 'API1Reason'],
      api2HitDate: ['API 2 Hit Date', 'api2_hit_date', 'api2HitDate', 'API2HitDate'],
      api2Response: ['API 2 Response', 'api2_response', 'api2Response', 'API2Response'],
      api2Reason: ['API 2 Reason', 'api2_reason', 'api2Reason', 'API2Reason'],
      sanctionedAmount: ['Sanctioned Amount', 'sanctioned_amount', 'sanctionedAmount', 'SanctionedAmount'],
      sanctionedDate: ['Sanctioned Date', 'sanctioned_date', 'sanctionedDate', 'SanctionedDate'],
      disbursedAmount: ['Disbursed Amount', 'disbursed_amount', 'disbursedAmount', 'DisbursedAmount'],
      disbursedDate: ['Disbursed Date', 'disbursed_date', 'disbursedDate', 'DisbursedDate'],
      afMediaSource: ['AF Media Source', 'af_media_source', 'afMediaSource', 'AFMediaSource'],
      afPartner: ['AF Partner', 'af_partner', 'afPartner', 'AFPartner'],
    },
    successStatuses: ['DISBURSED', 'SANCTION', 'SANCTION-ACCEPTED'],
    extractReferenceId: (requestPayload) => {
      if (!requestPayload) return null;
      let parsed = requestPayload;
      if (typeof requestPayload === 'string') {
        try { parsed = JSON.parse(requestPayload); } catch { return null; }
      }
      if (parsed.ref_id && typeof parsed.ref_id === 'object' && parsed.ref_id.S) return parsed.ref_id.S;
      if (typeof parsed.ref_id === 'string') return parsed.ref_id;
      if (parsed.reference_id) {
        if (typeof parsed.reference_id === 'object' && parsed.reference_id.S) return parsed.reference_id.S;
        if (typeof parsed.reference_id === 'string') return parsed.reference_id;
      }
      if (parsed.referenceId) {
        if (typeof parsed.referenceId === 'object' && parsed.referenceId.S) return parsed.referenceId.S;
        if (typeof parsed.referenceId === 'string') return parsed.referenceId;
      }
      return null;
    },
    filterUpdateData: (rowData) => {
      const filtered = {};
      ['lpLeadId','lpLeadDate','lpStatus','lpIncomeType','lpRejectReason',
       'api1HitDate','api1Response','api1Reason','api2HitDate','api2Response',
       'api2Reason','sanctionedAmount','sanctionedDate','disbursedAmount',
       'disbursedDate','afMediaSource','afPartner'].forEach(f => {
        if (rowData[f]) filtered[f] = rowData[f];
      });
      return filtered;
    },
    updateMethod: 'updateFromCSV',
  },

  // ── New MIS-only lenders (match via leads table) ──────────────────────────

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
    sheetName: 'Raw_Data',         // use Raw_Data sheet (ignore Daywise_Summary, Funnel)
    idType: 'none',
    // ⚠️ FatakPay DCL MIS has no phone column — only their internal lead_id / lapp_id.
    // To enable auto-matching: store Fatakpay's lapp_id in pushedTo.FATAKPAY.lappId during API push.
    successStatuses: ['Disbursement', 'Disbursed'],
    extractId:      (row) => null,
    extractStatus:  (row) => pick(row, 'stage_name', 'Stage Name') || 'Unknown',
    extractDetails: (row) => ({
      lappId:    pick(row, 'lapp_id'),
      leadId:    pick(row, 'lead_id'),
      remarks:   pick(row, 'remarks'),
      payable:   pick(row, 'payable'),
      leadMonth: pick(row, 'lead_month'),
    }),
  },

  fatakpay_pl: {
    displayName: 'FatakPay PL',
    lenderKey: 'FATAKPAYPL',
    allowedExtensions: ['.xlsx', '.xls'],
    sheetName: 'Affiliate Data',   // use Affiliate Data sheet (ignore Pivot Summary)
    idType: 'none',
    // ⚠️ Same as FatakPay DCL — no phone in MIS file.
    successStatuses: ['Disbursement', 'Disbursed'],
    extractId:      (row) => null,
    extractStatus:  (row) => pick(row, 'latest_emi_stage_name') || 'Unknown',
    extractDetails: (row) => ({
      lappId:       pick(row, 'lapp_id'),
      leadId:       pick(row, 'lead_id'),
      fullName:     pick(row, 'full_name'),
      disbursedDate: pick(row, 'disb_dt'),
      loanProposed: pick(row, 'loan_amount_proposed'),
      loanProvided: pick(row, 'loan_amount_provided'),
      rejectReason: pick(row, 'final_reject_reason'),
      city:         pick(row, 'Final_City'),
      state:        pick(row, 'Final_State'),
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

function extractColumnValue(row, possibleNames) {
  for (const name of possibleNames) {
    const value = row[name] || row[name.toLowerCase()] || row[name.toUpperCase()];
    if (value !== undefined && value !== null && value !== '') return String(value).trim();
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

// ─── Legacy response-log sync (Ovly / LendingPlate) ──────────────────────────

async function findMatchingLogsOvly(fileMap, config) {
  const matches = [];
  let lastKey = null;
  do {
    const { items, lastEvaluatedKey } = await config.model.findAll({ limit: 500, lastEvaluatedKey: lastKey });
    items.forEach(item => {
      const bodyLeadId = config.extractLeadId(item.responseBody);
      if (bodyLeadId && fileMap.has(bodyLeadId)) {
        const updateData = config.filterUpdateData(fileMap.get(bodyLeadId));
        matches.push({ logId: item.logId, matchedLeadId: bodyLeadId, updateData });
      }
    });
    lastKey = lastEvaluatedKey;
  } while (lastKey);
  return matches;
}

async function findMatchingLogsLP(fileMap, config) {
  const matches = [];
  const sources = ['CashKuber', 'FREO', 'BatterySmart', 'Ratecut', 'VFC', 'Apr'];
  for (const source of sources) {
    let lastKey = null;
    do {
      const params = {
        TableName: config.model.TABLE_NAME || 'lending_plate_response_logs',
        IndexName: 'source-createdAt-index',
        KeyConditionExpression: '#source = :source',
        ExpressionAttributeNames: { '#source': 'source' },
        ExpressionAttributeValues: { ':source': source },
        Limit: 500,
      };
      if (lastKey) params.ExclusiveStartKey = lastKey;
      const result = await config.model.docClient.send(
        new (require('@aws-sdk/lib-dynamodb').QueryCommand)(params)
      );
      (result.Items || []).forEach(item => {
        const refId = config.extractReferenceId(item.requestPayload);
        if (refId && fileMap.has(refId)) {
          const updateData = config.filterUpdateData(fileMap.get(refId));
          matches.push({ logId: item.logId, matchedRefId: refId, source, updateData });
        }
      });
      lastKey = result.LastEvaluatedKey;
    } while (lastKey);
  }
  return matches;
}

async function batchUpdateResponseLogs(matches, config) {
  const BATCH = 25;
  const errors = [];
  const method = config.updateMethod || 'updateCurrentStatus';
  for (let i = 0; i < matches.length; i += BATCH) {
    const chunk = matches.slice(i, i + BATCH);
    const results = await Promise.allSettled(
      chunk.map(({ logId, updateData }) => config.model[method](logId, updateData))
    );
    results.forEach((r, idx) => {
      if (r.status === 'rejected') errors.push({ logId: chunk[idx].logId, reason: r.reason?.message });
    });
  }
  return errors;
}

// ─── New MIS → leads table sync ───────────────────────────────────────────────

async function syncMISToLeads(rows, config) {
  const result = { total: rows.length, matched: 0, updated: 0, successful: 0, unmatched: 0, skipped: 0, errors: [] };

  if (config.idType === 'none') {
    result.skipped = rows.length;
    result.note = `Auto-matching not available for ${config.displayName}. MIS file has no phone or leadId column. Store the lender's internal ID during the API push to enable future matching.`;
    return result;
  }

  for (const row of rows) {
    try {
      const identifier = config.extractId(row);
      if (!identifier) { result.unmatched++; continue; }

      const lead = config.idType === 'phone'
        ? await Lead.findByPhone(identifier)
        : await Lead.findById(identifier);

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

      // Mirror success into lead_success table
      if (isSuccess) {
        try {
          await LeadSuccess.upsertByLeadId(lead.leadId, {
            source:    lead.source,
            phone:     lead.phone,
            email:     lead.email,
            panNumber: lead.panNumber,
            fullName:  lead.fullName,
            [config.lenderKey]: true,
          });
        } catch (lsErr) {
          console.error(`[${config.lenderKey}] LeadSuccess upsert error for ${lead.leadId}:`, lsErr.message);
        }
      }

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

    let syncResult;

    if (config.idType === 'responselog') {
      // Legacy: update the lender's own response log table
      const mapping = config.columnMapping;
      const parsed = rows.map(row => {
        const extracted = {};
        for (const [fieldKey, possibleNames] of Object.entries(mapping)) {
          const value = extractColumnValue(row, possibleNames);
          if (value) extracted[fieldKey] = value;
        }
        return extracted;
      });

      if (lender.toLowerCase() === 'ovly') {
        const fileMap = new Map();
        parsed.filter(r => r.leadId && r.rejectionReason).forEach(r => fileMap.set(r.leadId, r));
        const matches = await findMatchingLogsOvly(fileMap, config);
        const errors  = await batchUpdateResponseLogs(matches, config);
        const unlockedCount = parsed.filter(r => r.rejectionReason === 'Unlocked').length;
        syncResult = {
          lender: config.displayName,
          totalInFile: fileMap.size,
          matched: matches.length,
          updated: matches.length - errors.length,
          unmatched: fileMap.size - matches.length,
          unlockedCount,
          errors,
        };
      } else {
        const fileMap = new Map();
        parsed.filter(r => r.referenceId).forEach(r => fileMap.set(r.referenceId, r));
        const matches = await findMatchingLogsLP(fileMap, config);
        const errors  = await batchUpdateResponseLogs(matches, config);
        const disbursedCount  = parsed.filter(r => r.lpStatus === 'DISBURSED').length;
        const sanctionedCount = parsed.filter(r => ['SANCTION', 'SANCTION-ACCEPTED'].includes(r.lpStatus)).length;
        syncResult = {
          lender: config.displayName,
          totalInFile: fileMap.size,
          matched: matches.length,
          updated: matches.length - errors.length,
          unmatched: fileMap.size - matches.length,
          disbursedCount,
          sanctionedCount,
          errors,
        };
      }
    } else {
      // New: update leads table directly by phone or leadId
      syncResult = await syncMISToLeads(rows, config);
      syncResult.lender = config.displayName;
    }

    res.json({ success: true, ...syncResult });

  } catch (error) {
    console.error('[uploadAndSync] Error:', error);
    if (filePath && fs.existsSync(filePath)) fs.unlinkSync(filePath);
    res.status(500).json({ success: false, error: error.message || 'Internal server error' });
  }
};

exports.getStats = async (req, res) => {
  try {
    const { lender } = req.params;
    const config = LENDER_CONFIGS[lender.toLowerCase()];
    if (!config) return res.status(400).json({ success: false, error: `Unknown lender: ${lender}` });
    if (!config.model) {
      return res.status(400).json({
        success: false,
        error: `Stats not available for ${config.displayName} (MIS-only lender — no response log table)`,
      });
    }
    const stats = await config.model.getStats();
    res.json({ success: true, lender: config.displayName, stats });
  } catch (error) {
    res.status(500).json({ success: false, error: error.message });
  }
};

exports.LENDER_CONFIGS = LENDER_CONFIGS;

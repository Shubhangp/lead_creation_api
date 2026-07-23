'use strict';

const fs   = require('fs');
const path = require('path');
const csv  = require('csv-parse/sync');
const XLSX = require('xlsx');

const Lead         = require('../models/leadModel');
const Disbursement = require('../models/disbursementModel');
const { docClient } = require('../dynamodb');
const { QueryCommand } = require('@aws-sdk/lib-dynamodb');
const { resolveSource } = require('../config/sourceAliases');

const RESPONSELOG_SOURCES = require('../config/registry').RESPONSELOG_SOURCES;

// ─── Lender Configs ───────────────────────────────────────────────────────────
//
// idType values:
//   'responselog' → match MIS row's lender-internal-id against our response log tables
//   'phone'       → match by normalised phone against leads table
//   'leadId'      → match by our UUID against leads table
//   'utm'         → no leads lookup; resolve source from UTM → write disbursement record only
//   'none'        → no viable match key in MIS; skip
//
// All lenders now only process DISBURSED rows (filtered by successStatuses).
// ─────────────────────────────────────────────────────────────────────────────

const LENDER_CONFIGS = {

  // ── Response-log matching ──────────────────────────────────────────────────

  ovly: {
    displayName: 'Ovly (SmartCoin)',
    lenderKey: 'OVLY',
    allowedExtensions: ['.csv', '.xlsx', '.xls'],
    sheetName: null,
    idType: 'responselog',
    tableName: 'ovly_response_logs',
    successFilterExpr: {
      FilterExpression: '#rs = :rs',
      ExpressionAttributeNames: { '#rs': 'responseStatus' },
      ExpressionAttributeValues: { ':rs': 'success' },
    },
    extractLenderIdFromLog: (log) => {
      const body = tryParseJSON(log.responseBody);
      return body?.lead_id || body?.leadId?.S || body?.leadId || null;
    },
    extractId:      (row) => pick(row, 'lead_id', 'leadId', 'LeadId', 'LEAD_ID'),
    extractStatus:  (row) => pick(row, 'rejection_reason', 'status', 'Status') || 'Unknown',
    successStatuses: ['Unlocked', 'disbursed', 'Disbursed'],
    extractDisbursalAmount: (row) => pick(row, 'loan_amount', 'unlock_amount'),
    extractDisbursalDate:   (row) => pick(row, 'loan_disbursed_date', 'approved_date'),
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

  fatakpay_dcl: {
    displayName: 'FatakPay DCL',
    lenderKey: 'FATAKPAY',
    allowedExtensions: ['.xlsx', '.xls'],
    sheetName: 'Raw_Data',
    idType: 'responselog',
    tableName: 'fatakpay_response_logs',
    successFilterExpr: {
      FilterExpression: 'contains(#rb, :msg)',
      ExpressionAttributeNames: { '#rb': 'responseBody' },
      ExpressionAttributeValues: { ':msg': 'You are eligible.' },
    },
    extractLenderIdFromLog: (log) => {
      const body = tryParseJSON(log.responseBody);
      return body?.lapp_id || body?.data?.lapp_id || null;
    },
    extractId:      (row) => pick(row, 'lapp_id'),
    extractStatus:  (row) => pick(row, 'stage_name', 'Stage Name') || 'Unknown',
    successStatuses: ['Disbursement', 'Disbursed'],
    extractDisbursalAmount: (row) => pick(row, 'payable', 'loan_amount'),
    extractDisbursalDate:   (row) => pick(row, 'disb_dt', 'disbursed_date', 'lead_month'),
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
    successFilterExpr: {
      FilterExpression: 'contains(#rb, :msg)',
      ExpressionAttributeNames: { '#rb': 'responseBody' },
      ExpressionAttributeValues: { ':msg': 'You are eligible.' },
    },
    extractLenderIdFromLog: (log) => {
      const body = tryParseJSON(log.responseBody);
      return body?.lapp_id || body?.data?.lapp_id || null;
    },
    extractId:      (row) => pick(row, 'lapp_id'),
    extractStatus:  (row) => pick(row, 'latest_emi_stage_name') || 'Unknown',
    successStatuses: ['Disbursement', 'Disbursed'],
    extractDisbursalAmount: (row) => pick(row, 'loan_amount_provided', 'loan_amount_proposed'),
    extractDisbursalDate:   (row) => pick(row, 'disb_dt', 'disbursed_date'),
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

  // ── Phone-based matching ───────────────────────────────────────────────────

  lendingplate: {
    displayName: 'Lending Plate',
    lenderKey: 'LendingPlate',
    allowedExtensions: ['.csv', '.xlsx', '.xls'],
    sheetName: null,
    idType: 'phone',
    successStatuses: ['DISBURSED', 'SANCTION', 'SANCTION-ACCEPTED'],
    extractId: (row) => normalizePhone(String(pick(row, 'Phone', 'phone', 'mobile', 'Customer Phone', 'Reference ID', 'reference_id') || '')),
    extractStatus:  (row) => pick(row, 'LP Status', 'lp_status', 'lpStatus') || 'Unknown',
    extractDisbursalAmount: (row) => pick(row, 'Disbursed Amount', 'disbursed_amount'),
    extractDisbursalDate:   (row) => pick(row, 'Disbursed Date', 'disbursed_date'),
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

  zype: {
    displayName: 'Zype',
    lenderKey: 'ZYPE',
    allowedExtensions: ['.csv', '.xlsx', '.xls'],
    sheetName: null,
    idType: 'phone',
    successStatuses: ['Disbursed'],
    extractId: (row) => normalizePhone(String(pick(row, 'mobile_number', 'phone', 'Mobile') || '')),
    extractStatus: (row) =>
      pick(row, 'disbursedon_date')
        ? 'Disbursed'
        : (pick(row, 'approval_status', 'l2_status', 'l1_status') || 'Unknown'),
    extractDisbursalAmount: (row) => pick(row, 'principal_amount'),
    extractDisbursalDate:   (row) => pick(row, 'disbursedon_date'),
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

  // tap4credit → phone per ODS (was leadId)
  tap4credit: {
    displayName: 'Tap4Credit',
    lenderKey: 'TAP4CREDIT',
    allowedExtensions: ['.xlsx', '.xls'],
    sheetName: 'Sheet1',
    idType: 'phone',
    successStatuses: ['Approved', 'Disbursed'],
    extractId: (row) => normalizePhone(String(pick(row, 'phoneNumber', 'phone', 'Phone', 'Mobile') || '')),
    extractStatus:  (row) => pick(row, 'status', 'Status') || 'Unknown',
    extractDisbursalAmount: (row) => pick(row, 'disbursalAmount', 'disbursal_amount'),
    extractDisbursalDate:   (row) => pick(row, 'disbursalDate', 'disbursal_date'),
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
    successStatuses: ['Disbursed', 'Approved', 'Sanctioned', 'Closed'],
    extractId: (row) => normalizePhone(String(pick(row, 'PhoneNumber', 'phone', 'Mobile') || '')),
    extractStatus:  (row) => pick(row, 'user_status', 'Status', 'status') || 'Unknown',
    extractDisbursalAmount: (row) => pick(row, 'DisbursalAmount', 'disbursal_amount'),
    extractDisbursalDate:   (row) => pick(row, 'DisbursedAt', 'disbursed_at'),
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
    lenderKey: 'CreditSea',
    allowedExtensions: ['.xlsx', '.xls'],
    sheetName: 'MIS Reports',
    idType: 'phone',
    successStatuses: ['Disbursed', 'Approved'],
    extractId: (row) => normalizePhone(String(pick(row, 'phoneNumber', 'phone', 'Phone') || '')),
    extractStatus:  (row) => pick(row, 'loanStatus', 'loan_status', 'status') || 'Unknown',
    extractDisbursalAmount: (row) => pick(row, 'disbursedAmount', 'disbursed_amount'),
    extractDisbursalDate:   (row) => pick(row, 'disbursedAt', 'disbursed_at'),
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
    sheetName: 'DisbursalData',
    idType: 'phone',
    successStatuses: ['Disbursed', 'Approve', 'Proceed to Bank'],
    extractId: (row) => normalizePhone(String(pick(row, 'Mobile Number', 'mobile_number', 'mobile', 'Phone') || '')),
    extractStatus:  (row) => pick(row, 'Status', 'status') || 'Unknown',
    extractDisbursalAmount: (row) => pick(row, 'Loan Amount', 'loan_amount'),
    extractDisbursalDate:   (row) => pick(row, 'Disbursed Date', 'disbursed_date'),
    extractDetails: (row) => ({
      loanAmount:    pick(row, 'Loan Amount', 'loan_amount'),
      disbursedDate: pick(row, 'Disbursed Date', 'disbursed_date'),
      rejectDate:    pick(row, 'Reject Date', 'reject_date'),
      rejectReason:  pick(row, 'Reject Reason', 'reject_reason'),
      profession:    pick(row, 'Profession', 'profession'),
      salary:        pick(row, 'Salary', 'salary'),
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
    extractDisbursalAmount: (row) => pick(row, 'disbursalamount', 'disbursed_amount'),
    extractDisbursalDate:   (row) => pick(row, 'disbursementdate', 'disbursed_date'),
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

  ponawalla: {
    displayName: 'Poonawalla Fincorp',
    lenderKey: 'PONAWALLA',
    allowedExtensions: ['.xlsx', '.xls', '.csv'],
    sheetName: null,
    idType: 'phone',
    successStatuses: ['Disbursed', 'Approved', 'APPROVED', 'DISBURSED'],
    extractId: (row) => normalizePhone(String(pick(row, 'Mobile', 'mobile', 'phone', 'Phone', 'MobileNo', 'mobile_number') || '')),
    extractStatus:  (row) => pick(row, 'Status', 'status', 'LoanStatus', 'loan_status') || 'Unknown',
    extractDisbursalAmount: (row) => pick(row, 'DisbursedAmount', 'disbursed_amount', 'LoanAmount', 'loan_amount'),
    extractDisbursalDate:   (row) => pick(row, 'DisbursedDate', 'disbursed_date', 'DisburseDate'),
    extractDetails: (row) => ({
      loanAmount:      pick(row, 'LoanAmount', 'loan_amount'),
      disbursedAmount: pick(row, 'DisbursedAmount', 'disbursed_amount'),
      disbursedDate:   pick(row, 'DisbursedDate', 'disbursed_date'),
      rejectReason:    pick(row, 'RejectReason', 'reject_reason'),
    }),
  },

  // ── UTM-based attribution (no leads-table lookup) ─────────────────────────
  // These lenders' MIS files contain UTM params. Source is resolved via
  // sourceAliases.js from the UTM value. Only disbursements table is written.

  // cashvia → UTM per ODS (was leadId)
  cashvia: {
    displayName: 'Cashvia',
    lenderKey: 'CASHVIA',
    allowedExtensions: ['.xlsx', '.xls'],
    sheetName: 'Sheet1',
    idType: 'utm',
    successStatuses: ['Approved', 'Disbursed'],
    extractId:     (row) => null,  // no lead lookup for utm type
    extractStatus: (row) => pick(row, 'Status', 'status') || 'Unknown',
    extractDisbursalAmount: (row) => pick(row, 'Disbursal Amount', 'disbursal_amount', 'Approval Amount'),
    extractDisbursalDate:   (row) => pick(row, 'Updated At', 'updated_at', 'Disbursal Date'),
    extractUTM: (row) => ({
      utmCampaign: pick(row, 'UTM Campaign', 'utm_campaign', 'campaign', 'Campaign'),
      utmMedium:   pick(row, 'UTM Medium',   'utm_medium',   'medium',   'Medium'),
      utmSource:   pick(row, 'UTM Source',   'utm_source',   'source',   'Source'),
    }),
    extractName:  (row) => pick(row, 'Name', 'name', 'Customer Name'),
    extractPhone: (row) => normalizePhone(String(pick(row, 'Phone', 'phone', 'Mobile', 'mobile') || '')),
    extractDetails: (row) => ({
      approvalAmount:  pick(row, 'Approval Amount', 'approval_amount'),
      disbursedAmount: pick(row, 'Disbursal Amount', 'disbursal_amount'),
      cibilScore:      pick(row, 'CIBIL Score', 'cibil_score'),
      decision:        pick(row, 'Decision', 'decision'),
      remark:          pick(row, 'Remark', 'remark'),
      agentName:       pick(row, 'Agent Name', 'agent_name'),
    }),
  },

  // digicredit → UTM per ODS (was leadId)
  digicredit: {
    displayName: 'Digicredit',
    lenderKey: 'DIGICREDIT',
    allowedExtensions: ['.xlsx', '.xls'],
    sheetName: 'Sheet1',
    idType: 'utm',
    successStatuses: ['Approved', 'Disbursed'],
    extractId:     (row) => null,
    extractStatus: (row) => pick(row, 'status', 'Status') || 'Unknown',
    extractDisbursalAmount: (row) => pick(row, 'disbursal_amount', 'Disbursal Amount', 'approval_amount'),
    extractDisbursalDate:   (row) => pick(row, 'disbursal_date', 'Disbursal Date'),
    extractUTM: (row) => ({
      utmCampaign: pick(row, 'utm_campaign', 'UTM Campaign', 'campaign'),
      utmMedium:   pick(row, 'utm_medium',   'UTM Medium',   'medium'),
      utmSource:   pick(row, 'utm_source',   'UTM Source',   'source'),
    }),
    extractName:  (row) => pick(row, 'name', 'Name', 'customer_name'),
    extractPhone: (row) => normalizePhone(String(pick(row, 'phone', 'Phone', 'mobile', 'Mobile') || '')),
    extractDetails: (row) => ({
      approvalAmount:  pick(row, 'approval_amount', 'Approval Amount'),
      disbursedAmount: pick(row, 'disbursal_amount', 'Disbursal Amount'),
      disbursedDate:   pick(row, 'disbursal_date', 'Disbursal Date'),
      remark:          pick(row, 'latest_call_remark', 'Latest Call Remark'),
      agentName:       pick(row, 'agent_name', 'Agent Name'),
      score:           pick(row, 'score', 'Score'),
    }),
  },

  ramfincorp: {
    displayName: 'RamFinCorp',
    lenderKey: 'RAMFINCROP',
    allowedExtensions: ['.xlsx', '.xls', '.csv'],
    sheetName: null,
    idType: 'utm',
    successStatuses: ['Disbursed', 'Approved', 'DISBURSED', 'APPROVED'],
    extractId:     (row) => null,
    extractStatus: (row) => pick(row, 'Status', 'status', 'loan_status') || 'Unknown',
    extractDisbursalAmount: (row) => pick(row, 'Disbursed Amount', 'disbursal_amount', 'loan_amount'),
    extractDisbursalDate:   (row) => pick(row, 'Disbursed Date', 'disbursal_date', 'disbursement_date'),
    extractUTM: (row) => ({
      utmCampaign: pick(row, 'utm_campaign', 'UTM Campaign', 'campaign'),
      utmMedium:   pick(row, 'utm_medium',   'UTM Medium',   'medium'),
      utmSource:   pick(row, 'utm_source',   'UTM Source',   'source'),
    }),
    extractName:  (row) => pick(row, 'Name', 'name', 'customer_name'),
    extractPhone: (row) => normalizePhone(String(pick(row, 'Phone', 'phone', 'Mobile', 'mobile') || '')),
    extractDetails: (row) => ({
      loanAmount:    pick(row, 'loan_amount', 'Loan Amount'),
      disbursedDate: pick(row, 'disbursal_date', 'Disbursed Date'),
    }),
  },

  // truefund → UTM attribution (MIS has no phone column; leadId/customerId are
  // TrueFund-internal UUIDs). Their leadId is used as the deterministic row key
  // so re-uploads overwrite instead of duplicating.
  truefund: {
    displayName: 'TrueFund',
    lenderKey: 'TrueFund',
    allowedExtensions: ['.xlsx', '.xls', '.csv'],
    sheetName: null,
    idType: 'utm',
    successStatuses: ['Approved', 'Disbursed', 'Auto_Disbursal'],
    extractId:     (row) => null,
    extractStatus: (row) => pick(row, 'status', 'Status') || 'Unknown',
    extractDisbursalAmount: (row) => pick(row, 'disbursalAmount', 'disbursal_amount', 'approvalAmount'),
    extractDisbursalDate:   (row) => pick(row, 'disbursalDate', 'disbursal_date', 'updated_at'),
    extractUTM: (row) => ({
      utmCampaign: pick(row, 'ppc_campaign', 'utm_campaign', 'campaign'),
      utmMedium:   pick(row, 'medium', 'utm_medium'),
      utmSource:   pick(row, 'utm_source', 'ppc_source', 'source'),
    }),
    extractName:  (row) => pick(row, 'name', 'Name', 'customer_name'),
    extractPhone: (row) => normalizePhone(String(pick(row, 'phone', 'Phone', 'mobile', 'Mobile') || '')),
    extractRowKey: (row) => pick(row, 'leadId', 'lead_id', 'customerId'),
    extractDetails: (row) => ({
      truefundLeadId:  pick(row, 'leadId'),
      approvalAmount:  pick(row, 'approvalAmount'),
      breApproveAmount:pick(row, 'breApproveAmount'),
      disbursedAmount: pick(row, 'disbursalAmount'),
      disbursedDate:   pick(row, 'disbursalDate'),
      remarks:         pick(row, 'remarks'),
      agentName:       pick(row, 'AgentName'),
      salaryMode:      pick(row, 'SalaryMode'),
      city:            pick(row, 'city'),
      state:           pick(row, 'state'),
    }),
  },

  // ── No match key ──────────────────────────────────────────────────────────

  herofincorp: {
    displayName: 'Hero Fincorp',
    lenderKey: 'HEROFINCORP',
    allowedExtensions: ['.xlsx', '.xls'],
    sheetName: 'Sheet1',
    idType: 'none',
    // ⚠️ Hero Fincorp MIS has no phone column; customer_id is a hashed string.
    successStatuses: ['DISBURSED', 'Disbursed'],
    extractId:      (row) => null,
    extractStatus:  (row) => pick(row, 'Stage', 'stage') || 'Unknown',
    extractDisbursalAmount: (row) => pick(row, 'Disbursed_Amount', 'disbursed_amount'),
    extractDisbursalDate:   (row) => pick(row, 'Disbursement_Date', 'disbursement_date'),
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

// Parse "₹26,460.00" / "26,460" / 26460 → 26460 (number) or null
function parseAmount(val) {
  if (val === null || val === undefined || val === '') return null;
  const cleaned = String(val).replace(/[^0-9.\-]/g, '');
  if (!cleaned) return null;
  const num = parseFloat(cleaned);
  return isNaN(num) ? null : num;
}

// Statuses that count as an actual disbursal (vs merely approved/sanctioned).
// Explicit config.disbursedStatuses wins; otherwise auto-derive from
// successStatuses (anything containing "disburs" or "unlock").
function getDisbursedStatuses(config) {
  if (config.disbursedStatuses) return config.disbursedStatuses;
  const derived = (config.successStatuses || []).filter(s => /disburs|unlock/i.test(s));
  return derived.length ? derived : (config.successStatuses || []);
}

// Normalize a date value ("26/06/2026", "2026-06-26", Excel Date string…)
// to canonical "YYYY-MM-DD" so date-range filtering works on the dashboard.
function normalizeDate(val) {
  if (!val) return null;
  const s = String(val).trim();
  // dd/mm/yyyy or dd-mm-yyyy (Indian MIS convention)
  const m = s.match(/^(\d{1,2})[\/\-](\d{1,2})[\/\-](\d{4})/);
  if (m) return `${m[3]}-${m[2].padStart(2, '0')}-${m[1].padStart(2, '0')}`;
  const d = new Date(s);
  if (!isNaN(d.getTime())) return d.toISOString().split('T')[0];
  return s; // keep raw value if unparseable
}

function tryParseJSON(val) {
  if (!val) return null;
  if (typeof val === 'object') return val;
  try { return JSON.parse(val); } catch { return null; }
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

// ─── Response-log map (success-only) ─────────────────────────────────────────
//
// Queries each source via source-createdAt-index GSI with FilterExpression
// (server-side filter) + ProjectionExpression (minimal payload).
// Returns Map<lenderInternalId → ourLeadId>
//
async function buildResponseLogMap(config) {
  const map = new Map();

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

// ─── Main sync: only process DISBURSED rows ───────────────────────────────────
//
// Flow per row:
//   1. Check if status is in successStatuses → skip if not
//   2. Resolve lead:
//      - responselog: look up lenderInternalId in pre-built map → Lead.findById
//      - phone:       Lead.findByPhone
//      - leadId:      Lead.findById
//      - utm:         no lead lookup → write disbursement record with UTM source
//      - none:        skip (no match key)
//   3. For non-utm types: update leads table (lenderStatuses + successfulLenders)
//   4. Write disbursement record to disbursements table
//
const CHUNK_SIZE = 25; // rows processed concurrently per chunk

async function syncMISToLeads(rows, config, options = {}) {
  // Optional common disbursal date (YYYY-MM-DD) applied to ALL disbursed rows.
  // When set, it overrides per-row dates so the whole upload lands on one date
  // and the dashboard date-range filter picks it up.
  const commonDisbursalDate = options.commonDisbursalDate || null;
  // Global row offset (used by batched /upload-rows so unmatched-row ids stay
  // stable across batch boundaries on re-uploads)
  const rowOffset = options.rowOffset || 0;

  const result = {
    totalInFile:     rows.length,
    successInFile:   0,   // rows with any success status (Approved/Sanctioned/Disbursed…)
    disbursedInFile: 0,   // rows with an actual disbursed status
    disbursedAmountInFile: 0, // sum of disbursal amounts across disbursed rows
    matched:         0,
    updated:         0,
    disbursementsRecorded: 0,
    unmatched:       0,
    errors:          [],
    lender:          config.displayName,
    tableName:       'leads',
    disbursalDateApplied: commonDisbursalDate,
  };

  const disbursedStatuses = getDisbursedStatuses(config);

  // Build response-log map once upfront for responselog-type lenders
  let responseLogMap = null;
  if (config.idType === 'responselog') {
    console.log(`[${config.lenderKey}] Building response log map…`);
    responseLogMap = await buildResponseLogMap(config);
    console.log(`[${config.lenderKey}] Map built: ${responseLogMap.size} entries`);
  }

  // ── Per-row processor (called concurrently within a chunk) ──────────────
  async function processRow(row, rowIndex) {
    try {
      // ── Step 1: Only process disbursed rows ───────────────────────────────
      const status    = config.extractStatus(row);
      const isSuccess = config.successStatuses.some(
        s => s.toLowerCase() === (status || '').toLowerCase()
      );
      if (!isSuccess) return;    // skip non-success rows entirely
      result.successInFile++;

      // Is this row an actual disbursal (vs merely approved/sanctioned)?
      const isDisbursed = disbursedStatuses.some(
        s => s.toLowerCase() === (status || '').toLowerCase()
      );
      const rowAmount = parseAmount(config.extractDisbursalAmount ? config.extractDisbursalAmount(row) : null);
      // Common date (if provided) overrides the row's own date
      const rowDate = commonDisbursalDate
        || normalizeDate(config.extractDisbursalDate ? config.extractDisbursalDate(row) : null);
      if (isDisbursed) {
        result.disbursedInFile++;
        if (rowAmount) result.disbursedAmountInFile += rowAmount;
      }

      // ── Step 2a: UTM-type → write disbursement record only (no leads lookup) ─
      if (config.idType === 'utm') {
        if (isDisbursed) {
          const utm  = config.extractUTM ? config.extractUTM(row) : {};
          // Resolve source from UTM (try utmSource → utmCampaign → utmMedium)
          const rawUTM   = utm.utmSource || utm.utmCampaign || utm.utmMedium || null;
          const source   = rawUTM ? resolveSource(rawUTM) : config.displayName;
          const phone    = config.extractPhone ? config.extractPhone(row) : null;
          // Fallback row key for MIS files without a phone column (e.g. TrueFund)
          const rowKey   = phone || (config.extractRowKey ? config.extractRowKey(row) : null);

          await Disbursement.create({
            // Deterministic id → re-uploads and duplicate rows overwrite, not duplicate
            _id:             rowKey ? `${config.lenderKey}#${rowKey}` : undefined,
            source,
            lender:          config.displayName,
            lenderKey:       config.lenderKey,
            disbursalAmount: rowAmount,
            disbursalDate:   rowDate,
            name:            config.extractName ? config.extractName(row) : null,
            phone,
            utmCampaign:     utm.utmCampaign || null,
            utmMedium:       utm.utmMedium   || null,
            utmSource:       utm.utmSource   || null,
          });
          result.disbursementsRecorded++;
        }
        return;
      }

      // ── Step 2b: none-type → skip (no matching key available) ─────────────
      if (config.idType === 'none') {
        result.unmatched++;
        return;
      }

      // ── Step 2c: Resolve our lead ─────────────────────────────────────────
      const identifier = config.extractId(row);
      if (!identifier) {
        result.unmatched++;
        // Still record disbursed rows so dashboard totals match the MIS file
        if (isDisbursed) {
          await Disbursement.create({
            _id:             `${config.lenderKey}#unmatched#row${rowIndex}`,
            source:          'Unknown',
            lender:          config.displayName,
            lenderKey:       config.lenderKey,
            disbursalAmount: rowAmount,
            disbursalDate:   rowDate,
            unmatched:       true,
          });
          result.disbursementsRecorded++;
        }
        return;
      }

      let lead;
      if (config.idType === 'responselog') {
        const ourLeadId = responseLogMap.get(String(identifier));
        if (!ourLeadId) { result.unmatched++; return; }
        lead = await Lead.findById(ourLeadId);
      } else if (config.idType === 'phone') {
        lead = await Lead.findByPhone(identifier);
      } else {
        // leadId
        lead = await Lead.findById(identifier);
      }

      if (!lead) {
        result.unmatched++;
        // Still record disbursed rows so dashboard totals match the MIS file
        if (isDisbursed) {
          await Disbursement.create({
            _id:             `${config.lenderKey}#unmatched#${identifier}`,
            source:          'Unknown',
            lender:          config.displayName,
            lenderKey:       config.lenderKey,
            disbursalAmount: rowAmount,
            disbursalDate:   rowDate,
            phone:           config.idType === 'phone' ? identifier : null,
            unmatched:       true,
          });
          result.disbursementsRecorded++;
        }
        return;
      }
      result.matched++;

      // ── Step 3: Update leads table ────────────────────────────────────────
      const details    = config.extractDetails(row);
      const statusEntry = { status, ...details, isSuccess: true, updatedAt: new Date().toISOString() };
      Object.keys(statusEntry).forEach(k => {
        if (statusEntry[k] === null || statusEntry[k] === undefined) delete statusEntry[k];
      });

      const updates = { [`lenderStatuses.${config.lenderKey}`]: statusEntry };

      // Append to successfulLenders if not already there
      const existing = Array.isArray(lead.successfulLenders) ? lead.successfulLenders : [];
      if (!existing.includes(config.lenderKey)) {
        updates.successfulLenders = [...existing, config.lenderKey];
      }

      await Lead.updateByIdNoValidation(lead.leadId, updates);
      result.updated++;

      // ── Step 4: Write disbursement record (disbursed rows only) ───────────
      // Approved/Sanctioned rows update the lead above but must NOT count as
      // disbursals on the dashboard.
      if (isDisbursed) {
        await Disbursement.create({
          // Deterministic id → re-uploads and duplicate rows overwrite, not duplicate
          _id:             `${config.lenderKey}#${lead.leadId}`,
          leadId:          lead.leadId,
          source:          lead.source,
          lender:          config.displayName,
          lenderKey:       config.lenderKey,
          disbursalAmount: rowAmount,
          disbursalDate:   rowDate,
          name:            lead.fullName || null,
          phone:           lead.phone   || null,
        });
        result.disbursementsRecorded++;
      }

    } catch (err) {
      console.error(`[${config.lenderKey}] Row error:`, err.message);
      result.errors.push({
        identifier: (() => { try { return config.extractId(row); } catch { return '?'; } })(),
        error: err.message,
      });
    }
  }

  // ── Chunked execution: CHUNK_SIZE rows in parallel, chunks sequential ────
  // Keeps total time low on production (no request timeout on big files)
  // without hammering DynamoDB with unbounded concurrency.
  for (let i = 0; i < rows.length; i += CHUNK_SIZE) {
    const chunk = rows.slice(i, i + CHUNK_SIZE);
    await Promise.all(chunk.map((row, j) => processRow(row, rowOffset + i + j)));
    if (rows.length > CHUNK_SIZE) {
      console.log(`[${config.lenderKey}] Processed ${Math.min(i + CHUNK_SIZE, rows.length)}/${rows.length} rows`);
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
        : c.idType === 'utm'
        ? 'UTM attribution — records go to disbursements table (no lead lookup)'
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

    // Optional common disbursal date (applied to every disbursed row)
    let commonDisbursalDate = null;
    if (req.body.disbursalDate) {
      commonDisbursalDate = normalizeDate(req.body.disbursalDate);
      if (!/^\d{4}-\d{2}-\d{2}$/.test(commonDisbursalDate || '')) {
        return res.status(400).json({
          success: false,
          error: `Invalid disbursalDate "${req.body.disbursalDate}". Use YYYY-MM-DD.`,
        });
      }
    }

    const rows = parseFile(filePath, file.originalname, config);
    console.log(`[${lender}] Parsed ${rows.length} rows (sheet: ${config.sheetName || 'first'})${commonDisbursalDate ? `, common disbursal date: ${commonDisbursalDate}` : ''}`);

    if (fs.existsSync(filePath)) { fs.unlinkSync(filePath); filePath = null; }

    const syncResult = await syncMISToLeads(rows, config, { commonDisbursalDate });

    res.json({ success: true, ...syncResult });

  } catch (error) {
    console.error('[uploadAndSync] Error:', error);
    if (filePath && fs.existsSync(filePath)) fs.unlinkSync(filePath);
    res.status(500).json({ success: false, error: error.message || 'Internal server error' });
  }
};

// ─── Row-batch sync (client-side parsed) ─────────────────────────────────────
//
// For large MIS files the dashboard parses the file in the browser (xlsx pkg)
// and POSTs rows in small JSON batches, so no single request exceeds the
// proxy body-size limit and the server never handles the full file.
//
//   POST /upload-rows  (json: { lender, rows: [...], rowOffset?, disbursalDate? })
//
// Each batch runs through the same syncMISToLeads as /upload; the client
// aggregates the per-batch results.
// ──────────────────────────────────────────────────────────────────────────────

exports.syncRows = async (req, res) => {
  try {
    const { lender, rows, disbursalDate, rowOffset } = req.body || {};

    if (!lender) return res.status(400).json({ success: false, error: 'lender is required' });
    if (!Array.isArray(rows) || rows.length === 0) {
      return res.status(400).json({ success: false, error: 'rows must be a non-empty array' });
    }
    if (rows.length > 1000) {
      return res.status(400).json({ success: false, error: 'Max 1000 rows per batch' });
    }

    const config = LENDER_CONFIGS[lender.toLowerCase()];
    if (!config) {
      return res.status(400).json({
        success: false,
        error: `Unknown lender: ${lender}. Available: ${Object.keys(LENDER_CONFIGS).join(', ')}`,
      });
    }

    let commonDisbursalDate = null;
    if (disbursalDate) {
      commonDisbursalDate = normalizeDate(disbursalDate);
      if (!/^\d{4}-\d{2}-\d{2}$/.test(commonDisbursalDate || '')) {
        return res.status(400).json({
          success: false,
          error: `Invalid disbursalDate "${disbursalDate}". Use YYYY-MM-DD.`,
        });
      }
    }

    const offset = parseInt(rowOffset, 10) || 0;
    console.log(`[${lender}] Row-batch sync: ${rows.length} rows (offset ${offset})${commonDisbursalDate ? `, common disbursal date: ${commonDisbursalDate}` : ''}`);

    const syncResult = await syncMISToLeads(rows, config, { commonDisbursalDate, rowOffset: offset });

    res.json({ success: true, ...syncResult });

  } catch (error) {
    console.error('[syncRows] Error:', error);
    res.status(500).json({ success: false, error: error.message || 'Internal server error' });
  }
};

exports.LENDER_CONFIGS = LENDER_CONFIGS;
exports.syncMISToLeads = syncMISToLeads; // exported for testing

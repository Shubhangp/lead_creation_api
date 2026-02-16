// controllers/lenderSyncController.js
const fs = require('fs');
const path = require('path');
const csv = require('csv-parse/sync');
const XLSX = require('xlsx');

// Import your DynamoDB models
const OvlyResponseLog = require('../models/ovlyResponseLog');
const LendingPlateResponseLog = require('../models/leadingPlateResponseLog');

const LENDER_CONFIGS = {

  ovly: {
    tableName: 'ovly_response_logs',
    displayName: 'Ovly',
    model: OvlyResponseLog,
    allowedExtensions: ['.csv', '.xlsx', '.xls'],

    columnMapping: {
      leadId: ['lead_id', 'leadId', 'LeadId', 'LEAD_ID'],
      rejectionReason: ['rejection_reason', 'rejectionReason', 'RejectionReason', 'REJECTION_REASON'],

      // Conditional fields (only if rejection_reason = "Unlocked")
      unlockAmount: ['unlock_amount', 'unlockAmount', 'UnlockAmount'],
      appliedDate: ['applied_date', 'appliedDate', 'AppliedDate'],
      kycCompletedDate: ['kyc_completed_date', 'kycCompletedDate', 'KycCompletedDate'],
      approvedDate: ['approved_date', 'approvedDate', 'ApprovedDate'],
      emandateDoneAt: ['emandate_done_at', 'emandateDoneAt', 'EmandateDoneAt'],
      agreementSignedDate: ['agreement_signed_date', 'agreementSignedDate', 'AgreementSignedDate'],
      loanAmount: ['loan_amount', 'loanAmount', 'LoanAmount'],
      loanDisbursedDate: ['loan_disbursed_date', 'loanDisbursedDate', 'LoanDisbursedDate']
    },

    extractLeadId: (responseBody) => {
      if (!responseBody) return null;

      let parsed = responseBody;
      if (typeof responseBody === 'string') {
        try {
          parsed = JSON.parse(responseBody);
        } catch {
          return null;
        }
      }

      if (parsed.leadId && typeof parsed.leadId === 'object' && parsed.leadId.S) {
        return parsed.leadId.S;
      }

      // Plain format: { leadId: "value" }
      if (typeof parsed.leadId === 'string') {
        return parsed.leadId;
      }

      return null;
    },

    filterUpdateData: (rowData) => {
      const filtered = {
        lead_id: rowData.leadId,
        rejection_reason: rowData.rejectionReason
      };

      if (rowData.rejectionReason === 'Unlocked') {
        if (rowData.unlockAmount) filtered.unlock_amount = rowData.unlockAmount;
        if (rowData.appliedDate) filtered.applied_date = rowData.appliedDate;
        if (rowData.kycCompletedDate) filtered.kyc_completed_date = rowData.kycCompletedDate;
        if (rowData.approvedDate) filtered.approved_date = rowData.approvedDate;
        if (rowData.emandateDoneAt) filtered.emandate_done_at = rowData.emandateDoneAt;
        if (rowData.agreementSignedDate) filtered.agreement_signed_date = rowData.agreementSignedDate;
        if (rowData.loanAmount) filtered.loan_amount = rowData.loanAmount;
        if (rowData.loanDisbursedDate) filtered.loan_disbursed_date = rowData.loanDisbursedDate;
      }

      return filtered;
    },

    updateMethod: 'updateStatusWithData'
  },

  lendingplate: {
    tableName: 'lending_plate_response_logs',
    displayName: 'Lending Plate',
    model: LendingPlateResponseLog,
    allowedExtensions: ['.csv', '.xlsx', '.xls'],

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
      afPartner: ['AF Partner', 'af_partner', 'afPartner', 'AFPartner']
    },

    // Extract Reference ID from requestPayload
    extractReferenceId: (requestPayload) => {
      if (!requestPayload) return null;

      let parsed = requestPayload;
      if (typeof requestPayload === 'string') {
        try {
          parsed = JSON.parse(requestPayload);
        } catch {
          return null;
        }
      }

      // Handle DynamoDB format: { "ref_id": { "S": "value" } }
      if (parsed.ref_id && typeof parsed.ref_id === 'object' && parsed.ref_id.S) {
        return parsed.ref_id.S;
      }

      // Handle normal format: { "ref_id": "value" }
      if (typeof parsed.ref_id === 'string') {
        return parsed.ref_id;
      }

      // Also try 'reference_id' or 'referenceId'
      if (parsed.reference_id) {
        if (typeof parsed.reference_id === 'object' && parsed.reference_id.S) {
          return parsed.reference_id.S;
        }
        if (typeof parsed.reference_id === 'string') {
          return parsed.reference_id;
        }
      }

      if (parsed.referenceId) {
        if (typeof parsed.referenceId === 'object' && parsed.referenceId.S) {
          return parsed.referenceId.S;
        }
        if (typeof parsed.referenceId === 'string') {
          return parsed.referenceId;
        }
      }

      return null;
    },

    filterUpdateData: (rowData) => {
      const filtered = {};

      // Map all LP CSV fields
      if (rowData.lpLeadId) filtered.lpLeadId = rowData.lpLeadId;
      if (rowData.lpLeadDate) filtered.lpLeadDate = rowData.lpLeadDate;
      if (rowData.lpStatus) filtered.lpStatus = rowData.lpStatus;
      if (rowData.lpIncomeType) filtered.lpIncomeType = rowData.lpIncomeType;
      if (rowData.lpRejectReason) filtered.lpRejectReason = rowData.lpRejectReason;
      if (rowData.api1HitDate) filtered.api1HitDate = rowData.api1HitDate;
      if (rowData.api1Response) filtered.api1Response = rowData.api1Response;
      if (rowData.api1Reason) filtered.api1Reason = rowData.api1Reason;
      if (rowData.api2HitDate) filtered.api2HitDate = rowData.api2HitDate;
      if (rowData.api2Response) filtered.api2Response = rowData.api2Response;
      if (rowData.api2Reason) filtered.api2Reason = rowData.api2Reason;
      if (rowData.sanctionedAmount) filtered.sanctionedAmount = rowData.sanctionedAmount;
      if (rowData.sanctionedDate) filtered.sanctionedDate = rowData.sanctionedDate;
      if (rowData.disbursedAmount) filtered.disbursedAmount = rowData.disbursedAmount;
      if (rowData.disbursedDate) filtered.disbursedDate = rowData.disbursedDate;
      if (rowData.afMediaSource) filtered.afMediaSource = rowData.afMediaSource;
      if (rowData.afPartner) filtered.afPartner = rowData.afPartner;

      return filtered;
    },

    updateMethod: 'updateFromCSV'
  }

};

function extractColumnValue(row, possibleNames) {
  for (const name of possibleNames) {
    const value = row[name] || row[name.toLowerCase()] || row[name.toUpperCase()];
    if (value !== undefined && value !== null && value !== '') {
      return String(value).trim();
    }
  }
  return null;
}

function parseUploadedFile(filePath, originalFilename, config) {
  const ext = path.extname(originalFilename).toLowerCase();

  if (!config.allowedExtensions.includes(ext)) {
    throw new Error(`File type ${ext} not supported for ${config.displayName}. Allowed: ${config.allowedExtensions.join(', ')}`);
  }

  let rows = [];

  if (ext === '.csv') {
    const content = fs.readFileSync(filePath, 'utf-8');
    rows = csv.parse(content, {
      columns: true,
      skip_empty_lines: true,
      trim: true
    });
  }

  else if (ext === '.xlsx' || ext === '.xls') {
    const workbook = XLSX.readFile(filePath);
    const sheet = workbook.Sheets[workbook.SheetNames[0]];
    rows = XLSX.utils.sheet_to_json(sheet);
  }

  const mapping = config.columnMapping;

  const parsed = rows.map(row => {
    const extracted = {};

    for (const [fieldKey, possibleNames] of Object.entries(mapping)) {
      const value = extractColumnValue(row, possibleNames);
      if (value) {
        extracted[fieldKey] = value;
      }
    }

    return extracted;
  });

  const filtered = parsed.filter(item => item.leadId && item.rejectionReason);

  console.log(`[parseFile] Parsed ${rows.length} rows → ${filtered.length} valid entries`);

  return filtered;
}

function buildFileMap(rows) {
  const map = new Map();
  rows.forEach((rowData) => {
    if (rowData.leadId) {
      map.set(rowData.leadId, rowData);
    }
  });
  return map;
}

async function findMatchingLogs(fileMap, config) {
  const matches = [];
  let lastKey = null;

  console.log(`[findMatching] Scanning ${config.tableName}...`);

  do {
    const { items, lastEvaluatedKey } = await config.model.findAll({
      limit: 500,
      lastEvaluatedKey: lastKey
    });

    console.log(`[findMatching] Scanned ${items.length} items`);

    items.forEach(item => {
      const bodyLeadId = config.extractLeadId(item.responseBody);

      if (bodyLeadId && fileMap.has(bodyLeadId)) {
        const rowData = fileMap.get(bodyLeadId);

        // Apply filterUpdateData if it exists
        const updateData = config.filterUpdateData
          ? config.filterUpdateData(rowData)
          : rowData;

        matches.push({
          logId: item.logId,
          matchedLeadId: bodyLeadId,
          updateData: updateData
        });
      }
    });

    lastKey = lastEvaluatedKey;
  } while (lastKey);

  console.log(`[findMatching] Found ${matches.length} matches`);
  return matches;
}

async function findMatchingLogsLP(fileMap, config) {
  const matches = [];
  const sources = ['CashKuber', 'FREO', 'BatterySmart', 'Ratecut', 'VFC'];

  console.log(`[findMatchingLP] Scanning ${sources.length} sources...`);

  for (const source of sources) {
    console.log(`[findMatchingLP] Querying source: ${source}...`);

    let lastKey = null;
    let sourceTotal = 0;

    do {
      // Query by source
      const params = {
        TableName: config.model.TABLE_NAME || 'lending_plate_response_logs',
        IndexName: 'source-createdAt-index',
        KeyConditionExpression: '#source = :source',
        ExpressionAttributeNames: { '#source': 'source' },
        ExpressionAttributeValues: { ':source': source },
        Limit: 500
      };

      if (lastKey) {
        params.ExclusiveStartKey = lastKey;
      }

      const result = await config.model.docClient.send(
        new (require('@aws-sdk/lib-dynamodb').QueryCommand)(params)
      );

      const items = result.Items || [];
      sourceTotal += items.length;

      items.forEach(item => {
        const refId = config.extractReferenceId(item.requestPayload);

        if (refId && fileMap.has(refId)) {
          const rowData = fileMap.get(refId);

          const updateData = config.filterUpdateData
            ? config.filterUpdateData(rowData)
            : rowData;

          matches.push({
            logId: item.logId,
            matchedRefId: refId,
            source: source,
            updateData: updateData
          });
        }
      });

      lastKey = result.LastEvaluatedKey;
    } while (lastKey);

    console.log(`[findMatchingLP] Source ${source}: scanned ${sourceTotal} items, found ${matches.filter(m => m.source === source).length} matches`);
  }

  console.log(`[findMatchingLP] Total matches: ${matches.length}`);
  return matches;
}

async function batchUpdateStatus(matches, config) {
  const BATCH_SIZE = 25;
  const errors = [];
  const updateMethodName = config.updateMethod || 'updateCurrentStatus';

  console.log(`[batchUpdate] Updating ${matches.length} records in batches of ${BATCH_SIZE}...`);

  for (let i = 0; i < matches.length; i += BATCH_SIZE) {
    const chunk = matches.slice(i, i + BATCH_SIZE);

    const results = await Promise.allSettled(
      chunk.map(({ logId, updateData }) =>
        config.model[updateMethodName](logId, updateData)
      )
    );

    results.forEach((result, idx) => {
      if (result.status === 'rejected') {
        errors.push({
          logId: chunk[idx].logId,
          reason: result.reason?.message || 'Unknown error'
        });
        console.error(`[batchUpdate] Error updating ${chunk[idx].logId}:`, result.reason);
      }
    });

    console.log(`[batchUpdate] Completed batch ${Math.floor(i / BATCH_SIZE) + 1}/${Math.ceil(matches.length / BATCH_SIZE)}`);
  }

  console.log(`[batchUpdate] Done. Errors: ${errors.length}`);
  return errors;
}

async function syncStatusFromFile(filePath, originalFilename, lenderKey) {
  const config = LENDER_CONFIGS[lenderKey];

  if (!config) {
    throw new Error(`Unknown lender: ${lenderKey}. Available: ${Object.keys(LENDER_CONFIGS).join(', ')}`);
  }

  console.log(`\n[${lenderKey}] ========== Starting Sync ==========`);
  console.log(`[${lenderKey}] File: ${filePath}`);
  console.log(`[${lenderKey}] Original filename: ${originalFilename}`);
  console.log(`[${lenderKey}] Table: ${config.tableName}`);

  // Step 1: Parse file
  const rows = parseUploadedFile(filePath, originalFilename, config);
  const fileMap = buildFileMap(rows);
  console.log(`[${lenderKey}] Loaded ${fileMap.size} unique leadIds from file`);

  // Step 2: Find matching logs
  const matches = await findMatchingLogs(fileMap, config);
  console.log(`[${lenderKey}] Found ${matches.length} matching logs in DynamoDB`);

  // Step 3: Update status/data
  const errors = await batchUpdateStatus(matches, config);

  // Step 4: Count how many were "Unlocked" vs other reasons
  let specialStats = {};

  if (config.displayName === 'Ovly') {
    const unlockedCount = Array.from(fileMap.values()).filter(
      row => row.rejectionReason === 'Unlocked'
    ).length;
    specialStats.unlockedCount = unlockedCount;
  } else if (config.displayName === 'Lending Plate') {
    const disbursedCount = Array.from(fileMap.values()).filter(
      row => row.lpStatus === 'DISBURSED'
    ).length;
    const sanctionedCount = Array.from(fileMap.values()).filter(
      row => row.lpStatus === 'SANCTION' || row.lpStatus === 'SANCTION-ACCEPTED'
    ).length;
    specialStats.disbursedCount = disbursedCount;
    specialStats.sanctionedCount = sanctionedCount;
  }

  // Summary
  const summary = {
    lender: config.displayName,
    tableName: config.tableName,
    totalInFile: fileMap.size,
    matched: matches.length,
    updated: matches.length - errors.length,
    unmatched: fileMap.size - matches.length,
    unlockedCount: unlockedCount,
    errors: errors
  };

  console.log(`[${lenderKey}] ========== Sync Complete ==========`);
  console.log(`[${lenderKey}] Matched: ${summary.matched}, Updated: ${summary.updated}, Unmatched: ${summary.unmatched}`);
  if (specialStats.unlockedCount !== undefined) {
    console.log(`[${lenderKey}] Unlocked records: ${specialStats.unlockedCount}`);
  }
  if (specialStats.disbursedCount !== undefined) {
    console.log(`[${lenderKey}] Disbursed: ${specialStats.disbursedCount}, Sanctioned: ${specialStats.sanctionedCount}`);
  }

  return summary;
}

exports.getLenders = (req, res) => {
  try {
    const lenders = Object.keys(LENDER_CONFIGS).map(key => ({
      key: key,
      displayName: LENDER_CONFIGS[key].displayName,
      tableName: LENDER_CONFIGS[key].tableName,
      allowedExtensions: LENDER_CONFIGS[key].allowedExtensions
    }));

    res.json({
      success: true,
      lenders
    });
  } catch (error) {
    console.error('[getLenders] Error:', error);
    res.status(500).json({
      success: false,
      error: error.message
    });
  }
};

/**
 * POST /api/lender-sync/upload
 * Handles file upload and sync
 */
exports.uploadAndSync = async (req, res) => {
  let filePath = null;

  try {
    const { lender } = req.body;
    const file = req.file;

    // ── Validation ──────────────────────────────────────────────────────────
    if (!file) {
      return res.status(400).json({
        success: false,
        error: 'No file provided'
      });
    }

    filePath = file.path;

    if (!lender) {
      return res.status(400).json({
        success: false,
        error: 'Lender parameter is required'
      });
    }

    const config = LENDER_CONFIGS[lender];
    if (!config) {
      return res.status(400).json({
        success: false,
        error: `Invalid lender: ${lender}. Available: ${Object.keys(LENDER_CONFIGS).join(', ')}`
      });
    }

    const ext = path.extname(file.originalname).toLowerCase();
    if (!config.allowedExtensions.includes(ext)) {
      return res.status(400).json({
        success: false,
        error: `File type ${ext} not supported for ${config.displayName}. Allowed: ${config.allowedExtensions.join(', ')}`
      });
    }

    // ── Run Sync ────────────────────────────────────────────────────────────
    const result = await syncStatusFromFile(filePath, file.originalname, lender);

    // ── Cleanup ─────────────────────────────────────────────────────────────
    if (fs.existsSync(filePath)) {
      fs.unlinkSync(filePath);
    }

    res.json({
      success: true,
      ...result
    });

  } catch (error) {
    console.error('[uploadAndSync] Error:', error);

    // Cleanup on error
    if (filePath && fs.existsSync(filePath)) {
      fs.unlinkSync(filePath);
    }

    res.status(500).json({
      success: false,
      error: error.message || 'Internal server error'
    });
  }
};

/**
 * GET /api/lender-sync/stats/:lender
 * Get statistics for a specific lender (optional endpoint)
 */
exports.getStats = async (req, res) => {
  try {
    const { lender } = req.params;
    const config = LENDER_CONFIGS[lender];

    if (!config) {
      return res.status(400).json({
        success: false,
        error: `Invalid lender: ${lender}`
      });
    }

    // If your model has a getStats method, use it:
    const stats = await config.model.getStats();

    res.json({
      success: true,
      lender: config.displayName,
      tableName: config.tableName,
      stats
    });

  } catch (error) {
    console.error('[getStats] Error:', error);
    res.status(500).json({
      success: false,
      error: error.message
    });
  }
};

// Export LENDER_CONFIGS for use in other files if needed
exports.LENDER_CONFIGS = LENDER_CONFIGS;
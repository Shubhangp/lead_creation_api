// controllers/lenderSyncController.js
const fs   = require('fs');
const path = require('path');
const csv  = require('csv-parse/sync');
const XLSX = require('xlsx');

// Import your DynamoDB models
const OvlyResponseLog = require('../models/ovlyResponseLog');
// Add other lender models here:
// const LenderAResponseLog = require('../models/LenderAResponseLog');
// const LenderBResponseLog = require('../models/LenderBResponseLog');

// ═══════════════════════════════════════════════════════════════════════════
// LENDER CONFIGURATIONS
// ═══════════════════════════════════════════════════════════════════════════
// 
// ⭐ TO ADD A NEW LENDER: Just add a new object to this LENDER_CONFIGS object
//
// Each lender config defines:
//   - tableName: DynamoDB table name
//   - displayName: Human-readable name
//   - model: Reference to the DynamoDB model class
//   - allowedExtensions: Which file types are accepted
//   - columnMapping: How to find leadId and status in uploaded file
//   - extractLeadId: Function to parse responseBody and extract leadId
//   - updateMethod: Name of the update method in the model (default: 'updateCurrentStatus')

const LENDER_CONFIGS = {
  
  // ── OVLY ──────────────────────────────────────────────────────────────────
  ovly: {
    tableName:          'ovly_response_logs',
    displayName:        'Ovly',
    model:              OvlyResponseLog,
    allowedExtensions:  ['.csv', '.xlsx', '.xls'],
    
    // Column mapping: tries these column names in order (case-insensitive)
    columnMapping: {
      leadId: ['leadId', 'lead_id', 'LeadId', 'LEAD_ID'],
      status: ['currentStatus', 'status', 'Status', 'current_status', 'CURRENT_STATUS']
    },
    
    // How to extract leadId from responseBody JSON
    // responseBody can be either:
    //   1. DynamoDB typed format: { leadId: { S: "value" } }
    //   2. Plain JSON: { leadId: "value" }
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
      
      // DynamoDB typed format: { leadId: { S: "value" } }
      if (parsed.leadId && typeof parsed.leadId === 'object' && parsed.leadId.S) {
        return parsed.leadId.S;
      }
      
      // Plain format: { leadId: "value" }
      if (typeof parsed.leadId === 'string') {
        return parsed.leadId;
      }
      
      return null;
    },
    
    updateMethod: 'updateCurrentStatus'  // method name in OvlyResponseLog model
  },

  // ── EXAMPLE: LENDER A ─────────────────────────────────────────────────────
  // Uncomment and customize when adding a new lender:
  //
  // lenderA: {
  //   tableName:          'lender_a_logs',
  //   displayName:        'Lender A',
  //   model:              LenderAResponseLog,
  //   allowedExtensions:  ['.csv'],
  //   
  //   columnMapping: {
  //     leadId: ['loan_id', 'loanId', 'LoanID'],
  //     status: ['status', 'loan_status']
  //   },
  //   
  //   extractLeadId: (responseBody) => {
  //     if (!responseBody) return null;
  //     let parsed = typeof responseBody === 'string' 
  //       ? JSON.parse(responseBody) 
  //       : responseBody;
  //     return parsed.loan_id || parsed.loanId || null;
  //   },
  //   
  //   updateMethod: 'updateStatus'
  // },

  // ── EXAMPLE: LENDER B ─────────────────────────────────────────────────────
  // lenderB: {
  //   tableName:          'lender_b_logs',
  //   displayName:        'Lender B',
  //   model:              LenderBResponseLog,
  //   allowedExtensions:  ['.xlsx', '.xls'],
  //   
  //   columnMapping: {
  //     leadId: ['application_id', 'appId'],
  //     status: ['current_status', 'app_status']
  //   },
  //   
  //   extractLeadId: (responseBody) => {
  //     if (!responseBody) return null;
  //     let parsed = typeof responseBody === 'string' 
  //       ? JSON.parse(responseBody) 
  //       : responseBody;
  //     return parsed.application?.id || parsed.appId || null;
  //   },
  //   
  //   updateMethod: 'updateCurrentStatus'
  // }
};

// ═══════════════════════════════════════════════════════════════════════════
// FILE PARSING UTILITIES
// ═══════════════════════════════════════════════════════════════════════════

/**
 * Parse uploaded CSV or Excel file
 * Returns array of { leadId, status } objects
 */
function parseUploadedFile(filePath, config) {
  const ext = path.extname(filePath).toLowerCase();
  
  if (!config.allowedExtensions.includes(ext)) {
    throw new Error(`File type ${ext} not supported for ${config.displayName}. Allowed: ${config.allowedExtensions.join(', ')}`);
  }

  let rows = [];

  // ── Parse CSV ─────────────────────────────────────────────────────────────
  if (ext === '.csv') {
    const content = fs.readFileSync(filePath, 'utf-8');
    rows = csv.parse(content, { 
      columns: true, 
      skip_empty_lines: true,
      trim: true
    });
  }
  
  // ── Parse Excel ───────────────────────────────────────────────────────────
  else if (ext === '.xlsx' || ext === '.xls') {
    const workbook = XLSX.readFile(filePath);
    const sheet    = workbook.Sheets[workbook.SheetNames[0]];  // first sheet
    rows           = XLSX.utils.sheet_to_json(sheet);
  }

  // ── Extract leadId and status using column mapping ────────────────────────
  const { leadId: leadIdCols, status: statusCols } = config.columnMapping;
  
  const parsed = rows.map(row => {
    // Try each possible column name for leadId (case-insensitive)
    let leadId = null;
    for (const col of leadIdCols) {
      const value = row[col] || row[col.toLowerCase()] || row[col.toUpperCase()];
      if (value) {
        leadId = String(value).trim();
        break;
      }
    }
    
    // Try each possible column name for status (case-insensitive)
    let status = null;
    for (const col of statusCols) {
      const value = row[col] || row[col.toLowerCase()] || row[col.toUpperCase()];
      if (value) {
        status = String(value).trim();
        break;
      }
    }
    
    return { leadId, status };
  });

  // Filter out rows that don't have both leadId and status
  const filtered = parsed.filter(item => item.leadId && item.status);
  
  console.log(`[parseFile] Parsed ${rows.length} rows → ${filtered.length} valid entries`);
  
  return filtered;
}

/**
 * Build a Map of leadId -> status from parsed rows
 * Last occurrence wins if there are duplicates
 */
function buildFileMap(rows) {
  const map = new Map();
  rows.forEach(({ leadId, status }) => {
    if (leadId) {
      map.set(leadId, status);
    }
  });
  return map;
}

// ═══════════════════════════════════════════════════════════════════════════
// SYNC LOGIC
// ═══════════════════════════════════════════════════════════════════════════

/**
 * Scan DynamoDB table and find logs where responseBody.leadId matches file
 * Returns array of { logId, matchedLeadId, newStatus }
 */
async function findMatchingLogs(fileMap, config) {
  const matches = [];
  let lastKey   = null;

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
        matches.push({
          logId:         item.logId,
          matchedLeadId: bodyLeadId,
          newStatus:     fileMap.get(bodyLeadId)
        });
      }
    });

    lastKey = lastEvaluatedKey;
  } while (lastKey);

  console.log(`[findMatching] Found ${matches.length} matches`);
  return matches;
}

/**
 * Batch update currentStatus in DynamoDB
 * Updates in batches of 25 to avoid rate limits
 * Returns array of errors (if any)
 */
async function batchUpdateStatus(matches, config) {
  const BATCH_SIZE       = 25;
  const errors           = [];
  const updateMethodName = config.updateMethod || 'updateCurrentStatus';

  console.log(`[batchUpdate] Updating ${matches.length} records in batches of ${BATCH_SIZE}...`);

  for (let i = 0; i < matches.length; i += BATCH_SIZE) {
    const chunk = matches.slice(i, i + BATCH_SIZE);

    const results = await Promise.allSettled(
      chunk.map(({ logId, newStatus }) =>
        config.model[updateMethodName](logId, newStatus)
      )
    );

    results.forEach((result, idx) => {
      if (result.status === 'rejected') {
        errors.push({
          logId:  chunk[idx].logId,
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

/**
 * Main sync function
 * Orchestrates: parse file → find matches → update status
 */
async function syncStatusFromFile(filePath, lenderKey) {
  const config = LENDER_CONFIGS[lenderKey];
  
  if (!config) {
    throw new Error(`Unknown lender: ${lenderKey}. Available: ${Object.keys(LENDER_CONFIGS).join(', ')}`);
  }

  console.log(`\n[${lenderKey}] ========== Starting Sync ==========`);
  console.log(`[${lenderKey}] File: ${filePath}`);
  console.log(`[${lenderKey}] Table: ${config.tableName}`);

  // Step 1: Parse file
  const rows    = parseUploadedFile(filePath, config);
  const fileMap = buildFileMap(rows);
  console.log(`[${lenderKey}] Loaded ${fileMap.size} unique leadIds from file`);

  // Step 2: Find matching logs
  const matches = await findMatchingLogs(fileMap, config);
  console.log(`[${lenderKey}] Found ${matches.length} matching logs in DynamoDB`);

  // Step 3: Update status
  const errors = await batchUpdateStatus(matches, config);

  // Summary
  const summary = {
    lender:      config.displayName,
    tableName:   config.tableName,
    totalInFile: fileMap.size,
    matched:     matches.length,
    updated:     matches.length - errors.length,
    unmatched:   fileMap.size - matches.length,
    errors:      errors
  };

  console.log(`[${lenderKey}] ========== Sync Complete ==========`);
  console.log(`[${lenderKey}] Matched: ${summary.matched}, Updated: ${summary.updated}, Unmatched: ${summary.unmatched}`);
  
  return summary;
}

// ═══════════════════════════════════════════════════════════════════════════
// CONTROLLER METHODS
// ═══════════════════════════════════════════════════════════════════════════

/**
 * GET /api/lender-sync/lenders
 * Returns list of available lenders for frontend dropdown
 */
exports.getLenders = (req, res) => {
  try {
    const lenders = Object.keys(LENDER_CONFIGS).map(key => ({
      key:                key,
      displayName:        LENDER_CONFIGS[key].displayName,
      tableName:          LENDER_CONFIGS[key].tableName,
      allowedExtensions:  LENDER_CONFIGS[key].allowedExtensions
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
    const file       = req.file;

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
    const result = await syncStatusFromFile(filePath, lender);

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
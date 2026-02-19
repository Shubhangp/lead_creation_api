const express = require('express');
const { docClient } = require('../dynamodb');
const { QueryCommand } = require('@aws-sdk/lib-dynamodb');

const router = express.Router();

// ═══════════════════════════════════════════════════════════════════════════════
// TABLE CONFIGURATION
// Defines available GSIs and known sources per table
// ═══════════════════════════════════════════════════════════════════════════════

const TABLE_CONFIG = {
  // ── Response log tables with source-createdAt-index ────────────────────────
  'ovly_response_logs': {
    type: 'response_log',
    primaryGSI: 'source-createdAt-index',
    sortKey: 'createdAt',
    sources: ['CashKuber', 'FREO', 'BatterySmart', 'Ratecut', 'VFC'],
    fallbackGSI: 'leadId-index'
  },
  'mpokket_response_logs': {
    type: 'response_log',
    primaryGSI: 'source-createdAt-index',
    sortKey: 'createdAt',
    sources: ['CashKuber', 'FREO', 'BatterySmart', 'Ratecut', 'VFC'],
    fallbackGSI: 'leadId-index'
  },
  'indialends_response_logs': {
    type: 'response_log',
    primaryGSI: 'source-createdAt-index',
    sortKey: 'createdAt',
    sources: ['CashKuber', 'FREO', 'BatterySmart', 'Ratecut', 'VFC'],
    fallbackGSI: 'leadId-index'
  },
  'mmm_response_logs': {
    type: 'response_log',
    primaryGSI: 'source-createdAt-index',
    sortKey: 'createdAt',
    sources: ['CashKuber', 'FREO', 'BatterySmart', 'Ratecut', 'VFC'],
    fallbackGSI: 'leadId-index'
  },
  'sml_response_logs': {
    type: 'response_log',
    primaryGSI: 'source-createdAt-index',
    sortKey: 'createdAt',
    sources: ['CashKuber', 'FREO', 'BatterySmart', 'Ratecut', 'VFC'],
    fallbackGSI: 'leadId-index'
  },
  'freo_response_logs': {
    type: 'response_log',
    primaryGSI: 'source-createdAt-index',
    sortKey: 'createdAt',
    sources: ['CashKuber', 'FREO', 'BatterySmart', 'Ratecut', 'VFC'],
    fallbackGSI: 'leadId-index'
  },
  'crmPaisa_response_logs': {
    type: 'response_log',
    primaryGSI: 'source-createdAt-index',
    sortKey: 'createdAt',
    sources: ['CashKuber', 'FREO', 'BatterySmart', 'Ratecut', 'VFC'],
    fallbackGSI: 'leadId-index'
  },
  'fintifi_response_logs': {
    type: 'response_log',
    primaryGSI: 'source-createdAt-index',
    sortKey: 'createdAt',
    sources: ['CashKuber', 'FREO', 'BatterySmart', 'Ratecut', 'VFC'],
    fallbackGSI: 'leadId-index'
  },

  // ── Response log tables WITHOUT source-createdAt-index (use leadId-index) ──
  'zype_response_logs': {
    type: 'response_log',
    primaryGSI: 'leadId-index',
    sortKey: null,
    requiresLeadId: true
  },
  'ramfincrop_logs': {
    type: 'response_log',
    primaryGSI: 'leadId-index',
    sortKey: null,
    requiresLeadId: true
  },
  'lending_plate_response_logs': {
    type: 'response_log',
    primaryGSI: 'source-createdAt-index', // NOW ACTIVE
    sortKey: 'createdAt',
    sources: ['CashKuber', 'FREO', 'BatterySmart', 'Ratecut', 'VFC'],
    fallbackGSI: 'leadId-index'
  },
  'fatakpay_response_logs': {
    type: 'response_log',
    primaryGSI: 'leadId-index',
    sortKey: null,
    requiresLeadId: true
  },
  'fatakpay_pl__response_logs': {
    type: 'response_log',
    primaryGSI: 'leadId-index',
    sortKey: null,
    requiresLeadId: true
  },

  // ── Leads tables ───────────────────────────────────────────────────────────
  'leads': {
    type: 'leads',
    primaryGSI: 'source-createdAt-index',
    sortKey: 'createdAt',
    partitionKey: 'source',
    sources: process.env.LEAD_SOURCES?.split(',').map(s => s.trim()) || [],
    alternativeGSIs: ['phone-index', 'panNumber-index']
  },
  'excel_leads': {
    type: 'leads',
    primaryGSI: 'source-createdAt-index',
    sortKey: 'createdAt',
    partitionKey: 'source',
    sources: process.env.LEAD_SOURCES?.split(',').map(s => s.trim()) || [],
    alternativeGSIs: ['phone-index', 'panNumber-index']
  },

  // ── Lead success table ─────────────────────────────────────────────────────
  'lead_success': {
    type: 'lead_success',
    primaryGSI: 'source-createdAt-index',
    sortKey: 'createdAt',
    partitionKey: 'source',
    sources: process.env.LEAD_SUCCESS_SOURCES?.split(',').map(s => s.trim()) || [],
    alternativeGSIs: ['leadId-index', 'phone-index', 'panNumber-index']
  },

  // ── RCS Queue ──────────────────────────────────────────────────────────────
  'rcs_queue': {
    type: 'rcs_queue',
    primaryGSI: 'status-scheduledTime-index',
    sortKey: 'scheduledTime',
    alternativeGSIs: ['leadId-rcsType-index', 'source-createdAt-index']
  }
};

// ═══════════════════════════════════════════════════════════════════════════════
// HELPER: Flatten nested objects for CSV export
// ═══════════════════════════════════════════════════════════════════════════════

function flattenObject(obj, prefix = '', maxDepth = 10, currentDepth = 0) {
  if (obj === null || obj === undefined) return { [prefix || 'value']: '' };
  if (currentDepth >= maxDepth) return { [prefix]: JSON.stringify(obj) };

  const result = {};

  try {
    if (typeof obj !== 'object' || obj === null) {
      result[prefix || 'value'] = obj === null ? '' : obj;
      return result;
    }
    if (Array.isArray(obj)) {
      result[prefix] = JSON.stringify(obj);
      return result;
    }

    for (const key in obj) {
      if (!obj.hasOwnProperty(key)) continue;
      const value = obj[key];
      const newKey = prefix ? `${prefix}.${key}` : key;

      if (value === null || value === undefined) {
        result[newKey] = '';
      } else if (Array.isArray(value)) {
        result[newKey] = JSON.stringify(value);
      } else if (typeof value === 'object' && value.constructor === Object) {
        Object.assign(result, flattenObject(value, newKey, maxDepth, currentDepth + 1));
      } else if (typeof value === 'boolean') {
        result[newKey] = value.toString();
      } else {
        result[newKey] = value;
      }
    }
  } catch (error) {
    result[prefix || 'error'] = `Error: ${error.message}`;
  }

  return result;
}

// ═══════════════════════════════════════════════════════════════════════════════
// CORE QUERY BUILDER
// Figures out the best way to query based on available filters + table config
// ═══════════════════════════════════════════════════════════════════════════════

function buildQueryParams(tableName, filters = {}) {
  const config = TABLE_CONFIG[tableName];
  const MAX_LIMIT = 5000;

  // ─── 1. Direct source query → use source-createdAt-index ──────────────────
  if (filters.source && (!config || config.primaryGSI === 'source-createdAt-index' || config.sources)) {
    return {
      type: 'SINGLE_SOURCE',
      source: filters.source,
      params: buildSourceDateParams(tableName, filters.source, filters, MAX_LIMIT)
    };
  }

  // ─── 2. leadId query → use leadId-index ───────────────────────────────────
  if (filters.leadId) {
    return {
      type: 'LEADID',
      params: buildLeadIdParams(tableName, filters, MAX_LIMIT)
    };
  }

  // ─── 3. phone query ────────────────────────────────────────────────────────
  if (filters.phone) {
    return {
      type: 'PHONE',
      params: buildPhoneParams(tableName, filters, MAX_LIMIT)
    };
  }

  // ─── 4. panNumber query ────────────────────────────────────────────────────
  if (filters.panNumber) {
    return {
      type: 'PAN',
      params: buildPanParams(tableName, filters, MAX_LIMIT)
    };
  }

  // ─── 5. status query for RCS queue ────────────────────────────────────────
  if (filters.status && config?.primaryGSI === 'status-scheduledTime-index') {
    return {
      type: 'STATUS',
      params: buildStatusScheduledTimeParams(tableName, filters, MAX_LIMIT)
    };
  }

  // ─── 6. No source given but table has known sources → multi-source ─────────
  if (config?.sources?.length > 0) {
    return {
      type: 'MULTI_SOURCE',
      sources: config.sources,
      tableName
    };
  }

  // ─── 7. Table requires leadId but none given ───────────────────────────────
  if (config?.requiresLeadId) {
    throw new Error(
      `Table "${tableName}" requires a leadId filter. ` +
      `The source-createdAt-index is not yet active for this table. ` +
      `Please provide: leadId`
    );
  }

  // ─── 8. Nothing useful ────────────────────────────────────────────────────
  const hint = config
    ? `Available: source (${config.sources?.join(', ') || 'any'}), leadId, phone, panNumber`
    : 'Available: source, leadId, phone, panNumber';

  throw new Error(
    `Cannot query "${tableName}" without filters. ${hint}`
  );
}

// ─── Individual param builders ────────────────────────────────────────────────

function buildSourceDateParams(tableName, source, filters, MAX_LIMIT) {
  const params = {
    TableName: tableName,
    IndexName: 'source-createdAt-index',
    KeyConditionExpression: '#src = :source',
    ExpressionAttributeNames: { '#src': 'source' },
    ExpressionAttributeValues: { ':source': source },
    ScanIndexForward: false
  };

  if (filters.startDate && filters.endDate) {
    params.KeyConditionExpression += ' AND #ca BETWEEN :start AND :end';
    params.ExpressionAttributeNames['#ca'] = 'createdAt';
    params.ExpressionAttributeValues[':start'] = filters.startDate;
    params.ExpressionAttributeValues[':end'] = filters.endDate;
  } else if (filters.startDate) {
    params.KeyConditionExpression += ' AND #ca >= :start';
    params.ExpressionAttributeNames['#ca'] = 'createdAt';
    params.ExpressionAttributeValues[':start'] = filters.startDate;
  } else if (filters.endDate) {
    params.KeyConditionExpression += ' AND #ca <= :end';
    params.ExpressionAttributeNames['#ca'] = 'createdAt';
    params.ExpressionAttributeValues[':end'] = filters.endDate;
  }

  addExtraFilters(params, filters, ['source', 'startDate', 'endDate']);
  if (filters.limit) params.Limit = Math.min(parseInt(filters.limit), MAX_LIMIT);

  return params;
}

function buildLeadIdParams(tableName, filters, MAX_LIMIT) {
  const params = {
    TableName: tableName,
    IndexName: 'leadId-index',
    KeyConditionExpression: '#lid = :lid',
    ExpressionAttributeNames: { '#lid': 'leadId' },
    ExpressionAttributeValues: { ':lid': filters.leadId }
  };

  addExtraFilters(params, filters, ['leadId']);
  if (filters.limit) params.Limit = Math.min(parseInt(filters.limit), MAX_LIMIT);

  return params;
}

function buildPhoneParams(tableName, filters, MAX_LIMIT) {
  const params = {
    TableName: tableName,
    IndexName: 'phone-index',
    KeyConditionExpression: '#ph = :ph',
    ExpressionAttributeNames: { '#ph': 'phone' },
    ExpressionAttributeValues: { ':ph': filters.phone }
  };

  addExtraFilters(params, filters, ['phone']);
  if (filters.limit) params.Limit = Math.min(parseInt(filters.limit), MAX_LIMIT);

  return params;
}

function buildPanParams(tableName, filters, MAX_LIMIT) {
  const params = {
    TableName: tableName,
    IndexName: 'panNumber-index',
    KeyConditionExpression: '#pan = :pan',
    ExpressionAttributeNames: { '#pan': 'panNumber' },
    ExpressionAttributeValues: { ':pan': filters.panNumber }
  };

  addExtraFilters(params, filters, ['panNumber']);
  if (filters.limit) params.Limit = Math.min(parseInt(filters.limit), MAX_LIMIT);

  return params;
}

function buildStatusScheduledTimeParams(tableName, filters, MAX_LIMIT) {
  const params = {
    TableName: tableName,
    IndexName: 'status-scheduledTime-index',
    KeyConditionExpression: '#st = :st',
    ScanIndexForward: false,
    ExpressionAttributeNames: { '#st': 'status' },
    ExpressionAttributeValues: { ':st': filters.status }
  };

  if (filters.scheduledTime) {
    params.KeyConditionExpression += ' AND #stime = :stime';
    params.ExpressionAttributeNames['#stime'] = 'scheduledTime';
    params.ExpressionAttributeValues[':stime'] = filters.scheduledTime;
  }

  addExtraFilters(params, filters, ['status', 'scheduledTime']);
  if (filters.limit) params.Limit = Math.min(parseInt(filters.limit), MAX_LIMIT);

  return params;
}

// ─── Adds non-key filters as FilterExpression ─────────────────────────────────
function addExtraFilters(params, filters, excludeFields) {
  const conditions = [];
  const filterable = ['responseStatus', 'status', 'rcsType', 'attempts'];

  filterable.forEach(field => {
    if (filters[field] && !excludeFields.includes(field)) {
      conditions.push(`#${field} = :${field}`);
      params.ExpressionAttributeNames[`#${field}`] = field;
      params.ExpressionAttributeValues[`:${field}`] = filters[field];
    }
  });

  if (conditions.length > 0) {
    params.FilterExpression = conditions.join(' AND ');
  }
}

// ═══════════════════════════════════════════════════════════════════════════════
// CORE EXECUTOR: Runs a single-source query with pagination
// ═══════════════════════════════════════════════════════════════════════════════

async function executeQuery(params) {
  let allItems = [];
  let lastKey = null;

  do {
    if (lastKey) params.ExclusiveStartKey = lastKey;
    const result = await docClient.send(new QueryCommand(params));
    allItems = allItems.concat(result.Items || []);
    lastKey = result.LastEvaluatedKey;
    delete params.ExclusiveStartKey;
  } while (lastKey);

  return allItems;
}

// ═══════════════════════════════════════════════════════════════════════════════
// CORE EXECUTOR: Runs a count-only query with pagination
// ═══════════════════════════════════════════════════════════════════════════════

async function executeCount(params) {
  let totalCount = 0;
  let lastKey = null;

  do {
    if (lastKey) params.ExclusiveStartKey = lastKey;
    const result = await docClient.send(new QueryCommand(params));
    totalCount += result.Count || 0;
    lastKey = result.LastEvaluatedKey;
    delete params.ExclusiveStartKey;
  } while (lastKey);

  return totalCount;
}

// ═══════════════════════════════════════════════════════════════════════════════
// GET TABLES
// ═══════════════════════════════════════════════════════════════════════════════

router.get('/tables', (req, res) => {
  const tables = Object.keys(TABLE_CONFIG).concat([
    'leads_uat', 'lead_distribution_stats',
    'lead_distribution_processing'
  ]);

  res.json({
    tables,
    tableInfo: Object.entries(TABLE_CONFIG).map(([name, config]) => ({
      name,
      type: config.type,
      primaryGSI: config.primaryGSI,
      availableSources: config.sources || null,
      requiresLeadId: config.requiresLeadId || false
    }))
  });
});

// ═══════════════════════════════════════════════════════════════════════════════
// PREVIEW ENDPOINT
// ═══════════════════════════════════════════════════════════════════════════════

router.post('/preview', async (req, res) => {
  try {
    const { tableName, filters = {} } = req.body;

    if (!tableName) {
      return res.status(400).json({ error: 'tableName is required' });
    }

    let queryConfig;
    try {
      queryConfig = buildQueryParams(tableName, filters);
    } catch (err) {
      return res.status(400).json({ error: err.message });
    }

    let items = [];

    if (queryConfig.type === 'MULTI_SOURCE') {
      // Fetch small sample from each source
      for (const source of queryConfig.sources.slice(0, 3)) {
        const params = buildSourceDateParams(tableName, source, filters, 5);
        params.Limit = 3;
        const result = await docClient.send(new QueryCommand(params));
        items = items.concat(result.Items || []);
        if (items.length >= 10) break;
      }
    } else {
      const params = { ...queryConfig.params, Limit: 10 };
      const result = await docClient.send(new QueryCommand(params));
      items = result.Items || [];
    }

    const flattenedData = items.map(item => flattenObject(item));

    res.json({
      preview: flattenedData,
      count: items.length,
      queryType: queryConfig.type,
      indexUsed: queryConfig.params?.IndexName || 'multi-source',
      sources: queryConfig.sources || null
    });

  } catch (error) {
    console.error('Preview error:', error);
    res.status(500).json({ error: error.message });
  }
});

// ═══════════════════════════════════════════════════════════════════════════════
// COUNT ENDPOINT
// ═══════════════════════════════════════════════════════════════════════════════

router.post('/export-count', async (req, res) => {
  try {
    const { tableName, filters = {} } = req.body;

    if (!tableName) {
      return res.status(400).json({ error: 'tableName is required' });
    }

    let queryConfig;
    try {
      queryConfig = buildQueryParams(tableName, filters);
    } catch (err) {
      return res.status(400).json({ error: err.message });
    }

    if (queryConfig.type === 'MULTI_SOURCE') {
      // Count each source in parallel
      const countPromises = queryConfig.sources.map(async source => {
        const params = buildSourceDateParams(tableName, source, filters, 5000);
        params.Select = 'COUNT';
        const count = await executeCount(params);
        return { source, count };
      });

      const results = await Promise.all(countPromises);
      const sourceBreakdown = {};
      let totalCount = 0;

      results.forEach(({ source, count }) => {
        sourceBreakdown[source] = count;
        totalCount += count;
      });

      return res.json({
        count: totalCount,
        sourceBreakdown,
        queryType: 'MULTI_SOURCE',
        indexUsed: 'source-createdAt-index'
      });
    }

    // Single query count
    const countParams = { ...queryConfig.params, Select: 'COUNT' };
    const count = await executeCount(countParams);

    res.json({
      count,
      queryType: queryConfig.type,
      indexUsed: queryConfig.params?.IndexName || 'primary'
    });

  } catch (error) {
    console.error('Count error:', error);
    res.status(500).json({ error: error.message });
  }
});

// ═══════════════════════════════════════════════════════════════════════════════
// EXPORT ENDPOINT (streaming CSV)
// ═══════════════════════════════════════════════════════════════════════════════

router.post('/export', async (req, res) => {
  try {
    const { tableName, filters = {} } = req.body;

    if (!tableName) {
      return res.status(400).json({ error: 'tableName is required' });
    }

    let queryConfig;
    try {
      queryConfig = buildQueryParams(tableName, filters);
    } catch (err) {
      return res.status(400).json({ error: err.message });
    }

    res.header('Content-Type', 'text/csv');
    res.header('Content-Disposition', `attachment; filename="${tableName}_${new Date().toISOString().split('T')[0]}.csv"`);
    res.header('Transfer-Encoding', 'chunked');

    let totalProcessed = 0;
    let headerWritten = false;
    let allFields = new Set();
    let rowBuffer = [];
    const FLUSH_EVERY = 1000;

    // ─── Helper: flush buffer to response ────────────────────────────────────
    const flushBuffer = (force = false) => {
      if (rowBuffer.length === 0) return;
      if (!force && rowBuffer.length < FLUSH_EVERY) return;

      if (!headerWritten) {
        res.write(Array.from(allFields).join(',') + '\n');
        headerWritten = true;
      }

      rowBuffer.forEach(row => {
        const values = Array.from(allFields).map(field => {
          const value = row[field];
          if (value === null || value === undefined) return '';
          const str = String(value);
          return str.includes(',') || str.includes('"') || str.includes('\n')
            ? `"${str.replace(/"/g, '""')}"`
            : str;
        });
        res.write(values.join(',') + '\n');
      });

      rowBuffer = [];
    };

    // ─── Helper: process a page of items ─────────────────────────────────────
    const processItems = (items) => {
      const flat = items.map(item => flattenObject(item));
      flat.forEach(item => Object.keys(item).forEach(k => allFields.add(k)));
      rowBuffer = rowBuffer.concat(flat);
      totalProcessed += items.length;
      flushBuffer();
    };

    // ─── Execute based on query type ─────────────────────────────────────────
    if (queryConfig.type === 'MULTI_SOURCE') {
      console.log(`[EXPORT] Multi-source export for ${tableName}: ${queryConfig.sources.join(', ')}`);

      for (const source of queryConfig.sources) {
        console.log(`[EXPORT] Processing source: ${source}`);
        const params = buildSourceDateParams(tableName, source, filters, 5000);
        let lastKey = null;

        do {
          if (lastKey) params.ExclusiveStartKey = lastKey;
          const result = await docClient.send(new QueryCommand(params));
          processItems(result.Items || []);
          lastKey = result.LastEvaluatedKey;
          delete params.ExclusiveStartKey;
          console.log(`[EXPORT] ${source}: ${totalProcessed} total processed...`);
        } while (lastKey);
      }
    } else {
      console.log(`[EXPORT] ${queryConfig.type} export for ${tableName}`);
      const params = queryConfig.params;
      let lastKey = null;

      do {
        if (lastKey) params.ExclusiveStartKey = lastKey;
        const result = await docClient.send(new QueryCommand(params));
        processItems(result.Items || []);
        lastKey = result.LastEvaluatedKey;
        delete params.ExclusiveStartKey;
        console.log(`[EXPORT] ${totalProcessed} total processed...`);
      } while (lastKey);
    }

    // Final flush
    flushBuffer(true);
    console.log(`[EXPORT] ✅ Complete: ${totalProcessed} records from ${tableName}`);
    res.end();

  } catch (error) {
    console.error('Export error:', error);
    if (!res.headersSent) {
      res.status(500).json({ error: error.message });
    } else {
      res.end();
    }
  }
});

module.exports = router;
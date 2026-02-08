const express = require('express');
const { docClient } = require('../dynamodb');
const { QueryCommand, GetItemCommand } = require('@aws-sdk/lib-dynamodb');

const router = express.Router();

// Get list of all tables
router.get('/tables', async (req, res) => {
  try {
    const tables = [
      'leads', 'excel_leads', 'leads_uat', 'sml_response_logs',
      'freo_response_logs', 'ovly_response_logs', 'lending_plate_response_logs',
      'zype_response_logs', 'fintifi_response_logs', 'fatakpay_response_logs',
      'ramfincrop_logs', 'mpokket_response_logs', 'indialends_response_logs',
      'crmPaisa_response_logs', 'lead_distribution_stats', 'lead_success',
      'lead_distribution_processing', 'rcs_queue'
    ];
    res.json({ tables });
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

// Flatten object recursively
function flattenObject(obj, prefix = '', maxDepth = 10, currentDepth = 0) {
  if (obj === null || obj === undefined) {
    return { [prefix || 'value']: '' };
  }

  if (currentDepth >= maxDepth) {
    return { [prefix]: JSON.stringify(obj) };
  }

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
    console.error('Error flattening object:', error);
    result[prefix || 'error'] = `Error: ${error.message}`;
  }
  
  return result;
}

// ✅ OPTIMIZED: Build Query parameters (NO MORE SCAN)
function buildQueryParams(tableName, filters) {
  // Define log tables that MUST have source filter
  const LOG_TABLES = [
    'sml_response_logs',
    'freo_response_logs',
    'ovly_response_logs',
    'lending_plate_response_logs',
    'zype_response_logs',
    'fintifi_response_logs',
    'fatakpay_response_logs',
    'ramfincrop_logs',
    'mpokket_response_logs',
    'indialends_response_logs',
    'crmPaisa_response_logs'
  ];

  // ✅ BUSINESS RULE: Log tables MUST have source to avoid expensive scans
  if (LOG_TABLES.includes(tableName) && (!filters || !filters.source)) {
    throw new Error(
      `Source is mandatory for ${tableName}. This prevents expensive full table scans. ` +
      `Available sources: OVLY, FREO, SML, ZYPE, FINTIFI, FATAKPAY, MPOKKET, INDIALENDS, CRMPAISAY`
    );
  }

  if (!filters || typeof filters !== 'object') {
    throw new Error('Filters are required. Please specify at least source or leadId to avoid expensive full table scans.');
  }

  // STRATEGY 1: Query by source + createdAt (BEST - uses GSI)
  if (filters.source) {
    const params = {
      TableName: tableName,
      IndexName: 'source-createdAt-index',
      KeyConditionExpression: '#source = :source',
      ScanIndexForward: false, // ✅ Newest records first (descending order)
      ExpressionAttributeNames: {
        '#source': 'source'
      },
      ExpressionAttributeValues: {
        ':source': filters.source
      }
    };

    // Add date range to KeyCondition (SORT KEY) - This is CHEAP ✅
    if (filters.startDate && filters.endDate) {
      params.KeyConditionExpression += ' AND #createdAt BETWEEN :startDate AND :endDate';
      params.ExpressionAttributeNames['#createdAt'] = 'createdAt';
      params.ExpressionAttributeValues[':startDate'] = filters.startDate;
      params.ExpressionAttributeValues[':endDate'] = filters.endDate;
    } else if (filters.startDate) {
      params.KeyConditionExpression += ' AND #createdAt >= :startDate';
      params.ExpressionAttributeNames['#createdAt'] = 'createdAt';
      params.ExpressionAttributeValues[':startDate'] = filters.startDate;
    } else if (filters.endDate) {
      params.KeyConditionExpression += ' AND #createdAt <= :endDate';
      params.ExpressionAttributeNames['#createdAt'] = 'createdAt';
      params.ExpressionAttributeValues[':endDate'] = filters.endDate;
    }

    // Non-indexed fields go to FilterExpression (applied after query)
    const filterConditions = [];
    
    if (filters.responseStatus) {
      filterConditions.push('#responseStatus = :responseStatus');
      params.ExpressionAttributeNames['#responseStatus'] = 'responseStatus';
      params.ExpressionAttributeValues[':responseStatus'] = filters.responseStatus;
    }

    if (filters.leadId) {
      filterConditions.push('#leadId = :leadId');
      params.ExpressionAttributeNames['#leadId'] = 'leadId';
      params.ExpressionAttributeValues[':leadId'] = filters.leadId;
    }

    if (filterConditions.length > 0) {
      params.FilterExpression = filterConditions.join(' AND ');
    }

    // ✅ LIMIT GUARD: Protect memory and prevent abuse
    const MAX_LIMIT = 5000;
    if (filters.limit) {
      params.Limit = Math.min(parseInt(filters.limit), MAX_LIMIT);
    }

    return { type: 'QUERY', params };
  }

  // STRATEGY 2: Query by leadId (uses leadId index for leads table)
  if (filters.leadId && (tableName === 'leads' || tableName === 'excel_leads' || tableName === 'leads_uat')) {
    const params = {
      TableName: tableName,
      IndexName: 'leadId-index',
      KeyConditionExpression: '#leadId = :leadId',
      ExpressionAttributeNames: {
        '#leadId': 'leadId'
      },
      ExpressionAttributeValues: {
        ':leadId': filters.leadId
      }
    };

    // Add date filter if provided
    if (filters.startDate || filters.endDate) {
      const filterConditions = [];
      
      if (filters.startDate && filters.endDate) {
        filterConditions.push('#createdAt BETWEEN :startDate AND :endDate');
        params.ExpressionAttributeNames['#createdAt'] = 'createdAt';
        params.ExpressionAttributeValues[':startDate'] = filters.startDate;
        params.ExpressionAttributeValues[':endDate'] = filters.endDate;
      } else if (filters.startDate) {
        filterConditions.push('#createdAt >= :startDate');
        params.ExpressionAttributeNames['#createdAt'] = 'createdAt';
        params.ExpressionAttributeValues[':startDate'] = filters.startDate;
      } else if (filters.endDate) {
        filterConditions.push('#createdAt <= :endDate');
        params.ExpressionAttributeNames['#createdAt'] = 'createdAt';
        params.ExpressionAttributeValues[':endDate'] = filters.endDate;
      }

      if (filterConditions.length > 0) {
        params.FilterExpression = filterConditions.join(' AND ');
      }
    }

    // ✅ LIMIT GUARD: Protect memory and prevent abuse
    const MAX_LIMIT = 5000;
    if (filters.limit) {
      params.Limit = Math.min(parseInt(filters.limit), MAX_LIMIT);
    }

    return { type: 'QUERY', params };
  }

  // STRATEGY 3: Query by phone (for leads table)
  if (filters.phone && (tableName === 'leads' || tableName === 'excel_leads')) {
    const params = {
      TableName: tableName,
      IndexName: 'phone-index',
      KeyConditionExpression: '#phone = :phone',
      ExpressionAttributeNames: {
        '#phone': 'phone'
      },
      ExpressionAttributeValues: {
        ':phone': filters.phone
      }
    };

    // ✅ LIMIT GUARD: Protect memory and prevent abuse
    const MAX_LIMIT = 5000;
    if (filters.limit) {
      params.Limit = Math.min(parseInt(filters.limit), MAX_LIMIT);
    }

    return { type: 'QUERY', params };
  }

  // STRATEGY 4: Query by panNumber (for leads table)
  if (filters.panNumber && (tableName === 'leads' || tableName === 'excel_leads')) {
    const params = {
      TableName: tableName,
      IndexName: 'panNumber-index',
      KeyConditionExpression: '#panNumber = :panNumber',
      ExpressionAttributeNames: {
        '#panNumber': 'panNumber'
      },
      ExpressionAttributeValues: {
        ':panNumber': filters.panNumber
      }
    };

    // ✅ LIMIT GUARD: Protect memory and prevent abuse
    const MAX_LIMIT = 5000;
    if (filters.limit) {
      params.Limit = Math.min(parseInt(filters.limit), MAX_LIMIT);
    }

    return { type: 'QUERY', params };
  }

  // ❌ REJECT: No valid query path available
  throw new Error(
    `Invalid filter combination. To avoid expensive scans, please provide one of:
    - source (required for response logs)
    - leadId (for leads tables)
    - phone (for leads tables)
    - panNumber (for leads tables)
    
    Current filters: ${JSON.stringify(Object.keys(filters))}`
  );
}

// ✅ STREAMING EXPORT - Now uses QUERY instead of SCAN
router.post('/export', async (req, res) => {
  try {
    const { tableName, filters } = req.body;
    
    console.log('Export request:', { tableName, filters });
    
    if (!tableName) {
      return res.status(400).json({ error: 'Table name is required' });
    }

    // Validate and build query params
    let queryConfig;
    try {
      queryConfig = buildQueryParams(tableName, filters);
    } catch (error) {
      return res.status(400).json({ 
        error: error.message,
        hint: 'Provide source, leadId, phone, or panNumber to enable efficient querying'
      });
    }

    // Set headers for streaming CSV
    res.header('Content-Type', 'text/csv');
    res.header('Content-Disposition', `attachment; filename="${tableName}_export_${new Date().toISOString().split('T')[0]}.csv"`);
    res.header('Transfer-Encoding', 'chunked');

    const params = queryConfig.params;
    
    let lastEvaluatedKey = null;
    let totalProcessed = 0;
    let headerWritten = false;
    let allFields = new Set();
    let buffer = [];
    const BATCH_SIZE = 1000;

    console.log(`Using ${queryConfig.type} operation with index: ${params.IndexName || 'primary key'}`);

    do {
      if (lastEvaluatedKey) {
        params.ExclusiveStartKey = lastEvaluatedKey;
      }

      // ✅ Use QueryCommand instead of ScanCommand
      const result = await docClient.send(new QueryCommand(params));
      const items = (result.Items || []).filter(item => item && typeof item === 'object');
      
      // Flatten items
      const flattenedBatch = items.map(item => flattenObject(item));
      
      // Collect all fields for consistent CSV columns
      flattenedBatch.forEach(item => {
        Object.keys(item).forEach(key => allFields.add(key));
      });

      buffer = buffer.concat(flattenedBatch);
      totalProcessed += items.length;

      // Write batch when buffer is full or this is the last batch
      if (buffer.length >= BATCH_SIZE || !result.LastEvaluatedKey) {
        if (!headerWritten && buffer.length > 0) {
          // Write CSV header
          const header = Array.from(allFields).join(',') + '\n';
          res.write(header);
          headerWritten = true;
        }

        // Write CSV rows
        buffer.forEach(row => {
          const values = Array.from(allFields).map(field => {
            const value = row[field];
            if (value === null || value === undefined) return '';
            // Escape commas and quotes in CSV
            const stringValue = String(value);
            if (stringValue.includes(',') || stringValue.includes('"') || stringValue.includes('\n')) {
              return `"${stringValue.replace(/"/g, '""')}"`;
            }
            return stringValue;
          });
          res.write(values.join(',') + '\n');
        });

        buffer = [];
      }

      lastEvaluatedKey = result.LastEvaluatedKey;
      console.log(`Processed ${totalProcessed} items using ${queryConfig.type}...`);

      delete params.ExclusiveStartKey;
      
    } while (lastEvaluatedKey);

    console.log(`✅ Export complete: ${totalProcessed} items exported using ${queryConfig.type}`);
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

// ✅ OPTIMIZED: Get accurate count using Query (not full table scan)
router.post('/export-count', async (req, res) => {
  try {
    const { tableName, filters } = req.body;
    
    if (!tableName) {
      return res.status(400).json({ error: 'Table name is required' });
    }

    // Validate and build query params
    let queryConfig;
    try {
      queryConfig = buildQueryParams(tableName, filters);
    } catch (error) {
      return res.status(400).json({ 
        error: error.message,
        hint: 'Provide source, leadId, phone, or panNumber to get count'
      });
    }

    const params = { ...queryConfig.params, Select: 'COUNT' };

    let totalCount = 0;
    let lastEvaluatedKey = null;
    let iterations = 0;
    const MAX_ITERATIONS = 100; // Safety limit

    do {
      if (lastEvaluatedKey) {
        params.ExclusiveStartKey = lastEvaluatedKey;
      }

      const result = await docClient.send(new QueryCommand(params));
      totalCount += result.Count || 0;
      lastEvaluatedKey = result.LastEvaluatedKey;
      iterations++;

      delete params.ExclusiveStartKey;

      // Safety break
      if (iterations >= MAX_ITERATIONS) {
        console.warn(`Count reached max iterations (${MAX_ITERATIONS}), returning estimate`);
        break;
      }
      
    } while (lastEvaluatedKey);

    res.json({ 
      count: totalCount,
      isComplete: !lastEvaluatedKey,
      queryType: queryConfig.type,
      indexUsed: params.IndexName || 'primary'
    });

  } catch (error) {
    console.error('Count error:', error);
    res.status(500).json({ error: error.message });
  }
});

// ✅ NEW: Preview endpoint - shows first 10 rows without full export
router.post('/preview', async (req, res) => {
  try {
    const { tableName, filters } = req.body;
    
    if (!tableName) {
      return res.status(400).json({ error: 'Table name is required' });
    }

    // Validate and build query params
    let queryConfig;
    try {
      queryConfig = buildQueryParams(tableName, filters);
    } catch (error) {
      return res.status(400).json({ 
        error: error.message,
        hint: 'Provide source, leadId, phone, or panNumber to preview data'
      });
    }

    const params = { ...queryConfig.params, Limit: 10 };

    const result = await docClient.send(new QueryCommand(params));
    const items = (result.Items || []).filter(item => item && typeof item === 'object');
    const flattenedData = items.map(item => flattenObject(item));

    res.json({
      preview: flattenedData,
      hasMore: !!result.LastEvaluatedKey,
      count: items.length,
      queryType: queryConfig.type,
      indexUsed: params.IndexName || 'primary'
    });

  } catch (error) {
    console.error('Preview error:', error);
    res.status(500).json({ error: error.message });
  }
});

module.exports = router;
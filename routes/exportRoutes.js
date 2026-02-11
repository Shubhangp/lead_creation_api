const express = require('express');
const { docClient } = require('../dynamodb');
const { QueryCommand, ScanCommand } = require('@aws-sdk/lib-dynamodb');

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
      'lead_distribution_processing', 'rcs_queue', 'mmm_response_logs'
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

// ✅ TABLE-SPECIFIC INDEX CONFIGURATION
// This maps each table to its available and working GSIs
const TABLE_INDEX_CONFIG = {
  // Response logs with ACTIVE source-createdAt-index
  'ovly_response_logs': {
    primary: 'source-createdAt-index',
    fallback: 'leadId-index',
    sourceRequired: false // Has data in source-createdAt-index
  },
  'mpokket_response_logs': {
    primary: 'source-createdAt-index',
    fallback: 'leadId-index',
    alternative: 'status-index', // Can use status + createdAt
    sourceRequired: false
  },
  'indialends_response_logs': {
    primary: 'source-createdAt-index',
    fallback: 'leadId-createdAt-index', // Different pattern!
    sourceRequired: false
  },
  'mmm_response_logs': {
    primary: 'source-createdAt-index',
    fallback: 'leadId-index',
    sourceRequired: false
  },
  'sml_response_logs': {
    primary: 'source-createdAt-index',
    fallback: 'leadId-index',
    sourceRequired: false
  },
  'freo_response_logs': {
    primary: 'source-createdAt-index',
    fallback: 'leadId-index',
    sourceRequired: false
  },
  'crmPaisa_response_logs': {
    primary: 'source-createdAt-index',
    fallback: 'leadId-index',
    sourceRequired: false
  },
  
  // Response logs with CREATING source-createdAt-index (must use leadId)
  'zype_response_logs': {
    primary: 'leadId-index', // source-createdAt is Creating
    sourceRequired: false
  },
  'ramfincrop_logs': {
    primary: 'leadId-index', // source-createdAt is Creating
    sourceRequired: false
  },
  'lending_plate_response_logs': {
    primary: 'leadId-index', // source-createdAt is Creating
    sourceRequired: false
  },
  'fatakpay_response_logs': {
    primary: 'leadId-index', // source-createdAt is Creating
    sourceRequired: false
  },
  'fintifi_response_logs': {
    primary: 'source-createdAt-index',
    fallback: 'leadId-index',
    sourceRequired: false
  },
  
  // Leads tables
  'leads': {
    primary: 'source-createdAt-index',
    alternatives: ['phone-index', 'panNumber-index'],
    sourceRequired: false
  },
  'excel_leads': {
    primary: 'source-createdAt-index',
    alternatives: ['phone-index', 'panNumber-index'],
    sourceRequired: false
  },
  'lead_success': {
    primary: 'leadId-index',
    alternatives: ['phone-index', 'panNumber-index', 'source-createdAt-index'],
    sourceRequired: false
  },
  
  // RCS Queue - special table
  'rcs_queue': {
    primary: 'status-scheduledTime-index', // Most common query
    alternatives: ['leadId-rcsType-index', 'status-attempts-index', 'source-createdAt-index'],
    sourceRequired: false
  }
};

// ✅ SMART QUERY BUILDER - Detects best index automatically
function buildQueryParams(tableName, filters) {
  if (!filters || typeof filters !== 'object') {
    throw new Error('Filters are required. Please specify at least one filter field.');
  }

  const config = TABLE_INDEX_CONFIG[tableName];
  const MAX_LIMIT = 5000;

  // STRATEGY 1: Try primary index based on table config
  if (config) {
    // Check if we can use primary index
    if (config.primary === 'source-createdAt-index' && filters.source) {
      return buildSourceCreatedAtQuery(tableName, filters, MAX_LIMIT);
    }
    
    if (config.primary === 'leadId-index' && filters.leadId) {
      return buildLeadIdQuery(tableName, filters, MAX_LIMIT);
    }
    
    if (config.primary === 'leadId-createdAt-index' && filters.leadId) {
      return buildLeadIdCreatedAtQuery(tableName, filters, MAX_LIMIT);
    }
    
    if (config.primary === 'status-scheduledTime-index' && filters.status) {
      return buildStatusScheduledTimeQuery(tableName, filters, MAX_LIMIT);
    }
    
    // Try alternative indexes
    if (config.alternatives) {
      if (filters.phone && config.alternatives.includes('phone-index')) {
        return buildPhoneQuery(tableName, filters, MAX_LIMIT);
      }
      if (filters.panNumber && config.alternatives.includes('panNumber-index')) {
        return buildPanNumberQuery(tableName, filters, MAX_LIMIT);
      }
      if (filters.status && config.alternatives.includes('status-index')) {
        return buildStatusQuery(tableName, filters, MAX_LIMIT);
      }
      if (filters.leadId && config.alternatives.includes('leadId-rcsType-index')) {
        return buildLeadIdRcsTypeQuery(tableName, filters, MAX_LIMIT);
      }
    }
    
    // Try fallback index
    if (config.fallback === 'leadId-index' && filters.leadId) {
      return buildLeadIdQuery(tableName, filters, MAX_LIMIT);
    }
    
    if (config.fallback === 'leadId-createdAt-index' && filters.leadId) {
      return buildLeadIdCreatedAtQuery(tableName, filters, MAX_LIMIT);
    }
  }

  // FALLBACK: Generic strategies for tables without config
  if (filters.source) {
    return buildSourceCreatedAtQuery(tableName, filters, MAX_LIMIT);
  }
  
  if (filters.leadId) {
    return buildLeadIdQuery(tableName, filters, MAX_LIMIT);
  }
  
  if (filters.phone) {
    return buildPhoneQuery(tableName, filters, MAX_LIMIT);
  }
  
  if (filters.panNumber) {
    return buildPanNumberQuery(tableName, filters, MAX_LIMIT);
  }

  // ❌ No valid query path found
  const availableFilters = config ? 
    `Try: ${config.primary}${config.fallback ? `, ${config.fallback}` : ''}${config.alternatives ? `, ${config.alternatives.join(', ')}` : ''}` :
    'Try: source, leadId, phone, or panNumber';
  
  throw new Error(
    `Cannot query ${tableName} with provided filters. ${availableFilters}. ` +
    `Current filters: ${JSON.stringify(Object.keys(filters))}`
  );
}

// Query builders for different index patterns
function buildSourceCreatedAtQuery(tableName, filters, MAX_LIMIT) {
  const params = {
    TableName: tableName,
    IndexName: 'source-createdAt-index',
    KeyConditionExpression: '#source = :source',
    ScanIndexForward: false,
    ExpressionAttributeNames: { '#source': 'source' },
    ExpressionAttributeValues: { ':source': filters.source }
  };

  // Add date range to key condition
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

  addFilterExpression(params, filters, ['source', 'startDate', 'endDate']);
  
  if (filters.limit) {
    params.Limit = Math.min(parseInt(filters.limit), MAX_LIMIT);
  }

  return { type: 'QUERY', params };
}

function buildLeadIdQuery(tableName, filters, MAX_LIMIT) {
  const params = {
    TableName: tableName,
    IndexName: 'leadId-index',
    KeyConditionExpression: '#leadId = :leadId',
    ExpressionAttributeNames: { '#leadId': 'leadId' },
    ExpressionAttributeValues: { ':leadId': filters.leadId }
  };

  addFilterExpression(params, filters, ['leadId']);
  
  if (filters.limit) {
    params.Limit = Math.min(parseInt(filters.limit), MAX_LIMIT);
  }

  return { type: 'QUERY', params };
}

function buildLeadIdCreatedAtQuery(tableName, filters, MAX_LIMIT) {
  const params = {
    TableName: tableName,
    IndexName: 'leadId-createdAt-index',
    KeyConditionExpression: '#leadId = :leadId',
    ScanIndexForward: false,
    ExpressionAttributeNames: { '#leadId': 'leadId' },
    ExpressionAttributeValues: { ':leadId': filters.leadId }
  };

  // Add date range to key condition if available
  if (filters.startDate && filters.endDate) {
    params.KeyConditionExpression += ' AND #createdAt BETWEEN :startDate AND :endDate';
    params.ExpressionAttributeNames['#createdAt'] = 'createdAt';
    params.ExpressionAttributeValues[':startDate'] = filters.startDate;
    params.ExpressionAttributeValues[':endDate'] = filters.endDate;
  } else if (filters.startDate) {
    params.KeyConditionExpression += ' AND #createdAt >= :startDate';
    params.ExpressionAttributeNames['#createdAt'] = 'createdAt';
    params.ExpressionAttributeValues[':startDate'] = filters.startDate;
  }

  addFilterExpression(params, filters, ['leadId', 'startDate', 'endDate']);
  
  if (filters.limit) {
    params.Limit = Math.min(parseInt(filters.limit), MAX_LIMIT);
  }

  return { type: 'QUERY', params };
}

function buildPhoneQuery(tableName, filters, MAX_LIMIT) {
  const params = {
    TableName: tableName,
    IndexName: 'phone-index',
    KeyConditionExpression: '#phone = :phone',
    ExpressionAttributeNames: { '#phone': 'phone' },
    ExpressionAttributeValues: { ':phone': filters.phone }
  };

  addFilterExpression(params, filters, ['phone']);
  
  if (filters.limit) {
    params.Limit = Math.min(parseInt(filters.limit), MAX_LIMIT);
  }

  return { type: 'QUERY', params };
}

function buildPanNumberQuery(tableName, filters, MAX_LIMIT) {
  const params = {
    TableName: tableName,
    IndexName: 'panNumber-index',
    KeyConditionExpression: '#panNumber = :panNumber',
    ExpressionAttributeNames: { '#panNumber': 'panNumber' },
    ExpressionAttributeValues: { ':panNumber': filters.panNumber }
  };

  addFilterExpression(params, filters, ['panNumber']);
  
  if (filters.limit) {
    params.Limit = Math.min(parseInt(filters.limit), MAX_LIMIT);
  }

  return { type: 'QUERY', params };
}

function buildStatusQuery(tableName, filters, MAX_LIMIT) {
  const params = {
    TableName: tableName,
    IndexName: 'status-index',
    KeyConditionExpression: '#status = :status',
    ScanIndexForward: false,
    ExpressionAttributeNames: { '#status': 'status' },
    ExpressionAttributeValues: { ':status': filters.status }
  };

  // Add date range if index has createdAt as sort key
  if (filters.startDate && filters.endDate) {
    params.KeyConditionExpression += ' AND #createdAt BETWEEN :startDate AND :endDate';
    params.ExpressionAttributeNames['#createdAt'] = 'createdAt';
    params.ExpressionAttributeValues[':startDate'] = filters.startDate;
    params.ExpressionAttributeValues[':endDate'] = filters.endDate;
  }

  addFilterExpression(params, filters, ['status', 'startDate', 'endDate']);
  
  if (filters.limit) {
    params.Limit = Math.min(parseInt(filters.limit), MAX_LIMIT);
  }

  return { type: 'QUERY', params };
}

function buildStatusScheduledTimeQuery(tableName, filters, MAX_LIMIT) {
  const params = {
    TableName: tableName,
    IndexName: 'status-scheduledTime-index',
    KeyConditionExpression: '#status = :status',
    ScanIndexForward: false,
    ExpressionAttributeNames: { '#status': 'status' },
    ExpressionAttributeValues: { ':status': filters.status }
  };

  if (filters.scheduledTime) {
    params.KeyConditionExpression += ' AND #scheduledTime = :scheduledTime';
    params.ExpressionAttributeNames['#scheduledTime'] = 'scheduledTime';
    params.ExpressionAttributeValues[':scheduledTime'] = filters.scheduledTime;
  }

  addFilterExpression(params, filters, ['status', 'scheduledTime']);
  
  if (filters.limit) {
    params.Limit = Math.min(parseInt(filters.limit), MAX_LIMIT);
  }

  return { type: 'QUERY', params };
}

function buildLeadIdRcsTypeQuery(tableName, filters, MAX_LIMIT) {
  const params = {
    TableName: tableName,
    IndexName: 'leadId-rcsType-index',
    KeyConditionExpression: '#leadId = :leadId',
    ExpressionAttributeNames: { '#leadId': 'leadId' },
    ExpressionAttributeValues: { ':leadId': filters.leadId }
  };

  if (filters.rcsType) {
    params.KeyConditionExpression += ' AND #rcsType = :rcsType';
    params.ExpressionAttributeNames['#rcsType'] = 'rcsType';
    params.ExpressionAttributeValues[':rcsType'] = filters.rcsType;
  }

  addFilterExpression(params, filters, ['leadId', 'rcsType']);
  
  if (filters.limit) {
    params.Limit = Math.min(parseInt(filters.limit), MAX_LIMIT);
  }

  return { type: 'QUERY', params };
}

// Helper to add FilterExpression for non-key fields
function addFilterExpression(params, filters, excludeFields) {
  const filterConditions = [];
  
  // Common filter fields
  const filterableFields = {
    responseStatus: 'responseStatus',
    leadId: 'leadId',
    source: 'source',
    status: 'status',
    phone: 'phone',
    panNumber: 'panNumber',
    rcsType: 'rcsType',
    attempts: 'attempts'
  };

  Object.keys(filterableFields).forEach(field => {
    if (filters[field] && !excludeFields.includes(field)) {
      const attributeName = `#${field}`;
      const attributeValue = `:${field}`;
      
      filterConditions.push(`${attributeName} = ${attributeValue}`);
      params.ExpressionAttributeNames[attributeName] = filterableFields[field];
      params.ExpressionAttributeValues[attributeValue] = filters[field];
    }
  });

  // Date filters (if not in key condition)
  if (filters.startDate && !excludeFields.includes('startDate') && !excludeFields.includes('endDate')) {
    if (filters.endDate) {
      filterConditions.push('#createdAt BETWEEN :startDate AND :endDate');
      params.ExpressionAttributeNames['#createdAt'] = 'createdAt';
      params.ExpressionAttributeValues[':startDate'] = filters.startDate;
      params.ExpressionAttributeValues[':endDate'] = filters.endDate;
    } else {
      filterConditions.push('#createdAt >= :startDate');
      params.ExpressionAttributeNames['#createdAt'] = 'createdAt';
      params.ExpressionAttributeValues[':startDate'] = filters.startDate;
    }
  } else if (filters.endDate && !excludeFields.includes('endDate')) {
    filterConditions.push('#createdAt <= :endDate');
    params.ExpressionAttributeNames['#createdAt'] = 'createdAt';
    params.ExpressionAttributeValues[':endDate'] = filters.endDate;
  }

  if (filterConditions.length > 0) {
    params.FilterExpression = filterConditions.join(' AND ');
  }
}

// ✅ STREAMING EXPORT
router.post('/export', async (req, res) => {
  try {
    const { tableName, filters } = req.body;
    
    console.log('Export request:', { tableName, filters });
    
    if (!tableName) {
      return res.status(400).json({ error: 'Table name is required' });
    }

    let queryConfig;
    try {
      queryConfig = buildQueryParams(tableName, filters);
    } catch (error) {
      return res.status(400).json({ 
        error: error.message,
        hint: 'Provide at least one indexed field (source, leadId, phone, panNumber, status, etc.)'
      });
    }

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

    console.log(`Using ${queryConfig.type} with index: ${params.IndexName || 'primary key'}`);

    do {
      if (lastEvaluatedKey) {
        params.ExclusiveStartKey = lastEvaluatedKey;
      }

      const result = await docClient.send(new QueryCommand(params));
      const items = (result.Items || []).filter(item => item && typeof item === 'object');
      
      const flattenedBatch = items.map(item => flattenObject(item));
      
      flattenedBatch.forEach(item => {
        Object.keys(item).forEach(key => allFields.add(key));
      });

      buffer = buffer.concat(flattenedBatch);
      totalProcessed += items.length;

      if (buffer.length >= BATCH_SIZE || !result.LastEvaluatedKey) {
        if (!headerWritten && buffer.length > 0) {
          const header = Array.from(allFields).join(',') + '\n';
          res.write(header);
          headerWritten = true;
        }

        buffer.forEach(row => {
          const values = Array.from(allFields).map(field => {
            const value = row[field];
            if (value === null || value === undefined) return '';
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
      console.log(`Processed ${totalProcessed} items...`);

      delete params.ExclusiveStartKey;
      
    } while (lastEvaluatedKey);

    console.log(`✅ Export complete: ${totalProcessed} items`);
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

// ✅ COUNT ENDPOINT
router.post('/export-count', async (req, res) => {
  try {
    const { tableName, filters } = req.body;
    
    if (!tableName) {
      return res.status(400).json({ error: 'Table name is required' });
    }

    let queryConfig;
    try {
      queryConfig = buildQueryParams(tableName, filters);
    } catch (error) {
      return res.status(400).json({ 
        error: error.message,
        hint: 'Provide at least one indexed field to get count'
      });
    }

    const params = { ...queryConfig.params, Select: 'COUNT' };

    let totalCount = 0;
    let lastEvaluatedKey = null;
    let iterations = 0;
    const MAX_ITERATIONS = 100;

    do {
      if (lastEvaluatedKey) {
        params.ExclusiveStartKey = lastEvaluatedKey;
      }

      const result = await docClient.send(new QueryCommand(params));
      totalCount += result.Count || 0;
      lastEvaluatedKey = result.LastEvaluatedKey;
      iterations++;

      delete params.ExclusiveStartKey;

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

// ✅ PREVIEW ENDPOINT
router.post('/preview', async (req, res) => {
  try {
    const { tableName, filters } = req.body;
    
    if (!tableName) {
      return res.status(400).json({ error: 'Table name is required' });
    }

    let queryConfig;
    try {
      queryConfig = buildQueryParams(tableName, filters);
    } catch (error) {
      return res.status(400).json({ 
        error: error.message,
        hint: 'Provide at least one indexed field to preview data'
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
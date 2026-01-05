const express = require('express');
const { docClient } = require('../dynamodb');
const { ScanCommand } = require('@aws-sdk/lib-dynamodb');
const { Parser } = require('json2csv');
const { Writable } = require('stream');

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
      'lead_distribution_processing'
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

// Build scan parameters
function buildScanParams(tableName, filters) {
  const params = { TableName: tableName };

  if (!filters || typeof filters !== 'object' || Object.keys(filters).length === 0) {
    return params;
  }

  const conditions = [];
  const expressionAttributeNames = {};
  const expressionAttributeValues = {};

  // Date range filter
  if (filters.startDate && filters.endDate) {
    conditions.push(`#createdAt BETWEEN :startDate AND :endDate`);
    expressionAttributeNames['#createdAt'] = 'createdAt';
    expressionAttributeValues[':startDate'] = filters.startDate;
    expressionAttributeValues[':endDate'] = filters.endDate;
  } else if (filters.startDate) {
    conditions.push(`#createdAt >= :startDate`);
    expressionAttributeNames['#createdAt'] = 'createdAt';
    expressionAttributeValues[':startDate'] = filters.startDate;
  } else if (filters.endDate) {
    conditions.push(`#createdAt <= :endDate`);
    expressionAttributeNames['#createdAt'] = 'createdAt';
    expressionAttributeValues[':endDate'] = filters.endDate;
  }

  // Response status filter
  if (filters.responseStatus) {
    conditions.push(`#responseStatus = :responseStatus`);
    expressionAttributeNames['#responseStatus'] = 'responseStatus';
    expressionAttributeValues[':responseStatus'] = filters.responseStatus;
  }

  // Source filter
  if (filters.source) {
    conditions.push(`#src = :source`);
    expressionAttributeNames['#src'] = 'source';
    expressionAttributeValues[':source'] = filters.source;
  }

  // Lead ID filter
  if (filters.leadId) {
    conditions.push(`#leadId = :leadId`);
    expressionAttributeNames['#leadId'] = 'leadId';
    expressionAttributeValues[':leadId'] = filters.leadId;
  }

  if (conditions.length > 0) {
    params.FilterExpression = conditions.join(' AND ');
    params.ExpressionAttributeNames = expressionAttributeNames;
    params.ExpressionAttributeValues = expressionAttributeValues;
  }

  return params;
}

// STREAMING EXPORT - Handles large datasets
router.post('/export', async (req, res) => {
  try {
    const { tableName, filters } = req.body;
    
    console.log('Export request:', { tableName, filters });
    
    if (!tableName) {
      return res.status(400).json({ error: 'Table name is required' });
    }

    // Set headers for streaming CSV
    res.header('Content-Type', 'text/csv');
    res.header('Content-Disposition', `attachment; filename="${tableName}_export_${new Date().toISOString().split('T')[0]}.csv"`);
    res.header('Transfer-Encoding', 'chunked');

    const params = buildScanParams(tableName, filters);
    
    let lastEvaluatedKey = null;
    let totalProcessed = 0;
    let headerWritten = false;
    let allFields = new Set();
    let buffer = [];
    const BATCH_SIZE = 1000; // Process in batches

    do {
      if (lastEvaluatedKey) {
        params.ExclusiveStartKey = lastEvaluatedKey;
      }

      const result = await docClient.send(new ScanCommand(params));
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
      console.log(`Processed ${totalProcessed} items...`);

      delete params.ExclusiveStartKey;
      
    } while (lastEvaluatedKey);

    console.log(`Export complete: ${totalProcessed} items`);
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

// ALTERNATIVE: Paginated Export for very large datasets
router.post('/export-paginated', async (req, res) => {
  try {
    const { tableName, filters, page = 1, pageSize = 5000 } = req.body;
    
    if (!tableName) {
      return res.status(400).json({ error: 'Table name is required' });
    }

    const params = buildScanParams(tableName, filters);
    params.Limit = pageSize;

    // Calculate pagination
    let lastEvaluatedKey = null;
    let currentPage = 1;
    
    // Skip to requested page
    while (currentPage < page) {
      const result = await docClient.send(new ScanCommand(params));
      lastEvaluatedKey = result.LastEvaluatedKey;
      if (!lastEvaluatedKey) break;
      params.ExclusiveStartKey = lastEvaluatedKey;
      currentPage++;
    }

    // Get current page data
    const result = await docClient.send(new ScanCommand(params));
    const items = (result.Items || []).filter(item => item && typeof item === 'object');
    const flattenedData = items.map(item => flattenObject(item));

    // Convert to CSV
    const parser = new Parser();
    const csv = parser.parse(flattenedData);

    res.header('Content-Type', 'text/csv');
    res.header('Content-Disposition', `attachment; filename="${tableName}_export_page${page}_${new Date().toISOString().split('T')[0]}.csv"`);
    res.send(csv);

  } catch (error) {
    console.error('Paginated export error:', error);
    res.status(500).json({ error: error.message });
  }
});

// Get estimate of total records (for progress indication)
router.post('/export-estimate', async (req, res) => {
  try {
    const { tableName, filters } = req.body;
    
    if (!tableName) {
      return res.status(400).json({ error: 'Table name is required' });
    }

    const params = buildScanParams(tableName, filters);
    params.Select = 'COUNT';

    let totalCount = 0;
    let lastEvaluatedKey = null;

    // Quick scan to count (limit to 10 iterations for estimate)
    for (let i = 0; i < 10; i++) {
      if (lastEvaluatedKey) {
        params.ExclusiveStartKey = lastEvaluatedKey;
      }

      const result = await docClient.send(new ScanCommand(params));
      totalCount += result.Count || 0;
      lastEvaluatedKey = result.LastEvaluatedKey;

      if (!lastEvaluatedKey) break;
      delete params.ExclusiveStartKey;
    }

    res.json({ 
      estimatedCount: totalCount,
      isEstimate: !!lastEvaluatedKey 
    });

  } catch (error) {
    console.error('Estimate error:', error);
    res.status(500).json({ error: error.message });
  }
});

module.exports = router;
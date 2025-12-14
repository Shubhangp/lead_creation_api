const express = require('express');
const { docClient } = require('../dynamodb');
const { ScanCommand } = require('@aws-sdk/lib-dynamodb');
const { Parser } = require('json2csv');

const router = express.Router();

// Get list of all tables
router.get('/tables', async (req, res) => {
  try {
    const tables = [
      'leads',
      'excel_leads',
      'leads_uat',
      'sml_response_logs',
      'freo_response_logs',
      'ovly_response_logs',
      'leadingplate_response_logs',
      'zype_response_logs',
      'fintifi_response_logs',
      'fatakpay_response_logs',
      'ramfincrop_logs',
      'mpokket_response_logs',
      'indialends_response_logs',
      'lead_success'
    ];
    
    res.json({ tables });
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

// Flatten object recursively
function flattenObject(obj, prefix = '', maxDepth = 10, currentDepth = 0) {
  if (currentDepth >= maxDepth) {
    return { [prefix]: JSON.stringify(obj) };
  }

  const result = {};
  
  for (const key in obj) {
    const value = obj[key];
    const newKey = prefix ? `${prefix}.${key}` : key;
    
    if (value === null || value === undefined) {
      result[newKey] = '';
    } else if (Array.isArray(value)) {
      result[newKey] = JSON.stringify(value);
    } else if (typeof value === 'object' && value.constructor === Object) {
      Object.assign(result, flattenObject(value, newKey, maxDepth, currentDepth + 1));
    } else {
      result[newKey] = value;
    }
  }
  
  return result;
}

// Export table with filters
router.post('/export', async (req, res) => {
  try {
    const { tableName, filters } = req.body;
    
    console.log('Export request:', { tableName, filters });
    
    let items = [];
    let lastEvaluatedKey = null;
    let scannedCount = 0;

    // Build filter expression
    let filterExpression = '';
    const expressionAttributeNames = {};
    const expressionAttributeValues = {};
    let filterIndex = 0;

    if (filters) {
      const conditions = [];

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
        conditions.push(`#source = :source`);
        expressionAttributeNames['#source'] = 'source';
        expressionAttributeValues[':source'] = filters.source;
      }

      // Lead ID filter
      if (filters.leadId) {
        conditions.push(`#leadId = :leadId`);
        expressionAttributeNames['#leadId'] = 'leadId';
        expressionAttributeValues[':leadId'] = filters.leadId;
      }

      if (conditions.length > 0) {
        filterExpression = conditions.join(' AND ');
      }
    }

    // Scan table with filters
    do {
      const params = {
        TableName: tableName,
        ExclusiveStartKey: lastEvaluatedKey
      };

      if (filterExpression) {
        params.FilterExpression = filterExpression;
        params.ExpressionAttributeNames = expressionAttributeNames;
        params.ExpressionAttributeValues = expressionAttributeValues;
      }

      const result = await docClient.send(new ScanCommand(params));
      items = items.concat(result.Items || []);
      lastEvaluatedKey = result.LastEvaluatedKey;
      scannedCount += result.Count || 0;
      
      console.log(`Scanned ${scannedCount} items, filtered to ${items.length}...`);
    } while (lastEvaluatedKey);

    if (items.length === 0) {
      return res.status(404).json({ 
        error: 'No data found matching the filters',
        scannedCount 
      });
    }

    // Flatten all items
    const flattenedData = items.map(item => flattenObject(item));

    // Convert to CSV
    const parser = new Parser();
    const csv = parser.parse(flattenedData);

    res.header('Content-Type', 'text/csv');
    res.attachment(`${tableName}_export_${new Date().toISOString().split('T')[0]}.csv`);
    res.send(csv);
  } catch (error) {
    console.error('Export error:', error);
    res.status(500).json({ error: error.message });
  }
});

module.exports = router;
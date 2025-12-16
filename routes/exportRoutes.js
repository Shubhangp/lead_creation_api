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

// Export table with filters
router.post('/export', async (req, res) => {
  try {
    const { tableName, filters } = req.body;
    
    console.log('Export request received:', { tableName, filters });
    
    if (!tableName) {
      return res.status(400).json({ error: 'Table name is required' });
    }

    let items = [];
    let lastEvaluatedKey = null;
    let scannedCount = 0;

    // Build scan parameters
    const params = {
      TableName: tableName
    };

    // Only build filter if filters object exists and has values
    if (filters && typeof filters === 'object' && Object.keys(filters).length > 0) {
      const conditions = [];
      const expressionAttributeNames = {};
      const expressionAttributeValues = {};

      // Date range filter
      if (filters.startDate && filters.endDate && 
          filters.startDate !== '' && filters.endDate !== '' &&
          filters.startDate !== null && filters.endDate !== null) {
        conditions.push(`#createdAt BETWEEN :startDate AND :endDate`);
        expressionAttributeNames['#createdAt'] = 'createdAt';
        expressionAttributeValues[':startDate'] = filters.startDate;
        expressionAttributeValues[':endDate'] = filters.endDate;
      } else if (filters.startDate && filters.startDate !== '' && filters.startDate !== null) {
        conditions.push(`#createdAt >= :startDate`);
        expressionAttributeNames['#createdAt'] = 'createdAt';
        expressionAttributeValues[':startDate'] = filters.startDate;
      } else if (filters.endDate && filters.endDate !== '' && filters.endDate !== null) {
        conditions.push(`#createdAt <= :endDate`);
        expressionAttributeNames['#createdAt'] = 'createdAt';
        expressionAttributeValues[':endDate'] = filters.endDate;
      }

      // Response status filter
      if (filters.responseStatus && filters.responseStatus !== '' && filters.responseStatus !== null) {
        conditions.push(`#responseStatus = :responseStatus`);
        expressionAttributeNames['#responseStatus'] = 'responseStatus';
        expressionAttributeValues[':responseStatus'] = filters.responseStatus;
      }

      // Source filter
      if (filters.source && filters.source !== '' && filters.source !== null) {
        conditions.push(`#src = :source`);
        expressionAttributeNames['#src'] = 'source';
        expressionAttributeValues[':source'] = filters.source;
      }

      // Lead ID filter
      if (filters.leadId && filters.leadId !== '' && filters.leadId !== null) {
        conditions.push(`#leadId = :leadId`);
        expressionAttributeNames['#leadId'] = 'leadId';
        expressionAttributeValues[':leadId'] = filters.leadId;
      }

      // Only add filter expression if we have valid conditions
      if (conditions.length > 0 && Object.keys(expressionAttributeValues).length > 0) {
        params.FilterExpression = conditions.join(' AND ');
        params.ExpressionAttributeNames = expressionAttributeNames;
        params.ExpressionAttributeValues = expressionAttributeValues;
        
        console.log('Applied filters:', {
          filterExpression: params.FilterExpression,
          attributeNames: params.ExpressionAttributeNames,
          attributeValues: params.ExpressionAttributeValues
        });
      }
    }

    // Scan table with filters
    do {
      // Add pagination key if exists
      if (lastEvaluatedKey) {
        params.ExclusiveStartKey = lastEvaluatedKey;
      }

      console.log('Scanning with params:', JSON.stringify(params, null, 2));

      try {
        const result = await docClient.send(new ScanCommand(params));
        
        const validItems = (result.Items || []).filter(item => item !== null && item !== undefined);
        items = items.concat(validItems);
        
        lastEvaluatedKey = result.LastEvaluatedKey;
        scannedCount += result.Count || 0;
        
        console.log(`Scanned ${scannedCount} items, filtered to ${items.length}...`);
      } catch (scanError) {
        console.error('Scan error:', scanError);
        console.error('Scan params that caused error:', JSON.stringify(params, null, 2));
        throw scanError;
      }

      // Remove ExclusiveStartKey for next iteration
      delete params.ExclusiveStartKey;
      
    } while (lastEvaluatedKey);

    if (items.length === 0) {
      return res.status(404).json({ 
        error: 'No data found matching the filters',
        scannedCount 
      });
    }

    console.log(`Total items to flatten: ${items.length}`);

    // Flatten all items with error handling
    const flattenedData = [];
    for (let i = 0; i < items.length; i++) {
      try {
        const item = items[i];
        if (item && typeof item === 'object') {
          const flattened = flattenObject(item);
          flattenedData.push(flattened);
        }
      } catch (flattenError) {
        console.error(`Error flattening item ${i}:`, flattenError);
      }
    }

    if (flattenedData.length === 0) {
      return res.status(500).json({ 
        error: 'Failed to process any items',
        totalItems: items.length
      });
    }

    console.log(`Successfully flattened ${flattenedData.length} items`);

    // Convert to CSV
    try {
      const parser = new Parser();
      const csv = parser.parse(flattenedData);

      res.header('Content-Type', 'text/csv');
      res.header('Content-Disposition', `attachment; filename="${tableName}_export_${new Date().toISOString().split('T')[0]}.csv"`);
      res.send(csv);
    } catch (csvError) {
      console.error('CSV parsing error:', csvError);
      return res.status(500).json({ 
        error: 'Failed to generate CSV',
        details: csvError.message
      });
    }
  } catch (error) {
    console.error('Export error:', error);
    res.status(500).json({ 
      error: error.message,
      stack: process.env.NODE_ENV === 'development' ? error.stack : undefined
    });
  }
});

module.exports = router;
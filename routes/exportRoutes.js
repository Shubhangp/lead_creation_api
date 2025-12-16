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

// Flatten object recursively with better null/undefined handling
function flattenObject(obj, prefix = '', maxDepth = 10, currentDepth = 0) {
  // Handle null or undefined at the root
  if (obj === null || obj === undefined) {
    return { [prefix || 'value']: '' };
  }

  // Handle max depth
  if (currentDepth >= maxDepth) {
    return { [prefix]: JSON.stringify(obj) };
  }

  const result = {};
  
  try {
    // Handle primitive types
    if (typeof obj !== 'object' || obj === null) {
      result[prefix || 'value'] = obj === null ? '' : obj;
      return result;
    }

    // Handle arrays
    if (Array.isArray(obj)) {
      result[prefix] = JSON.stringify(obj);
      return result;
    }

    // Handle objects
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
    console.error('Error flattening object:', error, 'Object:', obj);
    result[prefix || 'error'] = `Error: ${error.message}`;
  }
  
  return result;
}

// Export table with filters
router.post('/export', async (req, res) => {
  try {
    const { tableName, filters } = req.body;
    
    console.log('Export request:', { tableName, filters });
    
    if (!tableName) {
      return res.status(400).json({ error: 'Table name is required' });
    }

    let items = [];
    let lastEvaluatedKey = null;
    let scannedCount = 0;

    // Build filter expression
    let filterExpression = '';
    const expressionAttributeNames = {};
    const expressionAttributeValues = {};

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

      try {
        const result = await docClient.send(new ScanCommand(params));
        
        // Filter out null/undefined items
        const validItems = (result.Items || []).filter(item => item !== null && item !== undefined);
        items = items.concat(validItems);
        
        lastEvaluatedKey = result.LastEvaluatedKey;
        scannedCount += result.Count || 0;
        
        console.log(`Scanned ${scannedCount} items, filtered to ${items.length}...`);
      } catch (scanError) {
        console.error('Scan error:', scanError);
        throw scanError;
      }
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
        } else {
          console.warn(`Skipping invalid item at index ${i}:`, item);
        }
      } catch (flattenError) {
        console.error(`Error flattening item ${i}:`, flattenError, items[i]);
        // Add error item to show which record failed
        flattenedData.push({
          error: `Failed to flatten item ${i}`,
          errorMessage: flattenError.message
        });
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
        details: csvError.message,
        sampleData: flattenedData.slice(0, 2)
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
// models/MpokketResponseLog.js
const { docClient } = require('../dynamodb');
const { PutCommand, GetCommand, QueryCommand, ScanCommand } = require('@aws-sdk/lib-dynamodb');
const { v4: uuidv4 } = require('uuid');

const TABLE_NAME = 'mpokket_response_logs';

class MpokketResponseLog {
  // Create log entry
  static async create(logData) {
    const item = {
      logId: uuidv4(),
      leadId: logData.leadId,
      source: logData.source || null,
      correlationId: logData.correlationId || null,
      requestPayload: logData.requestPayload || null,
      responseStatus: logData.responseStatus || null,
      responseBody: logData.responseBody || null,
      errorDetails: logData.errorDetails || null,
      step: logData.step || null,
      status: logData.status || null,
      createdAt: new Date().toISOString()
    };

    await docClient.send(new PutCommand({
      TableName: TABLE_NAME,
      Item: item
    }));

    return item;
  }

  // Find by ID
  static async findById(logId) {
    const result = await docClient.send(new GetCommand({
      TableName: TABLE_NAME,
      Key: { logId }
    }));

    return result.Item || null;
  }

  // Find by leadId
  static async findByLeadId(leadId) {
    const result = await docClient.send(new QueryCommand({
      TableName: TABLE_NAME,
      IndexName: 'leadId-index',
      KeyConditionExpression: 'leadId = :leadId',
      ExpressionAttributeValues: { ':leadId': leadId }
    }));

    return result.Items || [];
  }

  // Find by source with date range
  static async findBySource(source, options = {}) {
    const { limit = 100, startDate, endDate, sortAscending = false } = options;

    let keyConditionExpression = 'source = :source';
    const expressionAttributeValues = { ':source': source };

    if (startDate && endDate) {
      keyConditionExpression += ' AND createdAt BETWEEN :startDate AND :endDate';
      expressionAttributeValues[':startDate'] = startDate;
      expressionAttributeValues[':endDate'] = endDate;
    } else if (startDate) {
      keyConditionExpression += ' AND createdAt >= :startDate';
      expressionAttributeValues[':startDate'] = startDate;
    } else if (endDate) {
      keyConditionExpression += ' AND createdAt <= :endDate';
      expressionAttributeValues[':endDate'] = endDate;
    }

    const result = await docClient.send(new QueryCommand({
      TableName: TABLE_NAME,
      IndexName: 'source-createdAt-index',
      KeyConditionExpression: keyConditionExpression,
      ExpressionAttributeValues: expressionAttributeValues,
      ScanIndexForward: sortAscending,
      Limit: limit
    }));

    return result.Items || [];
  }

  // Find by status
  static async findByStatus(status, options = {}) {
    const { limit = 100, startDate, endDate, sortAscending = false } = options;

    let keyConditionExpression = 'status = :status';
    const expressionAttributeValues = { ':status': status };

    if (startDate && endDate) {
      keyConditionExpression += ' AND createdAt BETWEEN :startDate AND :endDate';
      expressionAttributeValues[':startDate'] = startDate;
      expressionAttributeValues[':endDate'] = endDate;
    } else if (startDate) {
      keyConditionExpression += ' AND createdAt >= :startDate';
      expressionAttributeValues[':startDate'] = startDate;
    } else if (endDate) {
      keyConditionExpression += ' AND createdAt <= :endDate';
      expressionAttributeValues[':endDate'] = endDate;
    }

    const result = await docClient.send(new QueryCommand({
      TableName: TABLE_NAME,
      IndexName: 'status-index',
      KeyConditionExpression: keyConditionExpression,
      ExpressionAttributeValues: expressionAttributeValues,
      ScanIndexForward: sortAscending,
      Limit: limit
    }));

    return result.Items || [];
  }

  // Find all logs (paginated)
  static async findAll(options = {}) {
    const { limit = 100, lastEvaluatedKey } = options;

    const params = {
      TableName: TABLE_NAME,
      Limit: limit
    };

    if (lastEvaluatedKey) {
      params.ExclusiveStartKey = lastEvaluatedKey;
    }

    const result = await docClient.send(new ScanCommand(params));

    return {
      items: result.Items || [],
      lastEvaluatedKey: result.LastEvaluatedKey
    };
  }

  // Get logs by date range
  static async findByDateRange(startDate, endDate, options = {}) {
    const { limit = 100, lastEvaluatedKey } = options;
    
    const params = {
      TableName: TABLE_NAME,
      FilterExpression: 'createdAt BETWEEN :startDate AND :endDate',
      ExpressionAttributeValues: {
        ':startDate': startDate,
        ':endDate': endDate
      },
      Limit: limit
    };

    if (lastEvaluatedKey) {
      params.ExclusiveStartKey = lastEvaluatedKey;
    }

    const result = await docClient.send(new ScanCommand(params));
    return {
      items: result.Items || [],
      lastEvaluatedKey: result.LastEvaluatedKey
    };
  }

  // ============================================================================
  // STATS FUNCTIONS
  // ============================================================================

  // Get quick stats with optional date range
  static async getQuickStats(source = null, startDate = null, endDate = null) {
    try {
      if (source && !startDate) {
        // Get count for specific source using GSI
        const result = await docClient.send(new QueryCommand({
          TableName: TABLE_NAME,
          IndexName: 'source-createdAt-index',
          KeyConditionExpression: 'source = :source',
          ExpressionAttributeValues: { ':source': source },
          Select: 'COUNT'
        }));

        return {
          totalLogs: result.Count || 0,
          source: source,
          isEstimate: false
        };
      } else if (startDate && endDate) {
        // Quick count for date range
        return this.getQuickStatsForDateRange(startDate, endDate);
      } else {
        // Use parallel scan for total count
        return this.getQuickStatsParallel();
      }
    } catch (error) {
      console.error('Error in getQuickStats:', error);
      throw error;
    }
  }

  // Quick count for date range (COUNT only, no data fetching)
  static async getQuickStatsForDateRange(startDate, endDate) {
    const segments = 8;
    const startTime = Date.now();

    try {
      console.log(`[${TABLE_NAME}] Quick count for date range:`, startDate, 'to', endDate);

      const countPromises = [];
      for (let segment = 0; segment < segments; segment++) {
        countPromises.push(this._countSegmentInRange(segment, segments, startDate, endDate));
      }

      const results = await Promise.all(countPromises);
      const totalCount = results.reduce((sum, result) => sum + result.count, 0);
      const elapsed = Date.now() - startTime;

      console.log(`[${TABLE_NAME}] Quick count complete: ${totalCount} items in ${elapsed}ms`);

      return {
        totalLogs: totalCount,
        isEstimate: false,
        scannedInMs: elapsed,
        method: 'parallel-count',
        dateRange: { start: startDate, end: endDate }
      };
    } catch (error) {
      console.error('Error in quick count:', error);
      throw error;
    }
  }

  // Helper: Count items in segment for date range (COUNT only)
  static async _countSegmentInRange(segment, totalSegments, startDate, endDate) {
    let count = 0;
    let lastKey = null;

    do {
      const params = {
        TableName: TABLE_NAME,
        Select: 'COUNT',
        Segment: segment,
        TotalSegments: totalSegments,
        FilterExpression: 'createdAt BETWEEN :startDate AND :endDate',
        ExpressionAttributeValues: {
          ':startDate': startDate,
          ':endDate': endDate
        }
      };

      if (lastKey) {
        params.ExclusiveStartKey = lastKey;
      }

      const result = await docClient.send(new ScanCommand(params));
      count += result.Count || 0;
      lastKey = result.LastEvaluatedKey;
    } while (lastKey);

    console.log(`Count segment ${segment}/${totalSegments}: ${count} items`);
    return { segment, count };
  }

  // Parallel scan for total count
  static async getQuickStatsParallel() {
    const segments = 8;
    const startTime = Date.now();

    try {
      console.log(`Starting parallel scan with ${segments} segments for ${TABLE_NAME}`);

      const scanPromises = [];
      for (let segment = 0; segment < segments; segment++) {
        scanPromises.push(this._scanSegment(segment, segments));
      }

      const results = await Promise.all(scanPromises);
      const totalCount = results.reduce((sum, result) => sum + result.count, 0);
      const elapsed = Date.now() - startTime;

      console.log(`Parallel scan complete: ${totalCount} items in ${elapsed}ms`);

      return {
        totalLogs: totalCount,
        isEstimate: false,
        scannedInMs: elapsed,
        method: 'parallel'
      };
    } catch (error) {
      console.error('Error in parallel scan:', error);
      throw error;
    }
  }

  // Helper method for parallel scanning
  static async _scanSegment(segment, totalSegments) {
    let count = 0;
    let lastKey = null;

    do {
      const params = {
        TableName: TABLE_NAME,
        Select: 'COUNT',
        Segment: segment,
        TotalSegments: totalSegments
      };

      if (lastKey) {
        params.ExclusiveStartKey = lastKey;
      }

      const result = await docClient.send(new ScanCommand(params));
      count += result.Count || 0;
      lastKey = result.LastEvaluatedKey;
    } while (lastKey);

    console.log(`Segment ${segment}/${totalSegments} complete: ${count} items`);
    return { segment, count };
  }

  // Get comprehensive stats (OPTIMIZED for date ranges)
  static async getStats(startDate = null, endDate = null) {
    let allItems = [];
    const startTime = Date.now();

    console.log(`[${TABLE_NAME}] Starting stats fetch for date range:`, startDate, 'to', endDate);

    try {
      // Use parallel scan with date filter for much faster results
      if (startDate && endDate) {
        const segments = 8;
        const scanPromises = [];

        for (let segment = 0; segment < segments; segment++) {
          scanPromises.push(this._scanSegmentWithFilter(segment, segments, startDate, endDate));
        }

        const results = await Promise.all(scanPromises);
        allItems = results.flat();

        console.log(`[${TABLE_NAME}] Parallel filtered scan complete: ${allItems.length} items in ${Date.now() - startTime}ms`);
      } else {
        // No date filter - regular scan
        let lastKey = null;
        do {
          const params = {
            TableName: TABLE_NAME,
            Limit: 1000
          };

          if (lastKey) {
            params.ExclusiveStartKey = lastKey;
          }

          const result = await docClient.send(new ScanCommand(params));
          allItems = allItems.concat(result.Items || []);
          lastKey = result.LastEvaluatedKey;
        } while (lastKey);
      }

      // Initialize stats
      const stats = {
        totalLogs: allItems.length,
        dateRange: {
          start: startDate,
          end: endDate
        },
        responseStatusBreakdown: {},
        sourceBreakdown: {},
        statusBreakdown: {}, // Internal status field
        stepBreakdown: {},
        statusCategoryBreakdown: {
          '200': 0,
          '400': 0,
          '403': 0,
          'other': 0
        },
        successRate: 0
      };

      // Process each log
      allItems.forEach(item => {
        // Response status breakdown (200, 400, 403, etc.)
        const responseStatus = item.responseStatus || 'unknown';
        stats.responseStatusBreakdown[responseStatus] = (stats.responseStatusBreakdown[responseStatus] || 0) + 1;

        // Source breakdown
        const source = item.source || 'unknown';
        stats.sourceBreakdown[source] = (stats.sourceBreakdown[source] || 0) + 1;

        // Internal status breakdown (if exists)
        if (item.status) {
          stats.statusBreakdown[item.status] = (stats.statusBreakdown[item.status] || 0) + 1;
        }

        // Step breakdown (if exists)
        if (item.step) {
          stats.stepBreakdown[item.step] = (stats.stepBreakdown[item.step] || 0) + 1;
        }

        // Status category breakdown (Mpokket specific: 200, 400, 403)
        const statusStr = String(responseStatus);
        if (statusStr === '200') {
          stats.statusCategoryBreakdown['200']++;
        } else if (statusStr === '400') {
          stats.statusCategoryBreakdown['400']++;
        } else if (statusStr === '403') {
          stats.statusCategoryBreakdown['403']++;
        } else {
          stats.statusCategoryBreakdown['other']++;
        }
      });

      // Calculate success rate (200 status)
      const successCount = stats.statusCategoryBreakdown['200'];
      stats.successRate = allItems.length > 0 
        ? ((successCount / allItems.length) * 100).toFixed(2) + '%'
        : '0%';

      const elapsed = Date.now() - startTime;
      console.log(`[${TABLE_NAME}] Stats processing complete in ${elapsed}ms`);
      stats.processingTimeMs = elapsed;

      return stats;
    } catch (error) {
      console.error('Error in getStats:', error);
      throw error;
    }
  }

  // Helper: Parallel scan with date filter
  static async _scanSegmentWithFilter(segment, totalSegments, startDate, endDate) {
    let items = [];
    let lastKey = null;

    do {
      const params = {
        TableName: TABLE_NAME,
        Segment: segment,
        TotalSegments: totalSegments,
        FilterExpression: 'createdAt BETWEEN :startDate AND :endDate',
        ExpressionAttributeValues: {
          ':startDate': startDate,
          ':endDate': endDate
        }
      };

      if (lastKey) {
        params.ExclusiveStartKey = lastKey;
      }

      const result = await docClient.send(new ScanCommand(params));
      items = items.concat(result.Items || []);
      lastKey = result.LastEvaluatedKey;
    } while (lastKey);

    console.log(`Segment ${segment}/${totalSegments} complete: ${items.length} items`);
    return items;
  }

  // Get stats grouped by date (OPTIMIZED)
  static async getStatsByDate(startDate, endDate) {
    const startTime = Date.now();
    console.log(`[${TABLE_NAME}] Fetching stats by date:`, startDate, 'to', endDate);

    try {
      // Use parallel scan
      const segments = 8;
      const scanPromises = [];

      for (let segment = 0; segment < segments; segment++) {
        scanPromises.push(this._scanSegmentWithFilter(segment, segments, startDate, endDate));
      }

      const results = await Promise.all(scanPromises);
      const allItems = results.flat();

      console.log(`[${TABLE_NAME}] Fetched ${allItems.length} items in ${Date.now() - startTime}ms`);

      // Group by date
      const statsByDate = {};

      allItems.forEach(item => {
        const date = item.createdAt.split('T')[0];
        
        if (!statsByDate[date]) {
          statsByDate[date] = {
            date,
            total: 0,
            statusBreakdown: {},
            statusCategories: {
              '200': 0,
              '400': 0,
              '403': 0,
              'other': 0
            }
          };
        }

        statsByDate[date].total++;

        // Response status breakdown
        const responseStatus = item.responseStatus || 'unknown';
        statsByDate[date].statusBreakdown[responseStatus] = 
          (statsByDate[date].statusBreakdown[responseStatus] || 0) + 1;

        // Status categories
        const statusStr = String(responseStatus);
        if (statusStr === '200') {
          statsByDate[date].statusCategories['200']++;
        } else if (statusStr === '400') {
          statsByDate[date].statusCategories['400']++;
        } else if (statusStr === '403') {
          statsByDate[date].statusCategories['403']++;
        } else {
          statsByDate[date].statusCategories['other']++;
        }
      });

      return Object.values(statsByDate).sort((a, b) => 
        a.date.localeCompare(b.date)
      );
    } catch (error) {
      console.error('Error in getStatsByDate:', error);
      throw error;
    }
  }
}

module.exports = MpokketResponseLog;
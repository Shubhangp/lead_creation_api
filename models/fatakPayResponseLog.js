const { docClient } = require('../dynamodb');
const { PutCommand, GetCommand, QueryCommand, ScanCommand } = require('@aws-sdk/lib-dynamodb');
const { v4: uuidv4 } = require('uuid');

const TABLE_NAME = 'fatakpay_response_logs';

// Response message categories
const PERMANENT_BLOCKS = [
  'application is permanently block.',
  'Attribute Error',
  'Internal Server Error',
  'loan Application already exists for this pan number.',
  'Loan application already exists.',
  'User already exists in the system.',
  'Unknown error',
  'You are eligible.',
  'you are not eligible due to age criteria',
  'You are not eligible.',
  'This PAN is already in use.'
];

class FatakPayResponseLog {
  // Create log entry
  static async create(logData) {
    const item = {
      logId: uuidv4(),
      leadId: logData.leadId,
      source: logData.source || null,
      requestPayload: logData.requestPayload || null,
      responseStatus: logData.responseStatus || null,
      responseBody: logData.responseBody || null,
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

  // Find by leadId (requires GSI: leadId-index)
  static async findByLeadId(leadId) {
    const result = await docClient.send(new QueryCommand({
      TableName: TABLE_NAME,
      IndexName: 'leadId-index',
      KeyConditionExpression: 'leadId = :leadId',
      ExpressionAttributeValues: { ':leadId': leadId }
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

  // Get logs by date range (requires GSI: createdAt-index)
  static async findByDateRange(startDate, endDate, options = {}) {
    const { limit = 100, lastEvaluatedKey } = options;
    
    const params = {
      TableName: TABLE_NAME,
      IndexName: 'createdAt-index',
      KeyConditionExpression: 'createdAt BETWEEN :startDate AND :endDate',
      ExpressionAttributeValues: {
        ':startDate': startDate,
        ':endDate': endDate
      },
      Limit: limit
    };

    if (lastEvaluatedKey) {
      params.ExclusiveStartKey = lastEvaluatedKey;
    }

    const result = await docClient.send(new QueryCommand(params));
    return {
      items: result.Items || [],
      lastEvaluatedKey: result.LastEvaluatedKey
    };
  }

  // Helper function to check if response is a permanent block
  static isPermanentBlock(responseBody) {
    if (!responseBody || !responseBody.message) return false;
    
    const message = responseBody.message.trim();
    return PERMANENT_BLOCKS.some(blockMsg => 
      message.toLowerCase() === blockMsg.toLowerCase()
    );
  }

  // Get permanent block logs
  static async getPermanentBlocks(options = {}) {
    const { limit = 1000 } = options;
    let allItems = [];
    let lastKey = null;

    do {
      const params = {
        TableName: TABLE_NAME,
        Limit: limit
      };

      if (lastKey) {
        params.ExclusiveStartKey = lastKey;
      }

      const result = await docClient.send(new ScanCommand(params));
      const items = result.Items || [];
      
      // Filter permanent blocks
      const permanentBlocks = items.filter(item => 
        this.isPermanentBlock(item.responseBody)
      );
      
      allItems = allItems.concat(permanentBlocks);
      lastKey = result.LastEvaluatedKey;
    } while (lastKey && allItems.length < limit);

    return allItems;
  }

  // Get comprehensive stats (OPTIMIZED for date ranges)
  static async getStats(startDate = null, endDate = null) {
    let allItems = [];
    const startTime = Date.now();

    console.log(`[${TABLE_NAME}] Starting stats fetch for date range:`, startDate, 'to', endDate);

    try {
      // Use parallel scan with date filter for much faster results
      if (startDate && endDate) {
        const segments = 8; // Increased from 4 to 8 for faster processing
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
        messageBreakdown: {},
        permanentBlocks: {
          total: 0,
          byMessage: {},
          percentage: '0%'
        },
        sourceBreakdown: {},
        successRate: 0,
        eligibilityStats: {
          eligible: 0,
          notEligible: 0,
          ageNotEligible: 0
        }
      };

      // Process each log
      allItems.forEach(item => {
        // Response status breakdown
        const status = item.responseStatus || 'unknown';
        stats.responseStatusBreakdown[status] = (stats.responseStatusBreakdown[status] || 0) + 1;

        // Source breakdown
        const source = item.source || 'unknown';
        stats.sourceBreakdown[source] = (stats.sourceBreakdown[source] || 0) + 1;

        // Message breakdown and permanent blocks
        if (item.responseBody && item.responseBody.message) {
          const message = item.responseBody.message.trim();
          stats.messageBreakdown[message] = (stats.messageBreakdown[message] || 0) + 1;

          // Check if permanent block
          if (this.isPermanentBlock(item.responseBody)) {
            stats.permanentBlocks.total++;
            stats.permanentBlocks.byMessage[message] = 
              (stats.permanentBlocks.byMessage[message] || 0) + 1;
          }

          // Eligibility stats
          const lowerMessage = message.toLowerCase();
          if (lowerMessage.includes('you are eligible')) {
            stats.eligibilityStats.eligible++;
          } else if (lowerMessage.includes('not eligible due to age')) {
            stats.eligibilityStats.ageNotEligible++;
          } else if (lowerMessage.includes('not eligible')) {
            stats.eligibilityStats.notEligible++;
          }
        }
      });

      // Calculate success rate
      const successCount = stats.responseStatusBreakdown['200'] || 0;
      stats.successRate = allItems.length > 0 
        ? ((successCount / allItems.length) * 100).toFixed(2) + '%'
        : '0%';

      // Calculate permanent block percentage
      stats.permanentBlocks.percentage = allItems.length > 0
        ? ((stats.permanentBlocks.total / allItems.length) * 100).toFixed(2) + '%'
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
      const segments = 4;
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
            messageBreakdown: {},
            permanentBlocks: 0,
            eligible: 0,
            notEligible: 0
          };
        }

        statsByDate[date].total++;

        // Status
        const status = item.responseStatus || 'unknown';
        statsByDate[date].statusBreakdown[status] = 
          (statsByDate[date].statusBreakdown[status] || 0) + 1;

        // Message
        if (item.responseBody && item.responseBody.message) {
          const message = item.responseBody.message.trim();
          statsByDate[date].messageBreakdown[message] = 
            (statsByDate[date].messageBreakdown[message] || 0) + 1;

          // Permanent blocks
          if (this.isPermanentBlock(item.responseBody)) {
            statsByDate[date].permanentBlocks++;
          }

          // Eligibility
          const lowerMessage = message.toLowerCase();
          if (lowerMessage.includes('you are eligible')) {
            statsByDate[date].eligible++;
          } else if (lowerMessage.includes('not eligible')) {
            statsByDate[date].notEligible++;
          }
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

  // Get quick stats with optional date range
  static async getQuickStats(source = null, startDate = null, endDate = null) {
    try {
      if (source && !startDate) {
        // Get count for specific source
        let totalCount = 0;
        let lastKey = null;

        do {
          const params = {
            TableName: TABLE_NAME,
            FilterExpression: 'source = :source',
            ExpressionAttributeValues: { ':source': source },
            Select: 'COUNT'
          };

          if (lastKey) {
            params.ExclusiveStartKey = lastKey;
          }

          const result = await docClient.send(new ScanCommand(params));
          totalCount += result.Count || 0;
          lastKey = result.LastEvaluatedKey;
        } while (lastKey);

        return {
          totalLogs: totalCount,
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

  // Parallel scan - 8x faster than sequential scan
  static async getQuickStatsParallel() {
    const segments = 8; // Use 8 parallel scans
    const startTime = Date.now();

    try {
      console.log(`Starting parallel scan with ${segments} segments for ${TABLE_NAME}`);

      // Create parallel scan promises
      const scanPromises = [];
      for (let segment = 0; segment < segments; segment++) {
        scanPromises.push(this._scanSegment(segment, segments));
      }

      // Wait for all segments to complete
      const results = await Promise.all(scanPromises);
      
      // Sum up all counts
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
}

module.exports = FatakPayResponseLog;
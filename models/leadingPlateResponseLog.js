const { docClient } = require('../dynamodb');
const { PutCommand, GetCommand, QueryCommand, UpdateCommand } = require('@aws-sdk/lib-dynamodb');
const { v4: uuidv4 } = require('uuid');

const TABLE_NAME = 'lending_plate_response_logs';

class LendingPlateResponseLog {

  // ─── Create log entry ──────────────────────────────────────────────────────

  static async create(logData) {
    const item = {
      logId:          uuidv4(),
      leadId:         logData.leadId,
      source:         logData.source || null,
      requestPayload: logData.requestPayload || null,
      responseStatus: logData.responseStatus || null,
      responseBody:   logData.responseBody   || null,
      createdAt:      new Date().toISOString()
    };

    await docClient.send(new PutCommand({ TableName: TABLE_NAME, Item: item }));
    return item;
  }

  // ─── Find by ID ────────────────────────────────────────────────────────────

  static async findById(logId) {
    const result = await docClient.send(new GetCommand({
      TableName: TABLE_NAME,
      Key: { logId }
    }));
    return result.Item || null;
  }

  // ─── Find by leadId  (uses leadId-index ACTIVE ✅) ──────────────────────────

  static async findByLeadId(leadId) {
    const result = await docClient.send(new QueryCommand({
      TableName: TABLE_NAME,
      IndexName: 'leadId-index',
      KeyConditionExpression: 'leadId = :leadId',
      ExpressionAttributeValues: { ':leadId': leadId }
    }));
    return result.Items || [];
  }

  // ─── getQuickStats WITH multi-source support ───────────────────────────────

  static async getQuickStats(source = null, startDate = null, endDate = null) {
    const startTime = Date.now();

    try {
      // ✅ If no source, get all known sources
      if (!source) {
        const sources = ['CashKuber', 'FREO', 'BatterySmart', 'Ratecut', 'VFC'];
        console.log(`[${TABLE_NAME}] Counting all sources:`, sources);

        const countPromises = sources.map(async (src) => {
          const count = await this._countSource(src, startDate, endDate);
          return { source: src, count };
        });

        const results = await Promise.all(countPromises);
        
        const sourceBreakdown = {};
        let totalCount = 0;
        
        results.forEach(({ source: src, count }) => {
          sourceBreakdown[src] = count;
          totalCount += count;
          console.log(`  Source "${src}": ${count.toLocaleString()} records`);
        });

        const elapsed = Date.now() - startTime;
        console.log(`[${TABLE_NAME}] ✅ Total: ${totalCount.toLocaleString()} in ${elapsed}ms`);

        return {
          totalLogs: totalCount,
          sourceBreakdown,
          sources,
          dateRange: startDate && endDate ? { start: startDate, end: endDate } : null,
          scannedInMs: elapsed,
          method: 'query-count-all-sources',
          indexUsed: 'source-createdAt-index'
        };
      }

      // ✅ Single source
      const count = await this._countSource(source, startDate, endDate);
      const elapsed = Date.now() - startTime;

      return {
        totalLogs: count,
        source,
        sourceBreakdown: { [source]: count },
        dateRange: startDate && endDate ? { start: startDate, end: endDate } : null,
        scannedInMs: elapsed,
        method: 'query-count',
        indexUsed: 'source-createdAt-index'
      };
    } catch (error) {
      console.error('Error in getQuickStats:', error);
      throw error;
    }
  }

  // ─── Helper: Count a single source ────────────────────────────────────────

  static async _countSource(source, startDate = null, endDate = null) {
    const params = {
      TableName: TABLE_NAME,
      IndexName: 'source-createdAt-index',
      KeyConditionExpression: '#source = :source',
      ExpressionAttributeNames: { '#source': 'source' },
      ExpressionAttributeValues: { ':source': source },
      Select: 'COUNT'
    };

    if (startDate && endDate) {
      params.KeyConditionExpression += ' AND #createdAt BETWEEN :startDate AND :endDate';
      params.ExpressionAttributeNames['#createdAt'] = 'createdAt';
      params.ExpressionAttributeValues[':startDate'] = startDate;
      params.ExpressionAttributeValues[':endDate'] = endDate;
    }

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

  // ─── Helper: Fetch items for a single source ──────────────────────────────

  static async _fetchItemsBySource(source, startDate, endDate) {
    let allItems = [];
    let lastKey = null;

    const params = {
      TableName: TABLE_NAME,
      IndexName: 'source-createdAt-index',
      KeyConditionExpression: '#source = :source',
      ExpressionAttributeNames: { '#source': 'source' },
      ExpressionAttributeValues: { ':source': source },
      ScanIndexForward: false
    };

    if (startDate && endDate) {
      params.KeyConditionExpression += ' AND #createdAt BETWEEN :startDate AND :endDate';
      params.ExpressionAttributeNames['#createdAt'] = 'createdAt';
      params.ExpressionAttributeValues[':startDate'] = startDate;
      params.ExpressionAttributeValues[':endDate'] = endDate;
    }

    do {
      if (lastKey) params.ExclusiveStartKey = lastKey;
      const result = await docClient.send(new QueryCommand(params));
      allItems = allItems.concat(result.Items || []);
      lastKey = result.LastEvaluatedKey;
      delete params.ExclusiveStartKey;
    } while (lastKey);

    console.log(`  ✅ ${source}: ${allItems.length} items`);
    return allItems;
  }

  // ─── getStats WITH multi-source and source-wise breakdown ──────────────────

  static async getStats(source = null, startDate = null, endDate = null) {
    const startTime = Date.now();

    try {
      // ✅ If no source, get stats for all sources
      if (!source) {
        const sources = ['CashKuber', 'FREO', 'BatterySmart', 'Ratecut', 'VFC'];
        console.log(`[${TABLE_NAME}] Fetching stats for all sources:`, sources);

        let allItems = [];
        for (const src of sources) {
          const items = await this._fetchItemsBySource(src, startDate, endDate);
          allItems = allItems.concat(items);
        }

        console.log(`✅ Query complete: ${allItems.length} items from ${sources.length} sources`);

        const stats = this._calculateStatsWithSourceBreakdown(allItems, null, startDate, endDate);
        stats.processingTimeMs = Date.now() - startTime;
        stats.method = 'query-all-sources';
        stats.indexUsed = 'source-createdAt-index';

        return stats;
      }

      // ✅ Single source
      console.log(`[${TABLE_NAME}] Fetching stats for source: ${source}`);
      const allItems = await this._fetchItemsBySource(source, startDate, endDate);

      const stats = this._calculateStatsWithSourceBreakdown(allItems, source, startDate, endDate);
      stats.processingTimeMs = Date.now() - startTime;
      stats.method = 'query';
      stats.indexUsed = 'source-createdAt-index';

      return stats;
    } catch (error) {
      console.error('Error in getStats:', error);
      throw error;
    }
  }

  // ─── _calculateStatsWithSourceBreakdown ────────────────────────────────────

  static _calculateStatsWithSourceBreakdown(items, source, startDate, endDate) {
    const stats = {
      totalLogs: items.length,
      source,
      dateRange: { start: startDate, end: endDate },
      responseStatusBreakdown: {},
      sourceBreakdown: {},
      statusCategoryBreakdown: {
        'Success': 0,
        'Fail': 0,
        'null': 0,
        'other': 0
      },
      sourceWiseStats: {},
      successRateBySource: {},
      lpStatusBreakdown: {},
      disbursedCount: 0,
      sanctionedCount: 0,
      rejectedCount: 0,
      successRate: '0%'
    };

    items.forEach(item => {
      // Determine the actual status from responseBody
      const actualStatus = this._extractStatus(item);
      
      stats.responseStatusBreakdown[actualStatus] = 
        (stats.responseStatusBreakdown[actualStatus] || 0) + 1;

      const src = item.source || 'unknown';
      stats.sourceBreakdown[src] = (stats.sourceBreakdown[src] || 0) + 1;

      // ✅ Initialize source-wise stats
      if (!stats.sourceWiseStats[src]) {
        stats.sourceWiseStats[src] = {
          totalLogs: 0,
          success: 0,
          fail: 0,
          error: 0,
          null: 0,
          other: 0,
          disbursed: 0,
          sanctioned: 0,
          rejected: 0
        };
      }

      stats.sourceWiseStats[src].totalLogs++;

      // Categorize status
      if (actualStatus === 'Success') {
        stats.statusCategoryBreakdown['Success']++;
        stats.sourceWiseStats[src].success++;
      } else if (actualStatus === 'Fail') {
        stats.statusCategoryBreakdown['Fail']++;
        stats.sourceWiseStats[src].fail++;
      } else if (actualStatus === 'Error') {
        stats.statusCategoryBreakdown['other']++;
        stats.sourceWiseStats[src].error++;
      } else if (actualStatus === 'null' || !actualStatus) {
        stats.statusCategoryBreakdown['null']++;
        stats.sourceWiseStats[src].null++;
      } else {
        stats.statusCategoryBreakdown['other']++;
        stats.sourceWiseStats[src].other++;
      }

      // LP Status tracking
      if (item.lpStatus) {
        stats.lpStatusBreakdown[item.lpStatus] = 
          (stats.lpStatusBreakdown[item.lpStatus] || 0) + 1;
        
        if (item.lpStatus === 'DISBURSED') {
          stats.disbursedCount++;
          stats.sourceWiseStats[src].disbursed++;
        } else if (item.lpStatus === 'SANCTION' || item.lpStatus === 'SANCTION-ACCEPTED') {
          stats.sanctionedCount++;
          stats.sourceWiseStats[src].sanctioned++;
        } else if (item.lpStatus === 'REJECT') {
          stats.rejectedCount++;
          stats.sourceWiseStats[src].rejected++;
        }
      }
    });

    // Calculate overall success rate
    const successCount = stats.statusCategoryBreakdown['Success'];
    stats.successRate = items.length > 0
      ? ((successCount / items.length) * 100).toFixed(2) + '%'
      : '0%';

    // Calculate success rate per source
    Object.keys(stats.sourceWiseStats).forEach(src => {
      const srcStats = stats.sourceWiseStats[src];
      srcStats.successRate = srcStats.totalLogs > 0
        ? ((srcStats.success / srcStats.totalLogs) * 100).toFixed(2) + '%'
        : '0%';
      stats.successRateBySource[src] = srcStats.successRate;
    });

    return stats;
  }

  // ─── Helper: Extract actual status from responseBody ──────────────────────

  static _extractStatus(item) {
    // First check responseStatus
    if (item.responseStatus) {
      return item.responseStatus;
    }

    // Parse responseBody to extract status
    if (!item.responseBody) return 'null';

    try {
      let body = item.responseBody;
      
      // If responseBody is a string, try to parse it
      if (typeof body === 'string') {
        body = JSON.parse(body);
      }

      // Handle DynamoDB format: { "Status": { "S": "Success" } }
      if (body.Status && body.Status.S) {
        return body.Status.S; // "Success" or "Fail"
      }

      // Handle normal format: { "Status": "Success" }
      if (body.Status) {
        return body.Status;
      }

      // Handle error format: { "status": { "S": "Error" } }
      if (body.status && body.status.S) {
        return body.status.S; // "Error"
      }

      // Handle normal error format: { "status": "Error" }
      if (body.status) {
        return body.status;
      }

      return 'null';
    } catch (error) {
      console.error('Error parsing responseBody:', error);
      return 'null';
    }
  }

  // ─── getStatsByDate WITH multi-source support ──────────────────────────────

  static async getStatsByDate(sourceOrStartDate, startDateOrEndDate, endDate) {
    const startTime = Date.now();

    try {
      // Detect call pattern
      let source, startDate, actualEndDate;
      
      if (endDate) {
        // Called as: getStatsByDate(source, startDate, endDate)
        source = sourceOrStartDate;
        startDate = startDateOrEndDate;
        actualEndDate = endDate;
      } else {
        // Called as: getStatsByDate(startDate, endDate) - from controller
        source = null;
        startDate = sourceOrStartDate;
        actualEndDate = startDateOrEndDate;
      }

      console.log(`[${TABLE_NAME}] getStatsByDate: source=${source}, start=${startDate}, end=${actualEndDate}`);

      // ✅ If no source, get stats for all sources
      if (!source) {
        const sources = ['CashKuber', 'FREO', 'BatterySmart', 'Ratecut', 'VFC'];
        console.log(`[${TABLE_NAME}] Fetching stats by date for all sources:`, sources);

        let allItems = [];
        for (const src of sources) {
          const items = await this._fetchItemsBySource(src, startDate, actualEndDate);
          allItems = allItems.concat(items);
        }

        console.log(`✅ Query complete: ${allItems.length} items`);

        const statsByDate = this._groupByDateWithSourceBreakdown(allItems);
        return Object.values(statsByDate).sort((a, b) => a.date.localeCompare(b.date));
      }

      // ✅ Single source
      const allItems = await this._fetchItemsBySource(source, startDate, actualEndDate);
      const statsByDate = this._groupByDate(allItems);
      return Object.values(statsByDate).sort((a, b) => a.date.localeCompare(b.date));
    } catch (error) {
      console.error('Error in getStatsByDate:', error);
      throw error;
    }
  }

  // ─── _groupByDateWithSourceBreakdown ───────────────────────────────────────

  static _groupByDateWithSourceBreakdown(items) {
    const statsByDate = {};

    items.forEach(item => {
      const date = item.createdAt.split('T')[0];
      const itemSource = item.source || 'unknown';
      const actualStatus = this._extractStatus(item);

      if (!statsByDate[date]) {
        statsByDate[date] = {
          date,
          total: 0,
          statusBreakdown: {},
          statusCategories: {
            'Success': 0,
            'Fail': 0,
            'null': 0,
            'other': 0
          },
          sourceBreakdown: {},
          bySource: {},
          lpStatusBreakdown: {}
        };
      }

      statsByDate[date].total++;

      // Overall status tracking
      statsByDate[date].statusBreakdown[actualStatus] =
        (statsByDate[date].statusBreakdown[actualStatus] || 0) + 1;

      if (actualStatus === 'Success') {
        statsByDate[date].statusCategories['Success']++;
      } else if (actualStatus === 'Fail') {
        statsByDate[date].statusCategories['Fail']++;
      } else if (actualStatus === 'Error') {
        statsByDate[date].statusCategories['other']++;
      } else if (!actualStatus || actualStatus === 'null') {
        statsByDate[date].statusCategories['null']++;
      } else {
        statsByDate[date].statusCategories['other']++;
      }

      // ✅ Track by source
      if (!statsByDate[date].bySource[itemSource]) {
        statsByDate[date].bySource[itemSource] = {
          total: 0,
          success: 0,
          fail: 0,
          error: 0,
          null: 0,
          other: 0
        };
      }

      statsByDate[date].sourceBreakdown[itemSource] = 
        (statsByDate[date].sourceBreakdown[itemSource] || 0) + 1;

      statsByDate[date].bySource[itemSource].total++;

      if (actualStatus === 'Success') {
        statsByDate[date].bySource[itemSource].success++;
      } else if (actualStatus === 'Fail') {
        statsByDate[date].bySource[itemSource].fail++;
      } else if (actualStatus === 'Error') {
        statsByDate[date].bySource[itemSource].error++;
      } else if (!actualStatus || actualStatus === 'null') {
        statsByDate[date].bySource[itemSource].null++;
      } else {
        statsByDate[date].bySource[itemSource].other++;
      }

      // LP Status tracking
      if (item.lpStatus) {
        statsByDate[date].lpStatusBreakdown[item.lpStatus] =
          (statsByDate[date].lpStatusBreakdown[item.lpStatus] || 0) + 1;
      }
    });

    return statsByDate;
  }

  // ─── _groupByDate (simple version) ─────────────────────────────────────────

  static _groupByDate(items) {
    const statsByDate = {};

    items.forEach(item => {
      const date = item.createdAt.split('T')[0];
      const actualStatus = this._extractStatus(item);

      if (!statsByDate[date]) {
        statsByDate[date] = {
          date,
          total: 0,
          statusBreakdown: {},
          statusCategories: {
            'Success': 0,
            'Fail': 0,
            'null': 0,
            'other': 0
          },
          lpStatusBreakdown: {}
        };
      }

      statsByDate[date].total++;

      statsByDate[date].statusBreakdown[actualStatus] =
        (statsByDate[date].statusBreakdown[actualStatus] || 0) + 1;

      if (actualStatus === 'Success') {
        statsByDate[date].statusCategories['Success']++;
      } else if (actualStatus === 'Fail') {
        statsByDate[date].statusCategories['Fail']++;
      } else if (actualStatus === 'Error') {
        statsByDate[date].statusCategories['other']++;
      } else if (!actualStatus || actualStatus === 'null') {
        statsByDate[date].statusCategories['null']++;
      } else {
        statsByDate[date].statusCategories['other']++;
      }

      if (item.lpStatus) {
        statsByDate[date].lpStatusBreakdown[item.lpStatus] =
          (statsByDate[date].lpStatusBreakdown[item.lpStatus] || 0) + 1;
      }
    });

    return statsByDate;
  }

  // ─── updateFromCSV (unchanged) ─────────────────────────────────────────────

  static async updateFromCSV(logId, data) {
    const updateParts = [];
    const expressionAttributeNames = {};
    const expressionAttributeValues = {
      ':csvUpdatedAt': new Date().toISOString()
    };

    updateParts.push('#csvUpdatedAt = :csvUpdatedAt');
    expressionAttributeNames['#csvUpdatedAt'] = 'csvUpdatedAt';

    const csvFields = [
      'lpLeadId', 'lpLeadDate', 'lpStatus', 'lpIncomeType', 'lpRejectReason',
      'api1HitDate', 'api1Response', 'api1Reason',
      'api2HitDate', 'api2Response', 'api2Reason',
      'sanctionedAmount', 'sanctionedDate',
      'disbursedAmount', 'disbursedDate',
      'afMediaSource', 'afPartner'
    ];

    csvFields.forEach(field => {
      if (data[field] !== undefined && data[field] !== null && data[field] !== '') {
        updateParts.push(`#${field} = :${field}`);
        expressionAttributeNames[`#${field}`] = field;
        expressionAttributeValues[`:${field}`] = data[field];
      }
    });

    const params = {
      TableName: TABLE_NAME,
      Key: { logId },
      UpdateExpression: `SET ${updateParts.join(', ')}`,
      ExpressionAttributeNames: expressionAttributeNames,
      ExpressionAttributeValues: expressionAttributeValues,
      ReturnValues: 'ALL_NEW'
    };

    const result = await docClient.send(new UpdateCommand(params));
    return result.Attributes;
  }
}

module.exports = LendingPlateResponseLog;
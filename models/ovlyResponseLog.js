const { docClient } = require('../dynamodb');
const { PutCommand, GetCommand, QueryCommand, UpdateCommand } = require('@aws-sdk/lib-dynamodb');
const { v4: uuidv4 } = require('uuid');

const TABLE_NAME = 'ovly_response_logs';

class OvlyResponseLog {
  // Create log entry
  static async create(logData) {
    if (!logData.source) {
      throw new Error('source is required to enable Query operations. Provide "OVLY" or appropriate source.');
    }

    const item = {
      logId: uuidv4(),
      leadId: logData.leadId,
      source: logData.source,
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

  static async findById(logId) {
    const result = await docClient.send(new GetCommand({
      TableName: TABLE_NAME,
      Key: { logId }
    }));
    return result.Item || null;
  }

  static async findByLeadId(leadId, options = {}) {
    if (!leadId) {
      throw new Error('leadId is required');
    }

    const { limit = 100, lastEvaluatedKey } = options;
    
    const params = {
      TableName: TABLE_NAME,
      IndexName: 'leadId-index',
      KeyConditionExpression: 'leadId = :leadId',
      ExpressionAttributeValues: { ':leadId': leadId },
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

  static async findBySourceAndDateRange(source, startDate, endDate, options = {}) {
    if (!source) {
      throw new Error('source is required to use efficient Query. This prevents expensive Scans that cost $0.45 each.');
    }

    const { limit = 1000, lastEvaluatedKey } = options;
    
    const params = {
      TableName: TABLE_NAME,
      IndexName: 'source-createdAt-index',
      KeyConditionExpression: '#source = :source',
      ExpressionAttributeNames: {
        '#source': 'source'
      },
      ExpressionAttributeValues: {
        ':source': source
      },
      ScanIndexForward: false,
      Limit: limit
    };

    if (startDate && endDate) {
      params.KeyConditionExpression += ' AND #createdAt BETWEEN :startDate AND :endDate';
      params.ExpressionAttributeNames['#createdAt'] = 'createdAt';
      params.ExpressionAttributeValues[':startDate'] = startDate;
      params.ExpressionAttributeValues[':endDate'] = endDate;
    } else if (startDate) {
      params.KeyConditionExpression += ' AND #createdAt >= :startDate';
      params.ExpressionAttributeNames['#createdAt'] = 'createdAt';
      params.ExpressionAttributeValues[':startDate'] = startDate;
    } else if (endDate) {
      params.KeyConditionExpression += ' AND #createdAt <= :endDate';
      params.ExpressionAttributeNames['#createdAt'] = 'createdAt';
      params.ExpressionAttributeValues[':endDate'] = endDate;
    }

    if (lastEvaluatedKey) {
      params.ExclusiveStartKey = lastEvaluatedKey;
    }

    const result = await docClient.send(new QueryCommand(params));
    return {
      items: result.Items || [],
      lastEvaluatedKey: result.LastEvaluatedKey
    };
  }

  /**
   * ✅ ENHANCED: Get quick stats WITH SOURCE BREAKDOWN
   */
  static async getQuickStats(source, startDate = null, endDate = null) {
    if (!source) {
      throw new Error(
        'source is required for getQuickStats(). This prevents expensive Scans. ' +
        'Example: getQuickStats("OVLY", startDate, endDate)'
      );
    }

    const startTime = Date.now();

    try {
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
      } else if (startDate) {
        params.KeyConditionExpression += ' AND #createdAt >= :startDate';
        params.ExpressionAttributeNames['#createdAt'] = 'createdAt';
        params.ExpressionAttributeValues[':startDate'] = startDate;
      } else if (endDate) {
        params.KeyConditionExpression += ' AND #createdAt <= :endDate';
        params.ExpressionAttributeNames['#createdAt'] = 'createdAt';
        params.ExpressionAttributeValues[':endDate'] = endDate;
      }

      let totalCount = 0;
      let lastKey = null;

      do {
        if (lastKey) {
          params.ExclusiveStartKey = lastKey;
        }

        const result = await docClient.send(new QueryCommand(params));
        totalCount += result.Count || 0;
        lastKey = result.LastEvaluatedKey;

        delete params.ExclusiveStartKey;
      } while (lastKey);

      const elapsed = Date.now() - startTime;
      console.log(`✅ Query COUNT: ${totalCount} items in ${elapsed}ms (source: ${source})`);

      return {
        totalLogs: totalCount,
        source: source,
        sourceBreakdown: { [source]: totalCount }, // ✅ NEW: Include source breakdown
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

  /**
   * ✅ ENHANCED: Get comprehensive stats WITH SOURCE-WISE BREAKDOWN
   */
  static async getStats(source, startDate = null, endDate = null) {
    if (!source) {
      throw new Error(
        'source is required for getStats(). This prevents expensive Scans. ' +
        'Example: getStats("OVLY", startDate, endDate)'
      );
    }

    const startTime = Date.now();
    console.log(`[${TABLE_NAME}] Fetching stats for source: ${source}, date range:`, startDate, 'to', endDate);

    try {
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
      } else if (startDate) {
        params.KeyConditionExpression += ' AND #createdAt >= :startDate';
        params.ExpressionAttributeNames['#createdAt'] = 'createdAt';
        params.ExpressionAttributeValues[':startDate'] = startDate;
      } else if (endDate) {
        params.KeyConditionExpression += ' AND #createdAt <= :endDate';
        params.ExpressionAttributeNames['#createdAt'] = 'createdAt';
        params.ExpressionAttributeValues[':endDate'] = endDate;
      }

      do {
        if (lastKey) {
          params.ExclusiveStartKey = lastKey;
        }

        const result = await docClient.send(new QueryCommand(params));
        allItems = allItems.concat(result.Items || []);
        lastKey = result.LastEvaluatedKey;

        delete params.ExclusiveStartKey;
      } while (lastKey);

      console.log(`✅ Query complete: ${allItems.length} items in ${Date.now() - startTime}ms`);

      // Calculate stats with SOURCE-WISE BREAKDOWN
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

  /**
   * ✅ NEW: Calculate stats WITH SOURCE-WISE BREAKDOWN for eligible, success, etc.
   */
  static _calculateStatsWithSourceBreakdown(items, source, startDate, endDate) {
    const stats = {
      totalLogs: items.length,
      source: source,
      dateRange: {
        start: startDate,
        end: endDate
      },
      responseStatusBreakdown: {},
      sourceBreakdown: {},
      statusCategoryBreakdown: {
        '403': 0,
        'success': 0,
        'duplicate': 0,
        'other': 0
      },
      // ✅ NEW: Source-wise breakdowns
      sourceWiseStats: {},
      successRate: 0,
      successRateBySource: {} // ✅ NEW: Success rate per source
    };

    items.forEach(item => {
      // Response status breakdown
      const status = item.responseStatus || 'unknown';
      stats.responseStatusBreakdown[status] = (stats.responseStatusBreakdown[status] || 0) + 1;

      // Source breakdown
      const itemSource = item.source || 'unknown';
      stats.sourceBreakdown[itemSource] = (stats.sourceBreakdown[itemSource] || 0) + 1;

      // ✅ NEW: Initialize source-wise stats
      if (!stats.sourceWiseStats[itemSource]) {
        stats.sourceWiseStats[itemSource] = {
          totalLogs: 0,
          eligible: 0,
          success: 0,
          duplicate: 0,
          forbidden: 0,
          other: 0
        };
      }

      stats.sourceWiseStats[itemSource].totalLogs++;

      // Status category breakdown
      const statusLower = String(status).toLowerCase();
      if (statusLower === '403') {
        stats.statusCategoryBreakdown['403']++;
        stats.sourceWiseStats[itemSource].forbidden++; // ✅ NEW
      } else if (statusLower === 'success' || statusLower === '200') {
        stats.statusCategoryBreakdown['success']++;
        stats.sourceWiseStats[itemSource].success++; // ✅ NEW
        stats.sourceWiseStats[itemSource].eligible++; // ✅ NEW: Success means eligible
      } else if (statusLower === 'duplicate') {
        stats.statusCategoryBreakdown['duplicate']++;
        stats.sourceWiseStats[itemSource].duplicate++; // ✅ NEW
      } else {
        stats.statusCategoryBreakdown['other']++;
        stats.sourceWiseStats[itemSource].other++; // ✅ NEW
      }
    });

    // Calculate overall success rate
    const successCount = stats.statusCategoryBreakdown['success'];
    stats.successRate = items.length > 0
      ? ((successCount / items.length) * 100).toFixed(2) + '%'
      : '0%';

    // ✅ NEW: Calculate success rate per source
    Object.keys(stats.sourceWiseStats).forEach(src => {
      const sourceStats = stats.sourceWiseStats[src];
      const sourceSuccessRate = sourceStats.totalLogs > 0
        ? ((sourceStats.success / sourceStats.totalLogs) * 100).toFixed(2) + '%'
        : '0%';
      
      stats.successRateBySource[src] = sourceSuccessRate;
      stats.sourceWiseStats[src].successRate = sourceSuccessRate;
    });

    return stats;
  }

  static async getStatsByDate(source, startDate, endDate) {
    if (!source) {
      throw new Error(
        'source is required for getStatsByDate(). This prevents expensive Scans. ' +
        'Example: getStatsByDate("OVLY", startDate, endDate)'
      );
    }

    const startTime = Date.now();
    console.log(`[${TABLE_NAME}] Fetching stats by date for source: ${source}, date range:`, startDate, 'to', endDate);

    try {
      let allItems = [];
      let lastKey = null;

      const params = {
        TableName: TABLE_NAME,
        IndexName: 'source-createdAt-index',
        KeyConditionExpression: '#source = :source AND #createdAt BETWEEN :startDate AND :endDate',
        ExpressionAttributeNames: {
          '#source': 'source',
          '#createdAt': 'createdAt'
        },
        ExpressionAttributeValues: {
          ':source': source,
          ':startDate': startDate,
          ':endDate': endDate
        },
        ScanIndexForward: false
      };

      do {
        if (lastKey) {
          params.ExclusiveStartKey = lastKey;
        }

        const result = await docClient.send(new QueryCommand(params));
        allItems = allItems.concat(result.Items || []);
        lastKey = result.LastEvaluatedKey;

        delete params.ExclusiveStartKey;
      } while (lastKey);

      console.log(`✅ Query complete: ${allItems.length} items in ${Date.now() - startTime}ms`);

      const statsByDate = this._groupByDate(allItems);

      return Object.values(statsByDate).sort((a, b) =>
        a.date.localeCompare(b.date)
      );
    } catch (error) {
      console.error('Error in getStatsByDate:', error);
      throw error;
    }
  }

  static _groupByDate(items) {
    const statsByDate = {};

    items.forEach(item => {
      const date = item.createdAt.split('T')[0];

      if (!statsByDate[date]) {
        statsByDate[date] = {
          date,
          total: 0,
          statusBreakdown: {},
          statusCategories: {
            '403': 0,
            'success': 0,
            'duplicate': 0,
            'other': 0
          }
        };
      }

      statsByDate[date].total++;

      const status = item.responseStatus || 'unknown';
      statsByDate[date].statusBreakdown[status] =
        (statsByDate[date].statusBreakdown[status] || 0) + 1;

      const statusLower = String(status).toLowerCase();
      if (statusLower === '403') {
        statsByDate[date].statusCategories['403']++;
      } else if (statusLower === 'success' || statusLower === '200') {
        statsByDate[date].statusCategories['success']++;
      } else if (statusLower === 'duplicate') {
        statsByDate[date].statusCategories['duplicate']++;
      } else {
        statsByDate[date].statusCategories['other']++;
      }
    });

    return statsByDate;
  }

  /**
   * ✅ ENHANCED: Batch get stats for multiple sources with combined source breakdown
   */
  static async getStatsBatch(sources, startDate, endDate) {
    if (!sources || !Array.isArray(sources) || sources.length === 0) {
      throw new Error('sources array is required. Example: ["OVLY", "FREO", "SML"]');
    }

    console.log(`Fetching stats for ${sources.length} sources:`, sources);

    const promises = sources.map(source =>
      this.getStats(source, startDate, endDate)
        .catch(err => {
          console.error(`Error fetching stats for ${source}:`, err);
          return null;
        })
    );

    const results = await Promise.all(promises);
    const successfulResults = results.filter(r => r !== null);

    // Combine stats with detailed source breakdown
    const combined = {
      totalLogs: successfulResults.reduce((sum, r) => sum + r.totalLogs, 0),
      sources: successfulResults.map(r => r.source),
      dateRange: { start: startDate, end: endDate },
      bySource: {},
      sourceWiseStats: {},
      overallSuccessRate: 0
    };

    let totalSuccess = 0;
    
    successfulResults.forEach(result => {
      combined.bySource[result.source] = {
        totalLogs: result.totalLogs,
        successRate: result.successRate,
        responseStatusBreakdown: result.responseStatusBreakdown
      };

      // ✅ NEW: Merge source-wise stats
      if (result.sourceWiseStats) {
        Object.keys(result.sourceWiseStats).forEach(src => {
          if (!combined.sourceWiseStats[src]) {
            combined.sourceWiseStats[src] = {
              totalLogs: 0,
              eligible: 0,
              success: 0,
              duplicate: 0,
              forbidden: 0,
              other: 0
            };
          }
          
          const srcStats = result.sourceWiseStats[src];
          combined.sourceWiseStats[src].totalLogs += srcStats.totalLogs;
          combined.sourceWiseStats[src].eligible += srcStats.eligible;
          combined.sourceWiseStats[src].success += srcStats.success;
          combined.sourceWiseStats[src].duplicate += srcStats.duplicate;
          combined.sourceWiseStats[src].forbidden += srcStats.forbidden;
          combined.sourceWiseStats[src].other += srcStats.other;
        });
      }

      // Extract success count for overall rate
      const successCount = result.statusCategoryBreakdown?.success || 0;
      totalSuccess += successCount;
    });

    // Calculate overall success rate
    combined.overallSuccessRate = combined.totalLogs > 0
      ? ((totalSuccess / combined.totalLogs) * 100).toFixed(2) + '%'
      : '0%';

    // Calculate success rate for each source in combined stats
    Object.keys(combined.sourceWiseStats).forEach(src => {
      const srcStats = combined.sourceWiseStats[src];
      srcStats.successRate = srcStats.totalLogs > 0
        ? ((srcStats.success / srcStats.totalLogs) * 100).toFixed(2) + '%'
        : '0%';
    });

    return combined;
  }

  static async updateStatusWithData(logId, data) {
    const updateParts = [];
    const expressionAttributeNames = {};
    const expressionAttributeValues = {
      ':updatedAt': new Date().toISOString()
    };

    if (data.lead_id) {
      updateParts.push('#lead_id = :lead_id');
      expressionAttributeNames['#lead_id'] = 'lead_id';
      expressionAttributeValues[':lead_id'] = data.lead_id;
    }

    if (data.rejection_reason) {
      updateParts.push('#rejection_reason = :rejection_reason');
      expressionAttributeNames['#rejection_reason'] = 'rejection_reason';
      expressionAttributeValues[':rejection_reason'] = data.rejection_reason;
    }

    const conditionalFields = [
      'unlock_amount',
      'applied_date',
      'kyc_completed_date',
      'approved_date',
      'emandate_done_at',
      'agreement_signed_date',
      'loan_amount',
      'loan_disbursed_date'
    ];

    conditionalFields.forEach(field => {
      if (data[field] !== undefined && data[field] !== null) {
        updateParts.push(`#${field} = :${field}`);
        expressionAttributeNames[`#${field}`] = field;
        expressionAttributeValues[`:${field}`] = data[field];
      }
    });

    updateParts.push('#updatedAt = :updatedAt');
    expressionAttributeNames['#updatedAt'] = 'updatedAt';

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

module.exports = OvlyResponseLog;
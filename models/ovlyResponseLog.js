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
   * ✅ FIXED: Get quick stats - handles both single source and multi-source
   */
  static async getQuickStats(source = null, startDate = null, endDate = null) {
    const startTime = Date.now();

    try {
      // ✅ NEW: If no source provided, get all known sources and aggregate
      if (!source) {
        console.log('[OVLY] No source provided, fetching all sources...');
        return await this._getQuickStatsAllSources(startDate, endDate);
      }

      // Original single-source logic
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
        sourceBreakdown: { [source]: totalCount },
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
   * ✅ NEW: Get quick stats for all sources (when source not specified)
   */
  static async _getQuickStatsAllSources(startDate = null, endDate = null) {
    const startTime = Date.now();

    try {
      // Get all known sources from environment or default to OVLY
      const sources = ['CashKuber', 'FREO', 'BatterySmart', 'Ratecut', 'VFC'];

      console.log(`[OVLY] Counting ${sources.length} sources:`, sources);

      // Count each source in parallel
      const countPromises = sources.map(async (source) => {
        const count = await this._countSource(source, startDate, endDate);
        return { source, count };
      });

      const results = await Promise.all(countPromises);

      // Build aggregated result
      const sourceBreakdown = {};
      let totalCount = 0;

      results.forEach(({ source, count }) => {
        sourceBreakdown[source] = count;
        totalCount += count;
        console.log(`  Source "${source}": ${count.toLocaleString()} records`);
      });

      const elapsed = Date.now() - startTime;
      console.log(`[OVLY] ✅ Total count: ${totalCount.toLocaleString()} in ${elapsed}ms`);

      return {
        totalLogs: totalCount,
        sourceBreakdown,
        sources: sources,
        dateRange: startDate && endDate ? { start: startDate, end: endDate } : null,
        scannedInMs: elapsed,
        method: 'query-count-all-sources',
        indexUsed: 'source-createdAt-index'
      };
    } catch (error) {
      console.error('Error in _getQuickStatsAllSources:', error);
      throw error;
    }
  }

  /**
   * ✅ NEW: Count a single source with optional date range
   */
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

    return totalCount;
  }

  /**
   * ✅ FIXED: Get comprehensive stats - handles both single source and multi-source
   */
  static async getStats(source = null, startDate = null, endDate = null) {
    const startTime = Date.now();

    try {
      // ✅ NEW: If no source provided, get stats for all sources
      if (!source) {
        console.log('[OVLY] No source provided, fetching stats for all sources...');
        return await this._getStatsAllSources(startDate, endDate);
      }

      // Original single-source logic
      console.log(`[${TABLE_NAME}] Fetching stats for source: ${source}, date range:`, startDate, 'to', endDate);

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
   * ✅ NEW: Get stats for all sources (when source not specified)
   */
  static async _getStatsAllSources(startDate = null, endDate = null) {
    const startTime = Date.now();

    try {
      const sources = ['CashKuber', 'FREO', 'BatterySmart', 'Ratecut', 'VFC'];

      console.log(`[OVLY] Fetching stats for ${sources.length} sources:`, sources);

      // Fetch stats for each source in parallel
      const statsPromises = sources.map(source =>
        this.getStats(source, startDate, endDate).catch(err => {
          console.error(`Error fetching stats for ${source}:`, err);
          return null;
        })
      );

      const results = await Promise.all(statsPromises);
      const validResults = results.filter(r => r !== null);

      // Merge all stats
      const mergedStats = this._mergeStats(validResults, startDate, endDate);
      mergedStats.processingTimeMs = Date.now() - startTime;
      mergedStats.method = 'query-all-sources';

      console.log(`[OVLY] ✅ Stats complete: ${mergedStats.totalLogs.toLocaleString()} records in ${Date.now() - startTime}ms`);

      return mergedStats;
    } catch (error) {
      console.error('Error in _getStatsAllSources:', error);
      throw error;
    }
  }

  /**
   * ✅ NEW: Merge stats from multiple sources
   */
  static _mergeStats(statsArray, startDate, endDate) {
    const merged = {
      totalLogs: 0,
      sources: [],
      dateRange: { start: startDate, end: endDate },
      responseStatusBreakdown: {},
      sourceBreakdown: {},
      statusCategoryBreakdown: {
        '403': 0,
        'success': 0,
        'duplicate': 0,
        'other': 0
      },
      sourceWiseStats: {},
      successRate: 0,
      successRateBySource: {}
    };

    statsArray.forEach(stats => {
      if (!stats) return;

      merged.totalLogs += stats.totalLogs;
      merged.sources.push(stats.source);

      // Merge response status breakdown
      Object.keys(stats.responseStatusBreakdown || {}).forEach(status => {
        merged.responseStatusBreakdown[status] =
          (merged.responseStatusBreakdown[status] || 0) + stats.responseStatusBreakdown[status];
      });

      // Merge source breakdown
      Object.keys(stats.sourceBreakdown || {}).forEach(source => {
        merged.sourceBreakdown[source] =
          (merged.sourceBreakdown[source] || 0) + stats.sourceBreakdown[source];
      });

      // Merge status category breakdown
      Object.keys(stats.statusCategoryBreakdown || {}).forEach(category => {
        merged.statusCategoryBreakdown[category] += stats.statusCategoryBreakdown[category] || 0;
      });

      // Merge source-wise stats
      Object.keys(stats.sourceWiseStats || {}).forEach(source => {
        if (!merged.sourceWiseStats[source]) {
          merged.sourceWiseStats[source] = {
            totalLogs: 0,
            eligible: 0,
            success: 0,
            duplicate: 0,
            forbidden: 0,
            other: 0
          };
        }

        const srcStats = stats.sourceWiseStats[source];
        merged.sourceWiseStats[source].totalLogs += srcStats.totalLogs || 0;
        merged.sourceWiseStats[source].eligible += srcStats.eligible || 0;
        merged.sourceWiseStats[source].success += srcStats.success || 0;
        merged.sourceWiseStats[source].duplicate += srcStats.duplicate || 0;
        merged.sourceWiseStats[source].forbidden += srcStats.forbidden || 0;
        merged.sourceWiseStats[source].other += srcStats.other || 0;
      });

      // Merge success rate by source
      Object.keys(stats.successRateBySource || {}).forEach(source => {
        merged.successRateBySource[source] = stats.successRateBySource[source];
      });
    });

    // Calculate overall success rate
    const totalSuccess = merged.statusCategoryBreakdown.success || 0;
    merged.successRate = merged.totalLogs > 0
      ? ((totalSuccess / merged.totalLogs) * 100).toFixed(2) + '%'
      : '0%';

    // Recalculate success rate for each source in merged stats
    Object.keys(merged.sourceWiseStats).forEach(source => {
      const srcStats = merged.sourceWiseStats[source];
      srcStats.successRate = srcStats.totalLogs > 0
        ? ((srcStats.success / srcStats.totalLogs) * 100).toFixed(2) + '%'
        : '0%';
      merged.successRateBySource[source] = srcStats.successRate;
    });

    return merged;
  }

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
      sourceWiseStats: {},
      successRate: 0,
      successRateBySource: {}
    };

    items.forEach(item => {
      const status = item.responseStatus || 'unknown';
      stats.responseStatusBreakdown[status] = (stats.responseStatusBreakdown[status] || 0) + 1;

      const itemSource = item.source || 'unknown';
      stats.sourceBreakdown[itemSource] = (stats.sourceBreakdown[itemSource] || 0) + 1;

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

      const statusLower = String(status).toLowerCase();
      if (statusLower === '403') {
        stats.statusCategoryBreakdown['403']++;
        stats.sourceWiseStats[itemSource].forbidden++;
      } else if (statusLower === 'success' || statusLower === '200') {
        stats.statusCategoryBreakdown['success']++;
        stats.sourceWiseStats[itemSource].success++;
        stats.sourceWiseStats[itemSource].eligible++;
      } else if (statusLower === 'duplicate') {
        stats.statusCategoryBreakdown['duplicate']++;
        stats.sourceWiseStats[itemSource].duplicate++;
      } else {
        stats.statusCategoryBreakdown['other']++;
        stats.sourceWiseStats[itemSource].other++;
      }
    });

    const successCount = stats.statusCategoryBreakdown['success'];
    stats.successRate = items.length > 0
      ? ((successCount / items.length) * 100).toFixed(2) + '%'
      : '0%';

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

  /**
   * Get stats grouped by date - supports multiple sources
   * ✅ FIXED: Handles call from controller: getStatsByDate(startDate, endDate)
   */
  static async getStatsByDate(startDateOrSource, endDateOrStart, endDate = null) {
    const startTime = Date.now();

    try {
      // ✅ Detect call pattern from controller
      let source, startDate, actualEndDate;

      // Pattern 1: getStatsByDate(startDate, endDate) - from controller
      if (!endDate && startDateOrSource && endDateOrStart) {
        source = null;
        startDate = startDateOrSource;
        actualEndDate = endDateOrStart;
      }
      // Pattern 2: getStatsByDate(source, startDate, endDate) - direct call
      else {
        source = startDateOrSource;
        startDate = endDateOrStart;
        actualEndDate = endDate;
      }

      console.log(`[${TABLE_NAME}] getStatsByDate called with source=${source}, startDate=${startDate}, endDate=${actualEndDate}`);

      // If no source provided, get stats for all sources
      if (!source) {
        const sources = ['CashKuber', 'FREO', 'BatterySmart', 'Ratecut', 'VFC'];
        console.log(`[${TABLE_NAME}] Fetching stats by date for all sources:`, sources);

        // Fetch stats for each source in parallel
        const promises = sources.map(src =>
          this._getStatsByDateForSource(src, startDate, actualEndDate).catch(err => {
            console.error(`Error fetching stats by date for ${src}:`, err);
            return [];
          })
        );

        const results = await Promise.all(promises);

        // Merge results by date
        const mergedByDate = this._mergeStatsByDate(results.flat());

        return Object.values(mergedByDate).sort((a, b) => a.date.localeCompare(b.date));
      }

      // Single source logic
      return this._getStatsByDateForSource(source, startDate, actualEndDate);
    } catch (error) {
      console.error('Error in getStatsByDate:', error);
      throw error;
    }
  }

  static async _getStatsByDateForSource(source, startDate, endDate) {
    console.log(`[${TABLE_NAME}] Fetching stats by date for source: ${source}, ${startDate} to ${endDate}`);

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

    console.log(`  ✅ ${source}: ${allItems.length} items`);

    // Group by date and return array
    return this._groupByDateArray(allItems);
  }

  static _mergeStatsByDate(statsArray) {
    const merged = {};

    statsArray.forEach(dayStat => {
      const date = dayStat.date;

      if (!merged[date]) {
        merged[date] = {
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

      merged[date].total += dayStat.total;

      // Merge status breakdown
      Object.keys(dayStat.statusBreakdown || {}).forEach(status => {
        merged[date].statusBreakdown[status] =
          (merged[date].statusBreakdown[status] || 0) + dayStat.statusBreakdown[status];
      });

      // Merge status categories
      Object.keys(dayStat.statusCategories || {}).forEach(category => {
        merged[date].statusCategories[category] += dayStat.statusCategories[category] || 0;
      });
    });

    return merged;
  }

  static _groupByDateArray(items) {
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

    return Object.values(statsByDate).sort((a, b) => a.date.localeCompare(b.date));
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

      const successCount = result.statusCategoryBreakdown?.success || 0;
      totalSuccess += successCount;
    });

    combined.overallSuccessRate = combined.totalLogs > 0
      ? ((totalSuccess / combined.totalLogs) * 100).toFixed(2) + '%'
      : '0%';

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
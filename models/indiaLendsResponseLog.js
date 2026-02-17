// models/indiaLendsResponseLog.js
const { docClient } = require('../dynamodb');
const { PutCommand, GetCommand, QueryCommand, UpdateCommand } = require('@aws-sdk/lib-dynamodb');
const { v4: uuidv4 } = require('uuid');

const TABLE_NAME = 'indialends_response_logs';
const SOURCES = process.env.INDIALENDS_SOURCES?.split(',').map(s => s.trim())
  || ['CashKuber', 'FREO', 'BatterySmart', 'Ratecut', 'VFC'];

class IndiaLendsResponseLog {

  // ═══════════════════════════════════════════════════════════════════════════
  // HELPERS
  // ═══════════════════════════════════════════════════════════════════════════

  static async _queryAll(params) {
    const items = [];
    let lastKey;
    const p = { ...params };
    do {
      if (lastKey) p.ExclusiveStartKey = lastKey;
      const res = await docClient.send(new QueryCommand(p));
      items.push(...(res.Items || []));
      lastKey = res.LastEvaluatedKey;
      delete p.ExclusiveStartKey;
    } while (lastKey);
    return items;
  }

  static async _queryCount(params) {
    let total = 0;
    let lastKey;
    const p = { ...params, Select: 'COUNT' };
    do {
      if (lastKey) p.ExclusiveStartKey = lastKey;
      const res = await docClient.send(new QueryCommand(p));
      total += res.Count || 0;
      lastKey = res.LastEvaluatedKey;
      delete p.ExclusiveStartKey;
    } while (lastKey);
    return total;
  }

  static _sourceParams(source, startDate, endDate, extra = {}) {
    const p = {
      TableName: TABLE_NAME,
      IndexName: 'source-createdAt-index',
      KeyConditionExpression: '#src = :src',
      ExpressionAttributeNames: { '#src': 'source' },
      ExpressionAttributeValues: { ':src': source },
      ScanIndexForward: false,
      ...extra
    };
    if (startDate && endDate) {
      p.KeyConditionExpression += ' AND #ca BETWEEN :s AND :e';
      p.ExpressionAttributeNames['#ca'] = 'createdAt';
      p.ExpressionAttributeValues[':s'] = startDate;
      p.ExpressionAttributeValues[':e'] = endDate;
    } else if (startDate) {
      p.KeyConditionExpression += ' AND #ca >= :s';
      p.ExpressionAttributeNames['#ca'] = 'createdAt';
      p.ExpressionAttributeValues[':s'] = startDate;
    } else if (endDate) {
      p.KeyConditionExpression += ' AND #ca <= :e';
      p.ExpressionAttributeNames['#ca'] = 'createdAt';
      p.ExpressionAttributeValues[':e'] = endDate;
    }
    return p;
  }

  static async _fetchAllSources(startDate, endDate) {
    let allItems = [];
    for (const src of SOURCES) {
      const items = await this._queryAll(this._sourceParams(src, startDate, endDate));
      console.log(`  [${TABLE_NAME}] ${src}: ${items.length} items`);
      allItems = allItems.concat(items);
    }
    return allItems;
  }

  // ═══════════════════════════════════════════════════════════════════════════
  // WRITE
  // ═══════════════════════════════════════════════════════════════════════════

  static async create(logData) {
    if (!logData.leadId) throw new Error('leadId is required');
    if (!logData.source) throw new Error('source is required for source-createdAt-index');

    const responseStatus = logData.responseStatus || 500;
    const isSuccess = responseStatus >= 200 && responseStatus < 300;

    const item = {
      logId: uuidv4(),
      leadId: logData.leadId,
      source: logData.source,
      accessToken: logData.accessToken || null,
      dedupCheck: logData.dedupCheck || null,
      isDuplicate: String(logData.isDuplicate || false),
      duplicateStatus: logData.duplicateStatus || '0',
      requestPayload: logData.requestPayload,
      responseStatus,
      responseBody: logData.responseBody,
      errorDetails: logData.errorDetails || null,
      retryCount: logData.retryCount || 0,
      isSuccess: String(isSuccess),
      createdAt: new Date().toISOString(),
      updatedAt: new Date().toISOString()
    };

    await docClient.send(new PutCommand({ TableName: TABLE_NAME, Item: item }));
    return item;
  }

  // ═══════════════════════════════════════════════════════════════════════════
  // READS
  // ═══════════════════════════════════════════════════════════════════════════

  static async findById(logId) {
    const res = await docClient.send(new GetCommand({ TableName: TABLE_NAME, Key: { logId } }));
    return res.Item || null;
  }

  // leadId-createdAt-index (sort key = createdAt → date range in key condition)
  static async findByLeadId(leadId, options = {}) {
    if (!leadId) throw new Error('leadId is required');
    const { limit = 100, sortAscending = false, startDate, endDate, lastEvaluatedKey } = options;

    const params = {
      TableName: TABLE_NAME,
      IndexName: 'leadId-createdAt-index',
      KeyConditionExpression: 'leadId = :lid',
      ExpressionAttributeValues: { ':lid': leadId },
      ScanIndexForward: sortAscending,
      Limit: limit
    };
    if (startDate && endDate) {
      params.KeyConditionExpression += ' AND #ca BETWEEN :s AND :e';
      params.ExpressionAttributeNames = { '#ca': 'createdAt' };
      params.ExpressionAttributeValues[':s'] = startDate;
      params.ExpressionAttributeValues[':e'] = endDate;
    }
    if (lastEvaluatedKey) params.ExclusiveStartKey = lastEvaluatedKey;

    const res = await docClient.send(new QueryCommand(params));
    return { items: res.Items || [], lastEvaluatedKey: res.LastEvaluatedKey };
  }

  static async findBySource(source, options = {}) {
    if (!source) throw new Error('source is required');
    const { limit = 100, startDate, endDate, sortAscending = false, lastEvaluatedKey } = options;
    const params = {
      ...this._sourceParams(source, startDate, endDate),
      ScanIndexForward: sortAscending,
      Limit: limit
    };
    if (lastEvaluatedKey) params.ExclusiveStartKey = lastEvaluatedKey;
    const res = await docClient.send(new QueryCommand(params));
    return { items: res.Items || [], lastEvaluatedKey: res.LastEvaluatedKey };
  }

  // IndiaLends-specific: isDuplicate-isSuccess-index
  static async getDuplicateLeads(options = {}) {
    const { limit = 100, startDate, endDate, lastEvaluatedKey } = options;

    const params = {
      TableName: TABLE_NAME,
      IndexName: 'isDuplicate-isSuccess-index',
      KeyConditionExpression: 'isDuplicate = :dup',
      ExpressionAttributeValues: { ':dup': 'true' },
      Limit: limit
    };
    if (startDate && endDate) {
      params.FilterExpression = '#ca BETWEEN :s AND :e';
      params.ExpressionAttributeNames = { '#ca': 'createdAt' };
      params.ExpressionAttributeValues[':s'] = startDate;
      params.ExpressionAttributeValues[':e'] = endDate;
    }
    if (lastEvaluatedKey) params.ExclusiveStartKey = lastEvaluatedKey;

    const res = await docClient.send(new QueryCommand(params));
    return { items: res.Items || [], lastEvaluatedKey: res.LastEvaluatedKey };
  }

  // ═══════════════════════════════════════════════════════════════════════════
  // QUICK STATS
  // ═══════════════════════════════════════════════════════════════════════════

  static async getQuickStats(source = null, startDate = null, endDate = null) {
    const t0 = Date.now();

    if (!source) {
      const results = await Promise.all(
        SOURCES.map(async src => ({
          source: src,
          count: await this._queryCount(this._sourceParams(src, startDate, endDate))
        }))
      );
      const sourceBreakdown = {};
      let totalLogs = 0;
      results.forEach(({ source: src, count }) => {
        sourceBreakdown[src] = count;
        totalLogs += count;
      });
      return {
        totalLogs, sourceBreakdown,
        dateRange: startDate ? { start: startDate, end: endDate } : null,
        scannedInMs: Date.now() - t0,
        method: 'query-count-all-sources',
        indexUsed: 'source-createdAt-index'
      };
    }

    const count = await this._queryCount(this._sourceParams(source, startDate, endDate));
    return {
      totalLogs: count, source,
      sourceBreakdown: { [source]: count },
      dateRange: startDate ? { start: startDate, end: endDate } : null,
      scannedInMs: Date.now() - t0,
      method: 'query-count',
      indexUsed: 'source-createdAt-index'
    };
  }

  // ═══════════════════════════════════════════════════════════════════════════
  // FULL STATS — source-wise breakdown
  // ═══════════════════════════════════════════════════════════════════════════

  static async getStats(source = null, startDate = null, endDate = null) {
    const t0 = Date.now();

    const allItems = source
      ? await this._queryAll(this._sourceParams(source, startDate, endDate))
      : await this._fetchAllSources(startDate, endDate);

    console.log(`[${TABLE_NAME}] getStats: ${allItems.length} items in ${Date.now() - t0}ms`);

    const stats = {
      totalLogs: allItems.length,
      source: source || 'all',
      dateRange: { start: startDate, end: endDate },
      responseStatusBreakdown: {},
      sourceBreakdown: {},
      duplicateBreakdown: { 'true': 0, 'false': 0, 'unknown': 0 },
      successBreakdown: { 'true': 0, 'false': 0 },
      sourceWiseStats: {},
      successRateBySource: {},
      messageBreakdown: {},
      verificationSent: 0,
      duplicateFound: 0,
      successRate: '0%',
      duplicateRate: '0%',
      processingTimeMs: 0,
      method: source ? 'query' : 'query-all-sources',
      indexUsed: 'source-createdAt-index'
    };

    allItems.forEach(item => {
      const rs = item.responseStatus || 'unknown';
      stats.responseStatusBreakdown[rs] = (stats.responseStatusBreakdown[rs] || 0) + 1;

      const src = item.source || 'unknown';
      stats.sourceBreakdown[src] = (stats.sourceBreakdown[src] || 0) + 1;

      const isDup = item.isDuplicate || 'unknown';
      stats.duplicateBreakdown[isDup] = (stats.duplicateBreakdown[isDup] || 0) + 1;

      const isSuc = item.isSuccess || 'false';
      stats.successBreakdown[isSuc] = (stats.successBreakdown[isSuc] || 0) + 1;

      // ✅ Source-wise stats
      if (!stats.sourceWiseStats[src]) {
        stats.sourceWiseStats[src] = {
          totalLogs: 0, successful: 0, duplicates: 0,
          verificationSent: 0, duplicateFound: 0, successRate: '0%', duplicateRate: '0%'
        };
      }
      stats.sourceWiseStats[src].totalLogs++;

      const isSuccessful = item.isSuccess === 'true' && item.isDuplicate === 'false';
      if (isSuccessful) stats.sourceWiseStats[src].successful++;
      if (item.isDuplicate === 'true') stats.sourceWiseStats[src].duplicates++;

      if (item.responseBody) {
        let body = item.responseBody;
        if (typeof body === 'string') { try { body = JSON.parse(body); } catch (_) { } }

        if (body.info?.message) {
          const msg = body.info.message;
          stats.messageBreakdown[msg] = (stats.messageBreakdown[msg] || 0) + 1;
          if (msg.includes('Verification code sent')) {
            stats.verificationSent++;
            stats.sourceWiseStats[src].verificationSent++;
          }
        }
        if (body.data?.message?.includes('Duplicate lead found')) {
          stats.duplicateFound++;
          stats.sourceWiseStats[src].duplicateFound++;
        }
      }
    });

    // Calculate rates per source
    Object.keys(stats.sourceWiseStats).forEach(src => {
      const s = stats.sourceWiseStats[src];
      s.successRate = s.totalLogs > 0 ? ((s.successful / s.totalLogs) * 100).toFixed(2) + '%' : '0%';
      s.duplicateRate = s.totalLogs > 0 ? ((s.duplicates / s.totalLogs) * 100).toFixed(2) + '%' : '0%';
      stats.successRateBySource[src] = s.successRate;
    });

    const successCount = allItems.filter(i => i.isSuccess === 'true' && i.isDuplicate === 'false').length;
    stats.successRate = allItems.length > 0 ? ((successCount / allItems.length) * 100).toFixed(2) + '%' : '0%';
    stats.duplicateRate = allItems.length > 0
      ? ((stats.duplicateBreakdown['true'] / allItems.length) * 100).toFixed(2) + '%' : '0%';
    stats.processingTimeMs = Date.now() - t0;
    return stats;
  }

  // ═══════════════════════════════════════════════════════════════════════════
  // STATS BY DATE
  // ═══════════════════════════════════════════════════════════════════════════

  static async getStatsByDate(sourceOrStart, startOrEnd, endDate) {
    const t0 = Date.now();

    let source, startDate, actualEndDate;
    if (endDate) {
      source = sourceOrStart; startDate = startOrEnd; actualEndDate = endDate;
    } else {
      source = null; startDate = sourceOrStart; actualEndDate = startOrEnd;
    }

    const allItems = source
      ? await this._queryAll(this._sourceParams(source, startDate, actualEndDate, { ScanIndexForward: true }))
      : await this._fetchAllSources(startDate, actualEndDate);

    console.log(`[${TABLE_NAME}] getStatsByDate: ${allItems.length} items in ${Date.now() - t0}ms`);

    const map = {};
    allItems.forEach(item => {
      const date = item.createdAt.split('T')[0];
      const src = item.source || 'unknown';

      if (!map[date]) {
        map[date] = {
          date, total: 0, statusBreakdown: {},
          duplicates: 0, nonDuplicates: 0, successful: 0, verificationSent: 0,
          sourceBreakdown: {}, bySource: {}
        };
      }
      map[date].total++;
      const rs = item.responseStatus || 'unknown';
      map[date].statusBreakdown[rs] = (map[date].statusBreakdown[rs] || 0) + 1;
      map[date].sourceBreakdown[src] = (map[date].sourceBreakdown[src] || 0) + 1;

      if (!map[date].bySource[src]) {
        map[date].bySource[src] = { total: 0, successful: 0, duplicates: 0, verificationSent: 0 };
      }
      map[date].bySource[src].total++;

      if (item.isDuplicate === 'true') {
        map[date].duplicates++;
        map[date].bySource[src].duplicates++;
      } else {
        map[date].nonDuplicates++;
      }

      if (item.isSuccess === 'true' && item.isDuplicate === 'false') {
        map[date].successful++;
        map[date].bySource[src].successful++;
      }

      if (item.responseBody?.info?.message?.includes('Verification code sent')) {
        map[date].verificationSent++;
        map[date].bySource[src].verificationSent++;
      }
    });

    return Object.values(map).sort((a, b) => a.date.localeCompare(b.date));
  }

  // ═══════════════════════════════════════════════════════════════════════════
  // DOMAIN HELPERS
  // ═══════════════════════════════════════════════════════════════════════════

  static isLeadSuccessful(log) {
    return log.isSuccess === 'true' && log.isDuplicate === 'false' && log.responseBody?.info?.status === 100;
  }

  static verificationSentCheck(log) {
    return log.responseBody?.info?.message?.includes('Verification code sent');
  }

  static async getSuccessRate(source, startDate, endDate) {
    if (!source) throw new Error('source is required for getSuccessRate()');
    const allItems = await this._queryAll(this._sourceParams(source, startDate, endDate));
    const total = allItems.length;
    const successful = allItems.filter(i => i.isSuccess === 'true' && i.isDuplicate === 'false').length;
    return {
      total, successful,
      successRate: total > 0 ? ((successful / total) * 100).toFixed(2) + '%' : '0%',
      indexUsed: 'source-createdAt-index'
    };
  }
}

module.exports = IndiaLendsResponseLog;
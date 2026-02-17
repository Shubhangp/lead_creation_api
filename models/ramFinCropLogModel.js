// models/ramFinCropLog.js
const { docClient } = require('../dynamodb');
const { PutCommand, GetCommand, QueryCommand } = require('@aws-sdk/lib-dynamodb');
const { v4: uuidv4 } = require('uuid');

const TABLE_NAME = 'ramfincrop_logs';
const SOURCES = process.env.RAMFINCROP_SOURCES?.split(',').map(s => s.trim())
  || ['CashKuber', 'FREO', 'BatterySmart', 'Ratecut', 'VFC'];

class RamFinCropLog {

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

    const item = {
      logId: uuidv4(),
      leadId: logData.leadId,
      source: logData.source,
      requestPayload: logData.requestPayload || null,
      responseStatus: logData.responseStatus || null,
      responseBody: logData.responseBody || null,
      createdAt: new Date().toISOString()
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

  static async findByLeadId(leadId, options = {}) {
    if (!leadId) throw new Error('leadId is required');
    const { limit = 100, lastEvaluatedKey } = options;
    const params = {
      TableName: TABLE_NAME,
      IndexName: 'leadId-index',
      KeyConditionExpression: 'leadId = :lid',
      ExpressionAttributeValues: { ':lid': leadId },
      ScanIndexForward: false,
      Limit: limit
    };
    if (lastEvaluatedKey) params.ExclusiveStartKey = lastEvaluatedKey;
    const res = await docClient.send(new QueryCommand(params));
    return { items: res.Items || [], lastEvaluatedKey: res.LastEvaluatedKey };
  }

  // ✅ Now uses source-createdAt-index (ACTIVE)
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
      statusCategoryBreakdown: { '200': 0, '400': 0, 'other': 0 },
      sourceWiseStats: {},
      successRateBySource: {},
      dedupeBreakdown: {},
      messageBreakdown: {},
      successRate: '0%',
      processingTimeMs: 0,
      method: source ? 'query' : 'query-all-sources',
      indexUsed: 'source-createdAt-index'
    };

    allItems.forEach(item => {
      const rs = String(item.responseStatus || 'unknown');
      stats.responseStatusBreakdown[rs] = (stats.responseStatusBreakdown[rs] || 0) + 1;

      const src = item.source || 'unknown';
      stats.sourceBreakdown[src] = (stats.sourceBreakdown[src] || 0) + 1;

      // ✅ Source-wise stats
      if (!stats.sourceWiseStats[src]) {
        stats.sourceWiseStats[src] = {
          totalLogs: 0, success: 0, failed400: 0, other: 0, successRate: '0%'
        };
      }
      stats.sourceWiseStats[src].totalLogs++;

      if (rs === '200') {
        stats.statusCategoryBreakdown['200']++;
        stats.sourceWiseStats[src].success++;
      } else if (rs === '400') {
        stats.statusCategoryBreakdown['400']++;
        stats.sourceWiseStats[src].failed400++;
      } else {
        stats.statusCategoryBreakdown['other']++;
        stats.sourceWiseStats[src].other++;
      }

      if (item.responseBody) {
        let body = item.responseBody;
        if (typeof body === 'string') { try { body = JSON.parse(body); } catch (_) {} }
        const dedupe = body.dedupe || 'unknown';
        stats.dedupeBreakdown[dedupe] = (stats.dedupeBreakdown[dedupe] || 0) + 1;
        if (body.message) {
          stats.messageBreakdown[body.message] = (stats.messageBreakdown[body.message] || 0) + 1;
        }
      }
    });

    // Per-source success rates
    Object.keys(stats.sourceWiseStats).forEach(src => {
      const s = stats.sourceWiseStats[src];
      s.successRate = s.totalLogs > 0
        ? ((s.success / s.totalLogs) * 100).toFixed(2) + '%' : '0%';
      stats.successRateBySource[src] = s.successRate;
    });

    const ok = stats.statusCategoryBreakdown['200'];
    stats.successRate = allItems.length > 0
      ? ((ok / allItems.length) * 100).toFixed(2) + '%' : '0%';
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
          statusCategories: { '200': 0, '400': 0, 'other': 0 },
          sourceBreakdown: {}, bySource: {}
        };
      }

      map[date].total++;
      const rs = String(item.responseStatus || 'unknown');
      map[date].statusBreakdown[rs] = (map[date].statusBreakdown[rs] || 0) + 1;
      map[date].sourceBreakdown[src] = (map[date].sourceBreakdown[src] || 0) + 1;

      if (!map[date].bySource[src]) {
        map[date].bySource[src] = { total: 0, success: 0, failed400: 0, other: 0 };
      }
      map[date].bySource[src].total++;

      if (rs === '200') {
        map[date].statusCategories['200']++;
        map[date].bySource[src].success++;
      } else if (rs === '400') {
        map[date].statusCategories['400']++;
        map[date].bySource[src].failed400++;
      } else {
        map[date].statusCategories['other']++;
        map[date].bySource[src].other++;
      }
    });

    return Object.values(map).sort((a, b) => a.date.localeCompare(b.date));
  }
}

module.exports = RamFinCropLog;
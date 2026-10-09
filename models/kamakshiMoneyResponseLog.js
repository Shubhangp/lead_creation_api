// models/kamakshiMoneyResponseLog.js
// Response log model for the Kamakshi Money (Credzo) Check Attribution API
// (POST /api/v1/lead/can_attribute_utm).
// Mirrors the CreditLinks model layout: writes to `kamakshi_money_response_logs`,
// reads via the `source-createdAt-index` GSI, and exposes the same
// getQuickStats / getStats / getStatsByDate surface the unified stats
// controller (statsType: 'status') expects.
const { docClient } = require('../dynamodb');
const { deepEncryptFields } = require('../utils/piiCrypto');
const { PutCommand, GetCommand, QueryCommand } = require('@aws-sdk/lib-dynamodb');
const { v4: uuidv4 } = require('uuid');

const TABLE_NAME = 'kamakshi_money_response_logs';
const SOURCES = process.env.LEAD_SOURCES?.split(',').map(s => s.trim())
  || (process.env.LEAD_SOURCES?.split(',').map(s => s.trim()))
  || require('../config/registry').LEAD_SOURCES_DEFAULT;

// Kamakshi Money responseStatus values written by sendToKamakshiMoney():
//   ATTRIBUTABLE     — can_attribute = true  (new_customer / customer_no_loan_lead_expired_or_none)
//   NOT_ATTRIBUTABLE — can_attribute = false (already_attributed / recent_login_exists /
//                      repeat_customer_with_loan / customer_has_active_lead)
//   FAILED           — 400 / 401 / 500 / network / unexpected error
// `reason` (the API's decision reason) is stored as a top-level attribute too.
const STATUSES = ['ATTRIBUTABLE', 'NOT_ATTRIBUTABLE', 'FAILED'];

class KamakshiMoneyResponseLog {
  // ─── Query helpers ──────────────────────────────────────────────────────────
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

  // Build source-createdAt-index query params (optionally date-bounded)
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

  static async _fetchAllSources(startDate, endDate, extra = {}) {
    const _perSource = await Promise.all(
      SOURCES.map(src => this._queryAll(this._sourceParams(src, startDate, endDate, extra)))
    );
    return _perSource.flat();
  }

  // ─── Create ─────────────────────────────────────────────────────────────────
  static async create(logData) {
    if (!logData.leadId) throw new Error('leadId is required');
    if (!logData.source) throw new Error('source is required for source-createdAt-index');

    const item = {
      logId: uuidv4(),
      leadId: logData.leadId,
      source: logData.source,
      // Kamakshi-specific extras (all optional / nullable)
      canAttribute: typeof logData.canAttribute === 'boolean' ? logData.canAttribute : null,
      reason: logData.reason || null,
      requestPayload: deepEncryptFields(logData.requestPayload) || null,
      responseStatus: logData.responseStatus || null,
      responseBody: deepEncryptFields(logData.responseBody) || null,
      createdAt: new Date().toISOString()
    };

    await docClient.send(new PutCommand({ TableName: TABLE_NAME, Item: item }));
    return item;
  }

  // ─── Reads ──────────────────────────────────────────────────────────────────
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
  // QUICK STATS — cheap COUNT only (fastest method for showing stats numbers)
  // ═══════════════════════════════════════════════════════════════════════════
  static async getQuickStats(source = null, startDate = null, endDate = null) {
    const t0 = Date.now();

    if (!source) {
      // Count all sources in parallel — pure DynamoDB COUNT, no item payloads
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
        totalLogs,
        sourceBreakdown,
        dateRange: startDate ? { start: startDate, end: endDate } : null,
        scannedInMs: Date.now() - t0,
        method: 'query-count-all-sources',
        indexUsed: 'source-createdAt-index'
      };
    }

    const count = await this._queryCount(this._sourceParams(source, startDate, endDate));
    return {
      totalLogs: count,
      source,
      sourceBreakdown: { [source]: count },
      dateRange: startDate ? { start: startDate, end: endDate } : null,
      scannedInMs: Date.now() - t0,
      method: 'query-count',
      indexUsed: 'source-createdAt-index'
    };
  }

  // ═══════════════════════════════════════════════════════════════════════════
  // FULL STATS — with source-wise + reason breakdown
  // ═══════════════════════════════════════════════════════════════════════════
  static async getStats(source = null, startDate = null, endDate = null) {
    const t0 = Date.now();

    const allItems = source
      ? await this._queryAll(this._sourceParams(source, startDate, endDate))
      : await this._fetchAllSources(startDate, endDate);

    console.log(`[${TABLE_NAME}] getStats: ${allItems.length} items in ${Date.now() - t0}ms`);

    const emptyBuckets = () => ({ ATTRIBUTABLE: 0, NOT_ATTRIBUTABLE: 0, FAILED: 0, other: 0 });
    const stats = {
      totalLogs: allItems.length,
      source: source || 'all',
      dateRange: { start: startDate, end: endDate },
      responseStatusBreakdown: {},
      sourceBreakdown: {},
      statusCategoryBreakdown: emptyBuckets(),
      reasonBreakdown: {},
      sourceWiseStats: {},
      successRateBySource: {},
      messageBreakdown: {},
      attributableCount: 0,
      successRate: '0%',
      processingTimeMs: 0,
      method: source ? 'query' : 'query-all-sources',
      indexUsed: 'source-createdAt-index'
    };

    allItems.forEach(item => {
      const status = this._extractStatus(item);
      const bucket = STATUSES.includes(status) ? status : 'other';
      const src = item.source || 'unknown';

      stats.responseStatusBreakdown[status] = (stats.responseStatusBreakdown[status] || 0) + 1;
      stats.sourceBreakdown[src] = (stats.sourceBreakdown[src] || 0) + 1;
      stats.statusCategoryBreakdown[bucket]++;

      const reason = item.reason || this._extractReason(item);
      if (reason) stats.reasonBreakdown[reason] = (stats.reasonBreakdown[reason] || 0) + 1;

      if (!stats.sourceWiseStats[src]) {
        stats.sourceWiseStats[src] = { totalLogs: 0, attributable: 0, notAttributable: 0, failed: 0, other: 0, successRate: '0%' };
      }
      const sws = stats.sourceWiseStats[src];
      sws.totalLogs++;
      const key = { ATTRIBUTABLE: 'attributable', NOT_ATTRIBUTABLE: 'notAttributable', FAILED: 'failed', other: 'other' }[bucket];
      sws[key]++;

      if (bucket === 'ATTRIBUTABLE') stats.attributableCount++;
      if (bucket === 'FAILED') {
        const msg = this._extractMessage(item);
        if (msg) stats.messageBreakdown[msg] = (stats.messageBreakdown[msg] || 0) + 1;
      }
    });

    Object.keys(stats.sourceWiseStats).forEach(src => {
      const s = stats.sourceWiseStats[src];
      s.successRate = s.totalLogs > 0 ? ((s.attributable / s.totalLogs) * 100).toFixed(2) + '%' : '0%';
      stats.successRateBySource[src] = s.successRate;
    });

    stats.successRate = allItems.length > 0
      ? ((stats.attributableCount / allItems.length) * 100).toFixed(2) + '%' : '0%';
    stats.processingTimeMs = Date.now() - t0;
    return stats;
  }

  // ═══════════════════════════════════════════════════════════════════════════
  // STATS BY DATE — with source-wise breakdown
  // ═══════════════════════════════════════════════════════════════════════════
  static async getStatsByDate(sourceOrStart, startOrEnd, endDate) {
    const t0 = Date.now();

    // Detect call pattern: (startDate, endDate) vs (source, startDate, endDate)
    let source, startDate, actualEndDate;
    if (endDate) {
      source = sourceOrStart; startDate = startOrEnd; actualEndDate = endDate;
    } else {
      source = null; startDate = sourceOrStart; actualEndDate = startOrEnd;
    }

    const PROJECTION = '#src, #ca, responseStatus, responseBody';
    const allItems = source
      ? await this._queryAll(this._sourceParams(source, startDate, actualEndDate, { ScanIndexForward: true, ProjectionExpression: PROJECTION }))
      : await this._fetchAllSources(startDate, actualEndDate, { ProjectionExpression: PROJECTION });

    console.log(`[${TABLE_NAME}] getStatsByDate: ${allItems.length} items in ${Date.now() - t0}ms`);

    const map = {};
    allItems.forEach(item => {
      const date = (item.createdAt || '').split('T')[0];
      const src = item.source || 'unknown';
      const status = this._extractStatus(item);
      const bucket = STATUSES.includes(status) ? status : 'other';

      if (!map[date]) {
        map[date] = {
          date, total: 0, statusBreakdown: {},
          statusCategories: { ATTRIBUTABLE: 0, NOT_ATTRIBUTABLE: 0, FAILED: 0, other: 0 },
          sourceBreakdown: {}, bySource: {}
        };
      }

      map[date].total++;
      map[date].statusBreakdown[status] = (map[date].statusBreakdown[status] || 0) + 1;
      map[date].sourceBreakdown[src] = (map[date].sourceBreakdown[src] || 0) + 1;
      map[date].statusCategories[bucket]++;

      if (!map[date].bySource[src]) {
        map[date].bySource[src] = { total: 0, attributable: 0, notAttributable: 0, failed: 0, other: 0 };
      }
      map[date].bySource[src].total++;
      const key = { ATTRIBUTABLE: 'attributable', NOT_ATTRIBUTABLE: 'notAttributable', FAILED: 'failed', other: 'other' }[bucket];
      map[date].bySource[src][key]++;
    });

    return Object.values(map).sort((a, b) => a.date.localeCompare(b.date));
  }

  // ─── Helpers ────────────────────────────────────────────────────────────────
  static _parseBody(item) {
    let body = item && item.responseBody;
    if (!body) return null;
    if (typeof body === 'string') { try { body = JSON.parse(body); } catch (_) { return null; } }
    return body;
  }

  static _extractStatus(item) {
    if (item.responseStatus) return item.responseStatus;
    const body = this._parseBody(item);
    if (!body) return 'other';
    if (body.can_attribute === true) return 'ATTRIBUTABLE';
    if (body.can_attribute === false) return 'NOT_ATTRIBUTABLE';
    return 'FAILED';
  }

  static _extractReason(item) {
    const body = this._parseBody(item);
    return (body && body.reason) || null;
  }

  static _extractMessage(item) {
    const body = this._parseBody(item);
    if (!body) return null;
    return body.message || body.error?.message || (typeof body.error === 'string' ? body.error : null) || null;
  }
}

module.exports = KamakshiMoneyResponseLog;

const { docClient } = require('../dynamodb');
const { deepEncryptFields } = require('../utils/piiCrypto');
const { PutCommand, GetCommand, QueryCommand } = require('@aws-sdk/lib-dynamodb');
const { v4: uuidv4 } = require('uuid');

const TABLE_NAME = 'rule_mudra_offer_response_logs';
const SOURCES = process.env.LEAD_SOURCES?.split(',').map(s => s.trim())
  || require('../config/registry').LEAD_SOURCES_DEFAULT;

const STATUSES = ['APPROVED', 'REJECTED', 'ERROR', 'SKIPPED'];
const STATUS_KEY = { APPROVED: 'approved', REJECTED: 'rejected', ERROR: 'error', SKIPPED: 'skipped', other: 'other' };

class RuleMudraOfferResponseLog {
  static TABLE_NAME = TABLE_NAME;

  // Query helpers 
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
    if (startDate || endDate) p.ExpressionAttributeNames['#ca'] = 'createdAt';
    if (startDate && endDate) {
      p.KeyConditionExpression += ' AND #ca BETWEEN :s AND :e';
      p.ExpressionAttributeValues[':s'] = startDate;
      p.ExpressionAttributeValues[':e'] = endDate;
    } else if (startDate) {
      p.KeyConditionExpression += ' AND #ca >= :s';
      p.ExpressionAttributeValues[':s'] = startDate;
    } else if (endDate) {
      p.KeyConditionExpression += ' AND #ca <= :e';
      p.ExpressionAttributeValues[':e'] = endDate;
    }
    return p;
  }

  static async _fetchAllSources(startDate, endDate, extra = {}) {
    const perSource = await Promise.all(
      SOURCES.map(src => this._queryAll(this._sourceParams(src, startDate, endDate, extra)))
    );
    return perSource.flat();
  }

  //  Create
  static async create(logData) {
    if (!logData.leadId) throw new Error('leadId is required');
    if (!logData.source) throw new Error('source is required for source-createdAt-index');

    const item = {
      logId: uuidv4(),
      leadId: logData.leadId,
      source: logData.source,
      offerRef: logData.offerRef || null,             // RuleMudra offer_ref — read the decision back with it
      offerAmount: typeof logData.offerAmount === 'number' ? logData.offerAmount : null,
      validUntil: logData.validUntil || null,
      bureauDecision: logData.bureauDecision || null,
      errorCode: logData.errorCode || null,
      httpStatus: logData.httpStatus ?? null,
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
    const params = { ...this._sourceParams(source, startDate, endDate), ScanIndexForward: sortAscending, Limit: limit };
    if (lastEvaluatedKey) params.ExclusiveStartKey = lastEvaluatedKey;
    const res = await docClient.send(new QueryCommand(params));
    return { items: res.Items || [], lastEvaluatedKey: res.LastEvaluatedKey };
  }

  // ─── Quick stats (COUNT only) ───────────────────────────────────────────────
  static async getQuickStats(source = null, startDate = null, endDate = null) {
    const t0 = Date.now();
    const list = source ? [source] : SOURCES;
    const results = await Promise.all(
      list.map(async src => ({ source: src, count: await this._queryCount(this._sourceParams(src, startDate, endDate)) }))
    );
    const sourceBreakdown = {};
    let totalLogs = 0;
    results.forEach(({ source: src, count }) => { sourceBreakdown[src] = count; totalLogs += count; });
    return {
      totalLogs,
      ...(source ? { source } : {}),
      sourceBreakdown,
      dateRange: startDate ? { start: startDate, end: endDate } : null,
      scannedInMs: Date.now() - t0,
      method: source ? 'query-count' : 'query-count-all-sources',
      indexUsed: 'source-createdAt-index'
    };
  }

  // ─── Full stats ─────────────────────────────────────────────────────────────
  static async getStats(source = null, startDate = null, endDate = null) {
    const t0 = Date.now();
    const allItems = source
      ? await this._queryAll(this._sourceParams(source, startDate, endDate))
      : await this._fetchAllSources(startDate, endDate);

    const stats = {
      totalLogs: allItems.length,
      source: source || 'all',
      dateRange: { start: startDate, end: endDate },
      responseStatusBreakdown: {},
      sourceBreakdown: {},
      statusCategoryBreakdown: { APPROVED: 0, REJECTED: 0, ERROR: 0, SKIPPED: 0, other: 0 },
      errorCodeBreakdown: {},
      sourceWiseStats: {},
      successRateBySource: {},
      approvedCount: 0,
      totalOfferAmount: 0,
      avgOfferAmount: 0,
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
      if (item.errorCode) stats.errorCodeBreakdown[item.errorCode] = (stats.errorCodeBreakdown[item.errorCode] || 0) + 1;

      if (!stats.sourceWiseStats[src]) {
        stats.sourceWiseStats[src] = { totalLogs: 0, approved: 0, rejected: 0, error: 0, skipped: 0, other: 0, successRate: '0%' };
      }
      const sws = stats.sourceWiseStats[src];
      sws.totalLogs++;
      sws[STATUS_KEY[bucket]]++;

      if (bucket === 'APPROVED') {
        stats.approvedCount++;
        stats.totalOfferAmount += Number(item.offerAmount) || 0;
      }
    });

    Object.keys(stats.sourceWiseStats).forEach(src => {
      const s = stats.sourceWiseStats[src];
      s.successRate = s.totalLogs > 0 ? ((s.approved / s.totalLogs) * 100).toFixed(2) + '%' : '0%';
      stats.successRateBySource[src] = s.successRate;
    });
    stats.avgOfferAmount = stats.approvedCount ? Math.round(stats.totalOfferAmount / stats.approvedCount) : 0;
    stats.successRate = allItems.length > 0 ? ((stats.approvedCount / allItems.length) * 100).toFixed(2) + '%' : '0%';
    stats.processingTimeMs = Date.now() - t0;
    return stats;
  }

  // ─── Stats by date ──────────────────────────────────────────────────────────
  static async getStatsByDate(sourceOrStart, startOrEnd, endDate) {
    let source, startDate, actualEndDate;
    if (endDate) { source = sourceOrStart; startDate = startOrEnd; actualEndDate = endDate; }
    else { source = null; startDate = sourceOrStart; actualEndDate = startOrEnd; }

    const PROJECTION = '#src, #ca, responseStatus, responseBody';
    const extra = { ProjectionExpression: PROJECTION };
    const withCa = (p) => { p.ExpressionAttributeNames['#ca'] = 'createdAt'; return p; };
    const allItems = source
      ? await this._queryAll(withCa(this._sourceParams(source, startDate, actualEndDate, { ...extra, ScanIndexForward: true })))
      : (await Promise.all(SOURCES.map(src => this._queryAll(withCa(this._sourceParams(src, startDate, actualEndDate, extra)))))).flat();

    const map = {};
    allItems.forEach(item => {
      const date = (item.createdAt || '').split('T')[0];
      const src = item.source || 'unknown';
      const status = this._extractStatus(item);
      const bucket = STATUSES.includes(status) ? status : 'other';
      if (!map[date]) {
        map[date] = {
          date, total: 0, statusBreakdown: {},
          statusCategories: { APPROVED: 0, REJECTED: 0, ERROR: 0, SKIPPED: 0, other: 0 },
          sourceBreakdown: {}, bySource: {}
        };
      }
      const d = map[date];
      d.total++;
      d.statusBreakdown[status] = (d.statusBreakdown[status] || 0) + 1;
      d.sourceBreakdown[src] = (d.sourceBreakdown[src] || 0) + 1;
      d.statusCategories[bucket]++;
      if (!d.bySource[src]) d.bySource[src] = { total: 0, approved: 0, rejected: 0, error: 0, skipped: 0, other: 0 };
      d.bySource[src].total++;
      d.bySource[src][STATUS_KEY[bucket]]++;
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
    if (item && item.responseStatus) return item.responseStatus;
    const body = this._parseBody(item);
    if (!body) return 'other';
    if (body.status === 'APPROVED' || body.status === 'REJECTED') return body.status;
    return 'ERROR';
  }

  static _extractMessage(item) {
    const body = this._parseBody(item);
    if (!body) return null;
    return body.code || body.message || (typeof body.error === 'string' ? body.error : null) || null;
  }
}

module.exports = RuleMudraOfferResponseLog;

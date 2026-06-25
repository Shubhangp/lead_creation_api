// models/creditLinksResponseLog.js
// Response log model for the CreditLinks Partner API integration.
// Mirrors the Zype model layout: writes to `credit_links_response_logs`,
// reads via the `source-createdAt-index` GSI, and exposes the same
// getQuickStats / getStats / getStatsByDate surface the unified stats
// controller (statsType: 'status') expects.
const { docClient } = require('../dynamodb');
const { PutCommand, GetCommand, QueryCommand } = require('@aws-sdk/lib-dynamodb');
const { v4: uuidv4 } = require('uuid');

const TABLE_NAME = 'credit_links_response_logs';
const SOURCES = process.env.LEAD_SOURCES?.split(',').map(s => s.trim())
  || (process.env.LEAD_SOURCES?.split(',').map(s => s.trim()))
  || require('../config/registry').LEAD_SOURCES_DEFAULT;

// CreditLinks responseStatus values written by sendToCreditLinks():
//   LEAD_CREATED   — 201, new lead created
//   ALREADY_EXISTS — 200, lead already created / data updated
//   NOT_ELIGIBLE   — 422, not eligible
//   FAILED         — 400 / network / unexpected error
class CreditLinksResponseLog {
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

  static async _fetchAllSources(startDate, endDate) {
    let allItems = [];
    for (const src of SOURCES) {
      const items = await this._queryAll(this._sourceParams(src, startDate, endDate));
      console.log(`  [${TABLE_NAME}] ${src}: ${items.length} items`);
      allItems = allItems.concat(items);
    }
    return allItems;
  }

  // ─── Create ─────────────────────────────────────────────────────────────────
  static async create(logData) {
    if (!logData.leadId) throw new Error('leadId is required');
    if (!logData.source) throw new Error('source is required for source-createdAt-index');

    const item = {
      logId: uuidv4(),
      leadId: logData.leadId,
      source: logData.source,
      // CreditLinks-specific extras (all optional / nullable)
      creditLinksLeadId: logData.creditLinksLeadId || null,
      offersCount: typeof logData.offersCount === 'number' ? logData.offersCount : null,
      requestPayload: logData.requestPayload || null,
      responseStatus: logData.responseStatus || null,
      responseBody: logData.responseBody || null,
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
  // FULL STATS — with source-wise breakdown
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
      statusCategoryBreakdown: { 'LEAD_CREATED': 0, 'ALREADY_EXISTS': 0, 'NOT_ELIGIBLE': 0, 'FAILED': 0, 'other': 0 },
      sourceWiseStats: {},
      successRateBySource: {},
      offerBreakdown: { withOffer: 0, withoutOffer: 0, totalOffers: 0, averageOffers: 0 },
      messageBreakdown: {},
      leadCreatedCount: 0,
      successRate: '0%',
      processingTimeMs: 0,
      method: source ? 'query' : 'query-all-sources',
      indexUsed: 'source-createdAt-index'
    };

    allItems.forEach(item => {
      const status = this._extractStatus(item);
      const src = item.source || 'unknown';

      stats.responseStatusBreakdown[status] = (stats.responseStatusBreakdown[status] || 0) + 1;
      stats.sourceBreakdown[src] = (stats.sourceBreakdown[src] || 0) + 1;

      if (!stats.sourceWiseStats[src]) {
        stats.sourceWiseStats[src] = {
          totalLogs: 0, leadCreated: 0, alreadyExists: 0, notEligible: 0, failed: 0, other: 0,
          withOffer: 0, totalOffers: 0, successRate: '0%'
        };
      }
      const sws = stats.sourceWiseStats[src];
      sws.totalLogs++;

      // Offer enrichment (offersCount written by sendToCreditLinks after get-offers)
      const offers = typeof item.offersCount === 'number' ? item.offersCount : null;
      if (offers && offers > 0) {
        stats.offerBreakdown.withOffer++;
        stats.offerBreakdown.totalOffers += offers;
        sws.withOffer++;
        sws.totalOffers += offers;
      }

      switch (status) {
        case 'LEAD_CREATED':
          stats.statusCategoryBreakdown['LEAD_CREATED']++;
          stats.leadCreatedCount++;
          sws.leadCreated++;
          break;
        case 'ALREADY_EXISTS':
          stats.statusCategoryBreakdown['ALREADY_EXISTS']++;
          sws.alreadyExists++;
          break;
        case 'NOT_ELIGIBLE':
          stats.statusCategoryBreakdown['NOT_ELIGIBLE']++;
          sws.notEligible++;
          break;
        case 'FAILED':
          stats.statusCategoryBreakdown['FAILED']++;
          sws.failed++;
          break;
        default:
          stats.statusCategoryBreakdown['other']++;
          sws.other++;
      }

      // Track failure / not-eligible messages for diagnostics
      if (status === 'FAILED' || status === 'NOT_ELIGIBLE') {
        const msg = this._extractMessage(item);
        if (msg) stats.messageBreakdown[msg] = (stats.messageBreakdown[msg] || 0) + 1;
      }
    });

    // Offer aggregates — "success" counts a created lead (with or without offers)
    const created = stats.statusCategoryBreakdown['LEAD_CREATED'] + stats.statusCategoryBreakdown['ALREADY_EXISTS'];
    stats.offerBreakdown.withoutOffer = created - stats.offerBreakdown.withOffer;
    if (stats.offerBreakdown.withOffer > 0) {
      stats.offerBreakdown.averageOffers = Math.round(
        (stats.offerBreakdown.totalOffers / stats.offerBreakdown.withOffer) * 100
      ) / 100;
    }

    Object.keys(stats.sourceWiseStats).forEach(src => {
      const s = stats.sourceWiseStats[src];
      const ok = s.leadCreated + s.alreadyExists;
      s.successRate = s.totalLogs > 0 ? ((ok / s.totalLogs) * 100).toFixed(2) + '%' : '0%';
      if (s.withOffer > 0) s.averageOffers = Math.round((s.totalOffers / s.withOffer) * 100) / 100;
      stats.successRateBySource[src] = s.successRate;
    });

    stats.successRate = allItems.length > 0
      ? ((created / allItems.length) * 100).toFixed(2) + '%' : '0%';
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

    const allItems = source
      ? await this._queryAll(this._sourceParams(source, startDate, actualEndDate, { ScanIndexForward: true }))
      : await this._fetchAllSources(startDate, actualEndDate);

    console.log(`[${TABLE_NAME}] getStatsByDate: ${allItems.length} items in ${Date.now() - t0}ms`);

    const map = {};
    allItems.forEach(item => {
      const date = (item.createdAt || '').split('T')[0];
      const src = item.source || 'unknown';
      const status = this._extractStatus(item);

      if (!map[date]) {
        map[date] = {
          date, total: 0, statusBreakdown: {},
          statusCategories: { 'LEAD_CREATED': 0, 'ALREADY_EXISTS': 0, 'NOT_ELIGIBLE': 0, 'FAILED': 0, 'other': 0 },
          sourceBreakdown: {}, bySource: {}
        };
      }

      map[date].total++;
      map[date].statusBreakdown[status] = (map[date].statusBreakdown[status] || 0) + 1;
      map[date].sourceBreakdown[src] = (map[date].sourceBreakdown[src] || 0) + 1;

      if (!map[date].bySource[src]) {
        map[date].bySource[src] = { total: 0, leadCreated: 0, alreadyExists: 0, notEligible: 0, failed: 0, other: 0 };
      }
      map[date].bySource[src].total++;

      const bucket = ['LEAD_CREATED', 'ALREADY_EXISTS', 'NOT_ELIGIBLE', 'FAILED'].includes(status)
        ? status : 'other';
      map[date].statusCategories[bucket]++;
      const key = { LEAD_CREATED: 'leadCreated', ALREADY_EXISTS: 'alreadyExists', NOT_ELIGIBLE: 'notEligible', FAILED: 'failed', other: 'other' }[bucket];
      map[date].bySource[src][key]++;
    });

    return Object.values(map).sort((a, b) => a.date.localeCompare(b.date));
  }

  // ─── Helpers ────────────────────────────────────────────────────────────────
  static _extractStatus(item) {
    if (item.responseStatus) return item.responseStatus;
    let body = item.responseBody;
    if (!body) return 'other';
    if (typeof body === 'string') { try { body = JSON.parse(body); } catch (_) { return 'other'; } }
    if (body.leadId) return 'LEAD_CREATED';
    if (String(body.success) === 'false') return 'FAILED';
    return 'other';
  }

  static _extractMessage(item) {
    let body = item.responseBody;
    if (!body) return null;
    if (typeof body === 'string') { try { body = JSON.parse(body); } catch (_) { return null; } }
    return body.message || (body.error && (body.error.message || body.error)) || null;
  }
}

module.exports = CreditLinksResponseLog;

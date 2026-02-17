const { docClient } = require('../dynamodb');
const { PutCommand, GetCommand, QueryCommand, UpdateCommand } = require('@aws-sdk/lib-dynamodb');
const { v4: uuidv4 } = require('uuid');

const TABLE_NAME = 'fatakpay_response_logs';

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

  // ─── write ────────────────────────────────────────────────────────────────

  static async create(logData) {
    if (!logData.leadId) throw new Error('leadId is required');
    if (!logData.source) throw new Error('source is required (needed once source-createdAt-index becomes ACTIVE)');

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

  // ─── reads ────────────────────────────────────────────────────────────────

  static async findById(logId) {
    const res = await docClient.send(new GetCommand({ TableName: TABLE_NAME, Key: { logId } }));
    return res.Item || null;
  }

  // Uses leadId-index (ACTIVE ✅)
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

  // ⚠️  source-createdAt-index is CREATING – will throw a clear error instead of
  //     silently falling back to a full table scan.
  static async findBySource(source, options = {}) {
    if (!source) throw new Error('source is required');
    throw new Error(
      `[${TABLE_NAME}] source-createdAt-index is still CREATING. ` +
      `Query by leadId instead, or wait until the index status becomes ACTIVE.`
    );
  }

  // findAll replaced: requires leadId to stay scan-free
  static async findAll() {
    throw new Error(
      `[${TABLE_NAME}] findAll() removed – no full-table index available. ` +
      `Use findByLeadId(leadId) instead.`
    );
  }

  // findByDateRange replaced: no usable date index yet
  static async findByDateRange() {
    throw new Error(
      `[${TABLE_NAME}] findByDateRange() requires source-createdAt-index which is CREATING. ` +
      `Use findByLeadId(leadId) instead.`
    );
  }

  // ─── domain helpers ───────────────────────────────────────────────────────

  static isPermanentBlock(responseBody) {
    if (!responseBody?.message) return false;
    const msg = responseBody.message.trim().toLowerCase();
    return PERMANENT_BLOCKS.some(b => b.toLowerCase() === msg);
  }

  // Get permanent blocks for a specific lead
  static async getPermanentBlocksByLeadId(leadId) {
    if (!leadId) throw new Error('leadId is required');
    const { items } = await this.findByLeadId(leadId, { limit: 1000 });
    return items.filter(item => this.isPermanentBlock(item.responseBody));
  }

  // ─── stats ────────────────────────────────────────────────────────────────

  // getQuickStats: count by leadId (source-createdAt unavailable)
  static async getQuickStats(leadId, startDate = null, endDate = null) {
    if (!leadId) throw new Error(
      `[${TABLE_NAME}] leadId is required for getQuickStats(). ` +
      `source-createdAt-index is still CREATING.`
    );

    const t0 = Date.now();
    const params = {
      TableName: TABLE_NAME,
      IndexName: 'leadId-index',
      KeyConditionExpression: 'leadId = :lid',
      ExpressionAttributeValues: { ':lid': leadId }
    };

    // post-filter by date client-side (no sort-key on leadId-index)
    const items = await this._queryAll(params);
    const filtered = this._filterByDate(items, startDate, endDate);

    return {
      totalLogs: filtered.length,
      leadId,
      dateRange: startDate ? { start: startDate, end: endDate } : null,
      scannedInMs: Date.now() - t0,
      method: 'query-leadId-index',
      indexUsed: 'leadId-index',
      note: 'source-createdAt-index is CREATING; switch to it once ACTIVE for cheaper counts'
    };
  }

  // getStats: full stats by leadId
  static async getStats(leadId, startDate = null, endDate = null) {
    if (!leadId) throw new Error(
      `[${TABLE_NAME}] leadId is required for getStats(). ` +
      `source-createdAt-index is still CREATING.`
    );

    const t0 = Date.now();
    const params = {
      TableName: TABLE_NAME,
      IndexName: 'leadId-index',
      KeyConditionExpression: 'leadId = :lid',
      ExpressionAttributeValues: { ':lid': leadId },
      ScanIndexForward: false
    };

    const allItems = this._filterByDate(await this._queryAll(params), startDate, endDate);
    console.log(`[${TABLE_NAME}] getStats query done: ${allItems.length} items in ${Date.now() - t0}ms`);

    const stats = {
      totalLogs: allItems.length,
      leadId,
      dateRange: { start: startDate, end: endDate },
      responseStatusBreakdown: {},
      messageBreakdown: {},
      permanentBlocks: { total: 0, byMessage: {}, percentage: '0%' },
      sourceBreakdown: {},
      eligibilityStats: { eligible: 0, notEligible: 0, ageNotEligible: 0 },
      statusCategoryBreakdown: { 'ACCEPT': 0, 'REJECTED': 0, 'Failed': 0, 'other': 0 },
      offerBreakdown: { withOffer: 0, withoutOffer: 0, totalOfferAmount: 0, averageOffer: 0 },
      acceptanceRate: '0%',
      processingTimeMs: 0,
      method: 'query-leadId-index',
      indexUsed: 'leadId-index',
      note: 'source-createdAt-index is CREATING; switch once ACTIVE for cheaper cross-lead stats'
    };

    allItems.forEach(item => {
      const st = item.responseStatus || 'unknown';
      stats.responseStatusBreakdown[st] = (stats.responseStatusBreakdown[st] || 0) + 1;

      const src = item.source || 'unknown';
      stats.sourceBreakdown[src] = (stats.sourceBreakdown[src] || 0) + 1;

      if (item.responseBody) {
        let body = item.responseBody;
        if (typeof body === 'string') { try { body = JSON.parse(body); } catch (_) {} }

        if (body.message) {
          const msg = body.message.trim();
          stats.messageBreakdown[msg] = (stats.messageBreakdown[msg] || 0) + 1;
          if (this.isPermanentBlock(body)) {
            stats.permanentBlocks.total++;
            stats.permanentBlocks.byMessage[msg] = (stats.permanentBlocks.byMessage[msg] || 0) + 1;
          }
          const low = msg.toLowerCase();
          if (low.includes('you are eligible')) stats.eligibilityStats.eligible++;
          else if (low.includes('not eligible due to age')) stats.eligibilityStats.ageNotEligible++;
          else if (low.includes('not eligible')) stats.eligibilityStats.notEligible++;
        }

        const bodyStatus = body.status || 'unknown';
        if (bodyStatus === 'ACCEPT') {
          stats.statusCategoryBreakdown['ACCEPT']++;
          if (body.offer) {
            stats.offerBreakdown.withOffer++;
            stats.offerBreakdown.totalOfferAmount += parseInt(body.offer) || 0;
          }
        } else if (bodyStatus === 'REJECTED') {
          stats.statusCategoryBreakdown['REJECTED']++;
        } else if (bodyStatus === 'Failed') {
          stats.statusCategoryBreakdown['Failed']++;
        } else {
          stats.statusCategoryBreakdown['other']++;
        }
      }
    });

    const accept = stats.statusCategoryBreakdown['ACCEPT'];
    if (stats.offerBreakdown.withOffer > 0) {
      stats.offerBreakdown.averageOffer = Math.round(stats.offerBreakdown.totalOfferAmount / stats.offerBreakdown.withOffer);
    }
    stats.offerBreakdown.withoutOffer = accept - stats.offerBreakdown.withOffer;
    stats.acceptanceRate = allItems.length > 0 ? ((accept / allItems.length) * 100).toFixed(2) + '%' : '0%';
    if (allItems.length > 0) {
      stats.permanentBlocks.percentage = ((stats.permanentBlocks.total / allItems.length) * 100).toFixed(2) + '%';
    }
    stats.processingTimeMs = Date.now() - t0;
    return stats;
  }

  // getStatsByDate: group by date for a specific leadId
  static async getStatsByDate(leadId, startDate, endDate) {
    if (!leadId) throw new Error(`[${TABLE_NAME}] leadId is required for getStatsByDate()`);

    const t0 = Date.now();
    const allItems = this._filterByDate(
      await this._queryAll({
        TableName: TABLE_NAME,
        IndexName: 'leadId-index',
        KeyConditionExpression: 'leadId = :lid',
        ExpressionAttributeValues: { ':lid': leadId },
        ScanIndexForward: true
      }),
      startDate, endDate
    );

    console.log(`[${TABLE_NAME}] getStatsByDate: ${allItems.length} items in ${Date.now() - t0}ms`);
    return this._groupByDate(allItems);
  }

  // ─── private ──────────────────────────────────────────────────────────────

  static _filterByDate(items, startDate, endDate) {
    if (!startDate && !endDate) return items;
    return items.filter(item => {
      const ca = item.createdAt;
      if (startDate && ca < startDate) return false;
      if (endDate && ca > endDate) return false;
      return true;
    });
  }

  static _groupByDate(items) {
    const map = {};
    items.forEach(item => {
      const date = item.createdAt.split('T')[0];
      if (!map[date]) {
        map[date] = {
          date, total: 0, statusBreakdown: {},
          messageBreakdown: {}, permanentBlocks: 0, eligible: 0, notEligible: 0,
          statusCategories: { 'ACCEPT': 0, 'REJECTED': 0, 'Failed': 0, 'other': 0 }
        };
      }
      map[date].total++;
      const st = item.responseStatus || 'unknown';
      map[date].statusBreakdown[st] = (map[date].statusBreakdown[st] || 0) + 1;

      if (item.responseBody) {
        let body = item.responseBody;
        if (typeof body === 'string') { try { body = JSON.parse(body); } catch (_) {} }
        if (body.message) {
          const msg = body.message.trim();
          map[date].messageBreakdown[msg] = (map[date].messageBreakdown[msg] || 0) + 1;
          if (this.isPermanentBlock(body)) map[date].permanentBlocks++;
          const low = msg.toLowerCase();
          if (low.includes('you are eligible')) map[date].eligible++;
          else if (low.includes('not eligible')) map[date].notEligible++;
        }
        const bs = body.status || 'unknown';
        if (['ACCEPT','REJECTED','Failed'].includes(bs)) map[date].statusCategories[bs]++;
        else map[date].statusCategories['other']++;
      }
    });
    return Object.values(map).sort((a, b) => a.date.localeCompare(b.date));
  }
}

module.exports = FatakPayResponseLog;
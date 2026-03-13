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

// Known sources — used for parallel queries once source-createdAt-index is ACTIVE.
// Override via env: FATAKPAY_SOURCES=CashKuber,FREO,BatterySmart,Ratecut,VFC
const SOURCES = ( 'CashKuber,FREO,BatterySmart,Ratecut,VFC')
  .split(',').map(s => s.trim()).filter(Boolean);

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
    if (!logData.source) throw new Error('source is required');

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

  // Internal: query one source via source-createdAt-index
  static async _queryBySource(source, startDate, endDate) {
    return this._queryAll({
      TableName: TABLE_NAME,
      IndexName: 'source-createdAt-index',
      KeyConditionExpression: '#src = :src AND createdAt BETWEEN :start AND :end',
      ExpressionAttributeNames: { '#src': 'source' },
      ExpressionAttributeValues: { ':src': source, ':start': startDate, ':end': endDate },
      ScanIndexForward: true
    });
  }

  // ─── domain helpers ───────────────────────────────────────────────────────

  static isPermanentBlock(responseBody) {
    if (!responseBody?.message) return false;
    const msg = responseBody.message.trim().toLowerCase();
    return PERMANENT_BLOCKS.some(b => b.toLowerCase() === msg);
  }

  static async getPermanentBlocksByLeadId(leadId) {
    if (!leadId) throw new Error('leadId is required');
    const { items } = await this.findByLeadId(leadId, { limit: 1000 });
    return items.filter(item => this.isPermanentBlock(item.responseBody));
  }

  // ─── stats ────────────────────────────────────────────────────────────────

  /**
   * getQuickStats(leadId?, startDate?, endDate?)
   *
   * Controller calls: Model.getQuickStats(null, startDate, endDate)
   * - When leadId is null and dates are provided: query source-createdAt-index
   *   for each source in parallel. Returns _indexError if index is CREATING.
   * - When leadId is provided: use leadId-index (legacy path).
   */
  static async getQuickStats(leadId, startDate = null, endDate = null) {
    if (leadId) return this._getQuickStatsByLeadId(leadId, startDate, endDate);

    if (!startDate || !endDate) {
      throw new Error(`[${TABLE_NAME}] startDate and endDate are required when no leadId is provided`);
    }

    const t0 = Date.now();
    try {
      const counts = await Promise.all(
        SOURCES.map(src =>
          this._queryCount({
            TableName: TABLE_NAME,
            IndexName: 'source-createdAt-index',
            KeyConditionExpression: '#src = :src AND createdAt BETWEEN :start AND :end',
            ExpressionAttributeNames: { '#src': 'source' },
            ExpressionAttributeValues: { ':src': src, ':start': startDate, ':end': endDate }
          }).then(n => ({ source: src, count: n }))
        )
      );
      const totalLogs = counts.reduce((s, r) => s + r.count, 0);
      const sourceBreakdown = {};
      counts.forEach(r => { if (r.count > 0) sourceBreakdown[r.source] = r.count; });
      return { totalLogs, sourceBreakdown, dateRange: { start: startDate, end: endDate }, scannedInMs: Date.now() - t0, indexUsed: 'source-createdAt-index' };
    } catch (err) {
      return { totalLogs: 0, sourceBreakdown: {}, _indexError: true, _errorNote: `source-createdAt-index is not yet ACTIVE: ${err.message}`, scannedInMs: Date.now() - t0 };
    }
  }

  static async _getQuickStatsByLeadId(leadId, startDate, endDate) {
    const t0 = Date.now();
    const items = await this._queryAll({ TableName: TABLE_NAME, IndexName: 'leadId-index', KeyConditionExpression: 'leadId = :lid', ExpressionAttributeValues: { ':lid': leadId } });
    const filtered = this._filterByDate(items, startDate, endDate);
    return { totalLogs: filtered.length, leadId, scannedInMs: Date.now() - t0, indexUsed: 'leadId-index' };
  }

  /**
   * getStats(startDate, endDate)    ← 2-arg call from controller
   * getStats(leadId, start, end)    ← 3-arg legacy call (backward compat)
   *
   * When called with dates only: queries source-createdAt-index for all SOURCES in
   * parallel and aggregates results. If index is CREATING, returns structured error
   * object (no exception thrown) so controller can return 200 with error note.
   */
  static async getStats(startDateOrLeadId, endDateOrStart = null, endDate = null) {
    const isLegacy = endDate !== null;
    if (isLegacy) return this._getStatsByLeadId(startDateOrLeadId, endDateOrStart, endDate);

    const startDate = startDateOrLeadId;
    const end = endDateOrStart;

    if (!startDate || !end) throw new Error(`[${TABLE_NAME}] startDate and endDate are required`);

    const t0 = Date.now();
    try {
      const allItems = (await Promise.all(SOURCES.map(src => this._queryBySource(src, startDate, end)))).flat();
      console.log(`[${TABLE_NAME}] getStats: ${allItems.length} items in ${Date.now() - t0}ms`);
      return this._buildStats(allItems, { start: startDate, end }, Date.now() - t0, 'source-createdAt-index');
    } catch (err) {
      console.warn(`[${TABLE_NAME}] getStats fallback — source-createdAt-index not ACTIVE: ${err.message}`);
      return {
        totalLogs: 0, dateRange: { start: startDate, end },
        _indexError: true,
        _errorNote: `source-createdAt-index is not yet ACTIVE in DynamoDB. Once it becomes ACTIVE, full stats will work automatically. Error: ${err.message}`,
        responseStatusBreakdown: {}, messageBreakdown: {},
        permanentBlocks: { total: 0, byMessage: {}, percentage: '0%' },
        sourceBreakdown: {},
        eligibilityStats: { eligible: 0, notEligible: 0, ageNotEligible: 0 },
        statusCategoryBreakdown: { ACCEPT: 0, REJECTED: 0, Failed: 0, other: 0 },
        offerBreakdown: { withOffer: 0, withoutOffer: 0, totalOfferAmount: 0, averageOffer: 0 },
        acceptanceRate: '0%'
      };
    }
  }

  static async _getStatsByLeadId(leadId, startDate, endDate) {
    if (!leadId) throw new Error(`[${TABLE_NAME}] leadId is required`);
    const t0 = Date.now();
    const allItems = this._filterByDate(
      await this._queryAll({ TableName: TABLE_NAME, IndexName: 'leadId-index', KeyConditionExpression: 'leadId = :lid', ExpressionAttributeValues: { ':lid': leadId }, ScanIndexForward: false }),
      startDate, endDate
    );
    return this._buildStats(allItems, { start: startDate, end: endDate }, Date.now() - t0, 'leadId-index');
  }

  static _buildStats(allItems, dateRange, processingTimeMs, indexUsed) {
    const stats = {
      totalLogs: allItems.length, dateRange,
      responseStatusBreakdown: {}, messageBreakdown: {},
      permanentBlocks: { total: 0, byMessage: {}, percentage: '0%' },
      sourceBreakdown: {},
      eligibilityStats: { eligible: 0, notEligible: 0, ageNotEligible: 0 },
      statusCategoryBreakdown: { ACCEPT: 0, REJECTED: 0, Failed: 0, other: 0 },
      offerBreakdown: { withOffer: 0, withoutOffer: 0, totalOfferAmount: 0, averageOffer: 0 },
      acceptanceRate: '0%', processingTimeMs, indexUsed
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
          if (body.offer) { stats.offerBreakdown.withOffer++; stats.offerBreakdown.totalOfferAmount += parseInt(body.offer) || 0; }
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
    if (stats.offerBreakdown.withOffer > 0) stats.offerBreakdown.averageOffer = Math.round(stats.offerBreakdown.totalOfferAmount / stats.offerBreakdown.withOffer);
    stats.offerBreakdown.withoutOffer = accept - stats.offerBreakdown.withOffer;
    stats.acceptanceRate = allItems.length > 0 ? ((accept / allItems.length) * 100).toFixed(2) + '%' : '0%';
    if (allItems.length > 0) stats.permanentBlocks.percentage = ((stats.permanentBlocks.total / allItems.length) * 100).toFixed(2) + '%';
    return stats;
  }

  /**
   * getStatsByDate(startDate, endDate)    ← 2-arg call from controller
   * getStatsByDate(leadId, start, end)    ← 3-arg legacy call
   */
  static async getStatsByDate(startDateOrLeadId, endDateOrStart = null, endDate = null) {
    const isLegacy = endDate !== null;
    if (isLegacy) return this._getStatsByDateForLeadId(startDateOrLeadId, endDateOrStart, endDate);

    const startDate = startDateOrLeadId;
    const end = endDateOrStart;

    if (!startDate || !end) throw new Error(`[${TABLE_NAME}] startDate and endDate are required`);

    try {
      const allItems = (await Promise.all(SOURCES.map(src => this._queryBySource(src, startDate, end)))).flat();
      console.log(`[${TABLE_NAME}] getStatsByDate: ${allItems.length} items`);
      return this._groupByDate(allItems);
    } catch (err) {
      console.warn(`[${TABLE_NAME}] getStatsByDate — source-createdAt-index not ACTIVE`);
      return [];
    }
  }

  static async _getStatsByDateForLeadId(leadId, startDate, endDate) {
    if (!leadId) throw new Error(`[${TABLE_NAME}] leadId is required`);
    const allItems = this._filterByDate(
      await this._queryAll({ TableName: TABLE_NAME, IndexName: 'leadId-index', KeyConditionExpression: 'leadId = :lid', ExpressionAttributeValues: { ':lid': leadId }, ScanIndexForward: true }),
      startDate, endDate
    );
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
        map[date] = { date, total: 0, statusBreakdown: {}, messageBreakdown: {}, permanentBlocks: 0, eligible: 0, notEligible: 0, statusCategories: { ACCEPT: 0, REJECTED: 0, Failed: 0, other: 0 } };
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
        if (['ACCEPT', 'REJECTED', 'Failed'].includes(bs)) map[date].statusCategories[bs]++;
        else map[date].statusCategories['other']++;
      }
    });
    return Object.values(map).sort((a, b) => a.date.localeCompare(b.date));
  }

  // ─── update ───────────────────────────────────────────────────────────────

  static async update(logId, updates) {
    const names = {}, values = {}, parts = [];
    Object.entries(updates).forEach(([key, val], i) => {
      names[`#f${i}`] = key; values[`:v${i}`] = val; parts.push(`#f${i} = :v${i}`);
    });
    const res = await docClient.send(new UpdateCommand({ TableName: TABLE_NAME, Key: { logId }, UpdateExpression: `SET ${parts.join(', ')}`, ExpressionAttributeNames: names, ExpressionAttributeValues: values, ReturnValues: 'ALL_NEW' }));
    return res.Attributes;
  }

  static async findAll() { throw new Error(`[${TABLE_NAME}] findAll() removed – use getStats(startDate, endDate) instead.`); }
  static async findByDateRange() { throw new Error(`[${TABLE_NAME}] findByDateRange() removed – use getStats(startDate, endDate) instead.`); }
}

module.exports = FatakPayResponseLog;
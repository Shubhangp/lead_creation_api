const { docClient } = require('../dynamodb');
const { PutCommand, GetCommand, QueryCommand, UpdateCommand, DeleteCommand } = require('@aws-sdk/lib-dynamodb');
const { v4: uuidv4 } = require('uuid');

const TABLE_NAME = 'rcs_queue';

class RCSQueue {

  // ─── helpers ──────────────────────────────────────────────────────────────

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

  static async create(queueData) {
    if (!queueData.leadId) throw new Error('leadId is required');
    if (!queueData.status) throw new Error('status is required for status-index queries');

    const item = {
      rcs_queue: uuidv4(),
      leadId: queueData.leadId,
      phone: queueData.phone,
      rcsType: queueData.rcsType,
      lenderName: queueData.lenderName || null,
      priority: queueData.priority || null,
      scheduledTime: queueData.scheduledTime instanceof Date
        ? queueData.scheduledTime.toISOString()
        : queueData.scheduledTime,
      status: queueData.status || 'PENDING',
      attempts: String(queueData.attempts || 0),
      sentAt: queueData.sentAt || null,
      failureReason: queueData.failureReason || null,
      rcsPayload: queueData.rcsPayload || null,
      rcsResponse: queueData.rcsResponse || null,
      createdAt: new Date().toISOString(),
      updatedAt: new Date().toISOString()
    };

    await docClient.send(new PutCommand({ TableName: TABLE_NAME, Item: item }));
    return item;
  }

  // ─── reads ────────────────────────────────────────────────────────────────

  static async findById(rcs_queue) {
    const res = await docClient.send(new GetCommand({ TableName: TABLE_NAME, Key: { rcs_queue } }));
    return res.Item || null;
  }

  // Uses leadId-rcsType-index (ACTIVE ✅)
  static async findByLeadId(leadId, options = {}) {
    if (!leadId) throw new Error('leadId is required');
    const { limit = 100, rcsType, lastEvaluatedKey } = options;

    const params = {
      TableName: TABLE_NAME,
      IndexName: 'leadId-rcsType-index',
      KeyConditionExpression: 'leadId = :lid',
      ExpressionAttributeValues: { ':lid': leadId },
      ScanIndexForward: false,
      Limit: limit
    };

    if (rcsType) {
      params.KeyConditionExpression += ' AND rcsType = :rt';
      params.ExpressionAttributeValues[':rt'] = rcsType;
    }

    if (lastEvaluatedKey) params.ExclusiveStartKey = lastEvaluatedKey;

    const res = await docClient.send(new QueryCommand(params));
    return { items: res.Items || [], lastEvaluatedKey: res.LastEvaluatedKey };
  }

  // Uses status-scheduledTime-index (ACTIVE ✅) – primary queue processing method
  static async findByStatusAndScheduledTime(status, scheduledTimeBefore, options = {}) {
    if (!status) throw new Error('status is required');
    const { limit = 100, lastEvaluatedKey } = options;

    const scheduledTime = scheduledTimeBefore instanceof Date
      ? scheduledTimeBefore.toISOString()
      : scheduledTimeBefore;

    const params = {
      TableName: TABLE_NAME,
      IndexName: 'status-scheduledTime-index',
      KeyConditionExpression: '#st = :st AND scheduledTime <= :sched',
      ExpressionAttributeNames: { '#st': 'status' },
      ExpressionAttributeValues: { ':st': status, ':sched': scheduledTime },
      Limit: limit
    };

    if (lastEvaluatedKey) params.ExclusiveStartKey = lastEvaluatedKey;

    const res = await docClient.send(new QueryCommand(params));
    return { items: res.Items || [], lastEvaluatedKey: res.LastEvaluatedKey };
  }

  // Uses status-scheduledTime-index for all pending in a window
  static async findPendingInWindow(scheduledTimeFrom, scheduledTimeTo, options = {}) {
    if (!scheduledTimeFrom || !scheduledTimeTo) throw new Error('scheduledTimeFrom and scheduledTimeTo are required');
    const { limit = 500, lastEvaluatedKey } = options;

    const params = {
      TableName: TABLE_NAME,
      IndexName: 'status-scheduledTime-index',
      KeyConditionExpression: '#st = :st AND scheduledTime BETWEEN :from AND :to',
      ExpressionAttributeNames: { '#st': 'status' },
      ExpressionAttributeValues: { ':st': 'PENDING', ':from': scheduledTimeFrom, ':to': scheduledTimeTo },
      ScanIndexForward: true,
      Limit: limit
    };

    if (lastEvaluatedKey) params.ExclusiveStartKey = lastEvaluatedKey;

    const res = await docClient.send(new QueryCommand(params));
    return { items: res.Items || [], lastEvaluatedKey: res.LastEvaluatedKey };
  }

  // Uses status-attempts-index (ACTIVE ✅) – find items that need retry
  static async findByStatusAndAttempts(status, maxAttempts, options = {}) {
    if (!status) throw new Error('status is required');
    const { limit = 100, lastEvaluatedKey } = options;

    // Query status + filter attempts client-side (attempts is sort key so range query works)
    const params = {
      TableName: TABLE_NAME,
      IndexName: 'status-attempts-index',
      KeyConditionExpression: '#st = :st AND attempts < :max',
      ExpressionAttributeNames: { '#st': 'status' },
      ExpressionAttributeValues: { ':st': status, ':max': String(maxAttempts) },
      Limit: limit
    };

    if (lastEvaluatedKey) params.ExclusiveStartKey = lastEvaluatedKey;

    const res = await docClient.send(new QueryCommand(params));
    return { items: res.Items || [], lastEvaluatedKey: res.LastEvaluatedKey };
  }

  static async findAll() {
    throw new Error(`[${TABLE_NAME}] findAll() removed – use findByStatus() or findByLeadId() to stay scan-free.`);
  }

  // ─── counts ───────────────────────────────────────────────────────────────

  // Uses status-scheduledTime-index COUNT (cheap)
  static async countByStatus(status) {
    if (!status) throw new Error('status is required');
    return this._queryCount({
      TableName: TABLE_NAME,
      IndexName: 'status-scheduledTime-index',
      KeyConditionExpression: '#st = :st',
      ExpressionAttributeNames: { '#st': 'status' },
      ExpressionAttributeValues: { ':st': status }
    });
  }

  // Aggregate stats across all statuses (4 cheap COUNT queries in parallel)
  static async getAggregateStats() {
    const statuses = ['PENDING', 'SENT', 'FAILED', 'CANCELLED'];
    const counts = await Promise.all(statuses.map(s => this.countByStatus(s)));
    return statuses
      .map((s, i) => ({ _id: s, count: counts[i] }))
      .filter(r => r.count > 0);
  }

  // ─── stats ────────────────────────────────────────────────────────────────

  // getQuickStats by status (no source available in this table)
  static async getQuickStats(status) {
    if (!status) throw new Error(
      `[${TABLE_NAME}] status is required for getQuickStats(). ` +
      `source-createdAt-index exists but has 0 items (items have no source field).`
    );

    const t0 = Date.now();
    const count = await this.countByStatus(status);

    return {
      totalItems: count,
      status,
      scannedInMs: Date.now() - t0,
      method: 'query-count',
      indexUsed: 'status-scheduledTime-index'
    };
  }

  // getStats: full breakdown by status using status-scheduledTime-index
  static async getStats(status) {
    if (!status) throw new Error(
      `[${TABLE_NAME}] status is required for getStats(). ` +
      `Use one of: PENDING, SENT, FAILED, CANCELLED`
    );

    const t0 = Date.now();
    const allItems = await this._queryAll({
      TableName: TABLE_NAME,
      IndexName: 'status-scheduledTime-index',
      KeyConditionExpression: '#st = :st',
      ExpressionAttributeNames: { '#st': 'status' },
      ExpressionAttributeValues: { ':st': status },
      ScanIndexForward: false
    });

    console.log(`[${TABLE_NAME}] getStats(${status}): ${allItems.length} items in ${Date.now() - t0}ms`);

    const stats = {
      totalItems: allItems.length,
      status,
      rcsTypeBreakdown: {},
      attemptsBreakdown: {},
      lenderBreakdown: {},
      failureReasonBreakdown: {},
      processingTimeMs: 0,
      method: 'query',
      indexUsed: 'status-scheduledTime-index'
    };

    allItems.forEach(item => {
      const rt = item.rcsType || 'unknown';
      stats.rcsTypeBreakdown[rt] = (stats.rcsTypeBreakdown[rt] || 0) + 1;

      const att = String(item.attempts || '0');
      stats.attemptsBreakdown[att] = (stats.attemptsBreakdown[att] || 0) + 1;

      const lender = item.lenderName || 'unknown';
      stats.lenderBreakdown[lender] = (stats.lenderBreakdown[lender] || 0) + 1;

      if (item.failureReason) {
        stats.failureReasonBreakdown[item.failureReason] = (stats.failureReasonBreakdown[item.failureReason] || 0) + 1;
      }
    });

    stats.processingTimeMs = Date.now() - t0;
    return stats;
  }

  // getStatsByDate: group by scheduled date for a given status
  static async getStatsByDate(status, scheduledTimeFrom, scheduledTimeTo) {
    if (!status) throw new Error(`[${TABLE_NAME}] status is required for getStatsByDate()`);
    if (!scheduledTimeFrom || !scheduledTimeTo) throw new Error('scheduledTimeFrom and scheduledTimeTo are required');

    const t0 = Date.now();
    const allItems = await this._queryAll({
      TableName: TABLE_NAME,
      IndexName: 'status-scheduledTime-index',
      KeyConditionExpression: '#st = :st AND scheduledTime BETWEEN :from AND :to',
      ExpressionAttributeNames: { '#st': 'status' },
      ExpressionAttributeValues: { ':st': status, ':from': scheduledTimeFrom, ':to': scheduledTimeTo },
      ScanIndexForward: true
    });

    console.log(`[${TABLE_NAME}] getStatsByDate(${status}): ${allItems.length} items in ${Date.now() - t0}ms`);

    const map = {};
    allItems.forEach(item => {
      const date = (item.scheduledTime || item.createdAt || '').split('T')[0];
      if (!date) return;
      if (!map[date]) {
        map[date] = { date, total: 0, rcsTypeBreakdown: {}, attemptsBreakdown: {} };
      }
      map[date].total++;
      const rt = item.rcsType || 'unknown';
      map[date].rcsTypeBreakdown[rt] = (map[date].rcsTypeBreakdown[rt] || 0) + 1;
      const att = String(item.attempts || '0');
      map[date].attemptsBreakdown[att] = (map[date].attemptsBreakdown[att] || 0) + 1;
    });

    return Object.values(map).sort((a, b) => a.date.localeCompare(b.date));
  }

  // ─── mutations ────────────────────────────────────────────────────────────

  static async update(rcs_queue, updates) {
    const names = {};
    const values = {};
    const parts = [];

    if (updates.attempts !== undefined) updates.attempts = String(updates.attempts);
    updates.updatedAt = new Date().toISOString();

    Object.entries(updates).forEach(([key, val], i) => {
      const n = `#f${i}`;
      const v = `:v${i}`;
      names[n] = key;
      values[v] = val;
      parts.push(`${n} = ${v}`);
    });

    const res = await docClient.send(new UpdateCommand({
      TableName: TABLE_NAME,
      Key: { rcs_queue },
      UpdateExpression: `SET ${parts.join(', ')}`,
      ExpressionAttributeNames: names,
      ExpressionAttributeValues: values,
      ReturnValues: 'ALL_NEW'
    }));

    return res.Attributes;
  }

  static async updateManyByLeadIdAndStatus(leadId, currentStatus, updates) {
    const { items } = await this.findByLeadId(leadId);
    const matching = items.filter(i => i.status === currentStatus);
    let modifiedCount = 0;
    for (const item of matching) {
      await this.update(item.rcs_queue, updates);
      modifiedCount++;
    }
    return { modifiedCount };
  }

  // Uses status-attempts-index instead of Scan
  static async updateManyByStatusAndAttempts(status, maxAttempts, updates) {
    const { items } = await this.findByStatusAndAttempts(status, maxAttempts, { limit: 1000 });
    let modifiedCount = 0;
    for (const item of items) {
      await this.update(item.rcs_queue, updates);
      modifiedCount++;
    }
    return { modifiedCount };
  }

  static async delete(rcs_queue) {
    await docClient.send(new DeleteCommand({ TableName: TABLE_NAME, Key: { rcs_queue } }));
    return { deleted: true };
  }

  // findRecent: uses status-scheduledTime-index for SENT items instead of Scan
  static async findRecent(limit = 10) {
    const res = await docClient.send(new QueryCommand({
      TableName: TABLE_NAME,
      IndexName: 'status-scheduledTime-index',
      KeyConditionExpression: '#st = :st',
      ExpressionAttributeNames: { '#st': 'status' },
      ExpressionAttributeValues: { ':st': 'SENT' },
      ScanIndexForward: false,
      Limit: limit,
      ProjectionExpression: 'rcs_queue, leadId, rcsType, #st, createdAt',
    }));
    return res.Items || [];
  }
}

module.exports = { RCSQueue };
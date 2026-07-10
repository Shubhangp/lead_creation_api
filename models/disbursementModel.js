'use strict';

const { v4: uuidv4 } = require('uuid');
const { docClient } = require('../dynamodb');
const {
  PutCommand,
  QueryCommand,
  ScanCommand,
} = require('@aws-sdk/lib-dynamodb');

const TABLE_NAME = 'disbursements';

// Robust amount parser: "₹26,460.00" / "26,460" / 26460 → 26460
function parseAmount(val) {
  if (val === null || val === undefined || val === '') return 0;
  const cleaned = String(val).replace(/[^0-9.\-]/g, '');
  const num = parseFloat(cleaned);
  return isNaN(num) ? 0 : num;
}

// Normalize a stored disbursalDate to "YYYY-MM-DD" for comparison (handles
// "26/06/2026", ISO timestamps, etc). Returns null if unparseable.
function toISODate(val) {
  if (!val) return null;
  const s = String(val).trim();
  const m = s.match(/^(\d{1,2})[\/\-](\d{1,2})[\/\-](\d{4})/);
  if (m) return `${m[3]}-${m[2].padStart(2, '0')}-${m[1].padStart(2, '0')}`;
  const iso = s.match(/^(\d{4}-\d{2}-\d{2})/);
  if (iso) return iso[1];
  const d = new Date(s);
  return isNaN(d.getTime()) ? null : d.toISOString().split('T')[0];
}

// Filter items by disbursalDate range (inclusive). Items without a parseable
// date are excluded when a range is active, included otherwise.
function filterByDateRange(items, startDate, endDate) {
  if (!startDate && !endDate) return items;
  return items.filter(item => {
    const d = toISODate(item.disbursalDate);
    if (!d) return false;
    if (startDate && d < startDate) return false;
    if (endDate && d > endDate) return false;
    return true;
  });
}

class Disbursement {
  /**
   * Create a new disbursement record.
   *
   * @param {Object} data
   * @param {string}  [data.leadId]
   * @param {string}   data.source
   * @param {string}  [data.disbursalDate]
   * @param {string}  [data.disbursalAmount]
   * @param {string}  [data.name]
   * @param {string}  [data.phone]
   * @param {string}  [data.utmCampaign]
   * @param {string}  [data.utmMedium]
   * @param {string}  [data.utmSource]
   * @param {string}   data.lender
   * @param {string}  [data.lenderKey]
   */
  static async create(data) {
    const now = new Date().toISOString();
    const item = {
      // Caller may pass a deterministic _id (e.g. `${lenderKey}#${leadId}`) so
      // re-uploads overwrite the existing record instead of duplicating it.
      _id:             data._id || uuidv4(),
      source:          data.source,
      lender:          data.lender       || null,
      lenderKey:       data.lenderKey    || null,
      leadId:          data.leadId       || null,
      disbursalDate:   data.disbursalDate   || null,
      disbursalAmount: data.disbursalAmount  ? String(data.disbursalAmount) : null,
      name:            data.name         || null,
      phone:           data.phone        || null,
      utmCampaign:     data.utmCampaign  || null,
      utmMedium:       data.utmMedium    || null,
      utmSource:       data.utmSource    || null,
      unmatched:       data.unmatched    || null,
      createdAt:       now,
      updatedAt:       now,
    };

    // Strip nulls so DynamoDB doesn't reject empty-string/null attributes
    Object.keys(item).forEach(k => {
      if (item[k] === null || item[k] === undefined) delete item[k];
    });

    await docClient.send(new PutCommand({ TableName: TABLE_NAME, Item: item }));
    return item;
  }

  /**
   * Get disbursement stats for one source.
   * Returns { count, totalAmount, items }
   */
  static async getStatsBySource(source, { startDate, endDate } = {}) {
    let items = await Disbursement._queryAllBySource(source);
    items = filterByDateRange(items, startDate, endDate);
    const count = items.length;
    const totalAmount = items.reduce((sum, item) => sum + parseAmount(item.disbursalAmount), 0);
    return { count, totalAmount, items };
  }

  /**
   * Get all disbursements for a source (all pages).
   */
  static async _queryAllBySource(source) {
    const all = [];
    let lastKey = null;
    do {
      const params = {
        TableName:                 TABLE_NAME,
        IndexName:                 'source-createdAt-index',
        KeyConditionExpression:    '#src = :src',
        ExpressionAttributeNames:  { '#src': 'source' },
        ExpressionAttributeValues: { ':src': source },
      };
      if (lastKey) params.ExclusiveStartKey = lastKey;

      const res = await docClient.send(new QueryCommand(params));
      all.push(...(res.Items || []));
      lastKey = res.LastEvaluatedKey;
    } while (lastKey);

    return all;
  }

  /**
   * Get per-source stats for all sources (superadmin).
   * Returns Array<{ source, count, totalAmount }>
   */
  static async getAllSourceStats({ startDate, endDate } = {}) {
    // Full scan — acceptable since this is superadmin-only and table is append-only
    let all = [];
    let lastKey = null;
    do {
      const params = {
        TableName:            TABLE_NAME,
        ProjectionExpression: '#src, disbursalAmount, disbursalDate',
        ExpressionAttributeNames: { '#src': 'source' },
      };
      if (lastKey) params.ExclusiveStartKey = lastKey;
      const res = await docClient.send(new ScanCommand(params));
      all.push(...(res.Items || []));
      lastKey = res.LastEvaluatedKey;
    } while (lastKey);

    all = filterByDateRange(all, startDate, endDate);

    // Group by source
    const bySource = {};
    for (const item of all) {
      const src = item.source || 'Unknown';
      if (!bySource[src]) bySource[src] = { source: src, count: 0, totalAmount: 0 };
      bySource[src].count++;
      bySource[src].totalAmount += parseAmount(item.disbursalAmount);
    }

    return Object.values(bySource).sort((a, b) => b.totalAmount - a.totalAmount);
  }

  /**
   * Get all disbursement records with optional source filter (superadmin detail view).
   */
  static async getAll({ source, limit = 500 } = {}) {
    if (source) {
      return Disbursement._queryAllBySource(source);
    }

    // Full scan for superadmin
    const all = [];
    let lastKey = null;
    let fetched = 0;
    do {
      const params = {
        TableName: TABLE_NAME,
        Limit:     Math.min(500, limit - fetched),
      };
      if (lastKey) params.ExclusiveStartKey = lastKey;
      const res = await docClient.send(new ScanCommand(params));
      all.push(...(res.Items || []));
      fetched += res.Items?.length || 0;
      lastKey = res.LastEvaluatedKey;
    } while (lastKey && fetched < limit);

    return all.sort((a, b) => (b.createdAt || '').localeCompare(a.createdAt || ''));
  }
}

module.exports = Disbursement;

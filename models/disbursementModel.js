'use strict';

const { v4: uuidv4 } = require('uuid');
const { docClient } = require('../dynamodb');
const {
  PutCommand,
  QueryCommand,
  ScanCommand,
} = require('@aws-sdk/lib-dynamodb');

const TABLE_NAME = 'disbursements';

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
      _id:             uuidv4(),
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
  static async getStatsBySource(source) {
    const items = await Disbursement._queryAllBySource(source);
    const count = items.length;
    const totalAmount = items.reduce((sum, item) => {
      const amt = parseFloat(item.disbursalAmount || '0');
      return sum + (isNaN(amt) ? 0 : amt);
    }, 0);
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
  static async getAllSourceStats() {
    // Full scan — acceptable since this is superadmin-only and table is append-only
    const all = [];
    let lastKey = null;
    do {
      const params = {
        TableName:            TABLE_NAME,
        ProjectionExpression: '#src, disbursalAmount',
        ExpressionAttributeNames: { '#src': 'source' },
      };
      if (lastKey) params.ExclusiveStartKey = lastKey;
      const res = await docClient.send(new ScanCommand(params));
      all.push(...(res.Items || []));
      lastKey = res.LastEvaluatedKey;
    } while (lastKey);

    // Group by source
    const bySource = {};
    for (const item of all) {
      const src = item.source || 'Unknown';
      if (!bySource[src]) bySource[src] = { source: src, count: 0, totalAmount: 0 };
      bySource[src].count++;
      const amt = parseFloat(item.disbursalAmount || '0');
      if (!isNaN(amt)) bySource[src].totalAmount += amt;
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

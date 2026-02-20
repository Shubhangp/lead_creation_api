const { docClient } = require('../dynamodb');
const {
  PutCommand,
  GetCommand,
  QueryCommand,
  UpdateCommand,
  DeleteCommand
} = require('@aws-sdk/lib-dynamodb');
const { v4: uuidv4 } = require('uuid');

const TABLE_NAME = 'lead_success';

const LENDERS = [
  'OVLY', 'FREO', 'LendingPlate', 'ZYPE', 'FINTIFI',
  'FATAKPAY', 'FATAKPAYPL', 'RAMFINCROP', 'MyMoneyMantra',
  'INDIALENDS', 'CRMPaisa', 'SML', 'MPOKKET'
];

const ALL_SOURCES = ['CashKuber', 'FREO', 'BatterySmart', 'Ratecut', 'VFC'];

class LeadSuccess {

  static async create(successData) {
    const item = {
      successId:     uuidv4(),
      leadId:        successData.leadId        || null,
      source:        successData.source        || null,
      phone:         successData.phone         || null,
      email:         successData.email         || null,
      panNumber:     successData.panNumber     || null,
      fullName:      successData.fullName      || null,
      OVLY:          successData.OVLY          || false,
      FREO:          successData.FREO          || false,
      LendingPlate:  successData.LendingPlate  || false,
      ZYPE:          successData.ZYPE          || false,
      FINTIFI:       successData.FINTIFI       || false,
      FATAKPAY:      successData.FATAKPAY      || false,
      FATAKPAYPL:    successData.FATAKPAYPL    || false,
      RAMFINCROP:    successData.RAMFINCROP    || false,
      MyMoneyMantra: successData.MyMoneyMantra || false,
      INDIALENDS:    successData.INDIALENDS    || false,
      CRMPaisa:      successData.CRMPaisa      || false,
      SML:           successData.SML           || false,
      MPOKKET:       successData.MPOKKET       || false,
      createdAt:     new Date().toISOString()
    };

    await docClient.send(new PutCommand({ TableName: TABLE_NAME, Item: item }));
    return item;
  }

  static async findById(successId) {
    const result = await docClient.send(new GetCommand({
      TableName: TABLE_NAME,
      Key: { successId }
    }));
    return result.Item || null;
  }

  static async findByLeadId(leadId) {
    const result = await docClient.send(new QueryCommand({
      TableName: TABLE_NAME,
      IndexName: 'leadId-index',
      KeyConditionExpression: 'leadId = :leadId',
      ExpressionAttributeValues: { ':leadId': leadId }
    }));
    return result.Items?.[0] || null;
  }

  static async findByPhone(phone) {
    const result = await docClient.send(new QueryCommand({
      TableName: TABLE_NAME,
      IndexName: 'phone-index',
      KeyConditionExpression: 'phone = :phone',
      ExpressionAttributeValues: { ':phone': phone }
    }));
    return result.Items || [];
  }

  static async findByPanNumber(panNumber) {
    const result = await docClient.send(new QueryCommand({
      TableName: TABLE_NAME,
      IndexName: 'panNumber-index',
      KeyConditionExpression: 'panNumber = :panNumber',
      ExpressionAttributeValues: { ':panNumber': panNumber }
    }));
    return result.Items || [];
  }

  static async updateLenderStatus(successId, lenderName, status) {
    if (!LENDERS.includes(lenderName)) {
      throw new Error(`Invalid lender name: ${lenderName}`);
    }
    const result = await docClient.send(new UpdateCommand({
      TableName: TABLE_NAME,
      Key: { successId },
      UpdateExpression: 'SET #lender = :status',
      ExpressionAttributeNames: { '#lender': lenderName },
      ExpressionAttributeValues: { ':status': status },
      ReturnValues: 'ALL_NEW'
    }));
    return result.Attributes;
  }

  static async updateByLeadId(leadId, updates) {
    const existing = await this.findByLeadId(leadId);
    if (!existing) throw new Error('Lead success record not found');

    const updateExpression = [];
    const expressionAttributeNames = {};
    const expressionAttributeValues = {};

    Object.keys(updates).forEach((key, index) => {
      updateExpression.push(`#field${index} = :value${index}`);
      expressionAttributeNames[`#field${index}`] = key;
      expressionAttributeValues[`:value${index}`] = updates[key];
    });

    const result = await docClient.send(new UpdateCommand({
      TableName: TABLE_NAME,
      Key: { successId: existing.successId },
      UpdateExpression: `SET ${updateExpression.join(', ')}`,
      ExpressionAttributeNames: expressionAttributeNames,
      ExpressionAttributeValues: expressionAttributeValues,
      ReturnValues: 'ALL_NEW'
    }));
    return result.Attributes;
  }

  static async findOrCreate(leadData) {
    const existing = await this.findByLeadId(leadData.leadId);
    if (existing) return { record: existing, created: false };
    const newRecord = await this.create(leadData);
    return { record: newRecord, created: true };
  }

  static async deleteById(successId) {
    await docClient.send(new DeleteCommand({
      TableName: TABLE_NAME,
      Key: { successId }
    }));
    return { deleted: true };
  }
  static async _fetchAllBySource(source, startDate, endDate, opts = {}) {
    const {
      limit = 500,
      sortAscending = false,
      projectionFields = null
    } = opts;

    let allItems = [];
    let lastKey  = null;

    do {
      const params = {
        TableName:    TABLE_NAME,
        IndexName:    'source-createdAt-index',

        // ✅ Use BETWEEN to push date filtering into DynamoDB — no more in-memory filtering
        KeyConditionExpression:    '#source = :source AND createdAt BETWEEN :start AND :end',
        ExpressionAttributeNames:  { '#source': 'source' },
        ExpressionAttributeValues: {
          ':source': source,
          ':start':  startDate,
          ':end':    endDate
        },

        ScanIndexForward: sortAscending,
        Limit: limit
      };

      // ✅ Projection: only request the fields you actually need (saves RCU bandwidth)
      if (projectionFields && projectionFields.length > 0) {
        const fieldNames = {};
        const fieldRefs  = projectionFields.map((f, i) => {
          const key = `#proj${i}`;
          fieldNames[key] = f;
          return key;
        });
        params.ProjectionExpression       = fieldRefs.join(', ');
        params.ExpressionAttributeNames   = {
          ...params.ExpressionAttributeNames,
          ...fieldNames
        };
      }

      if (lastKey) params.ExclusiveStartKey = lastKey;

      const result = await docClient.send(new QueryCommand(params));
      allItems = allItems.concat(result.Items || []);
      lastKey  = result.LastEvaluatedKey;
    } while (lastKey);

    return allItems;
  }

  static async _fetchAllSources(sources, startDate, endDate, opts = {}) {
    const results = await Promise.all(
      sources.map(src => this._fetchAllBySource(src.trim(), startDate, endDate, opts))
    );
    return results.flat();
  }

  static async _countBySource(source, startDate, endDate) {
    let totalCount = 0;
    let lastKey    = null;

    do {
      const params = {
        TableName:    TABLE_NAME,
        IndexName:    'source-createdAt-index',
        KeyConditionExpression:    '#source = :source AND createdAt BETWEEN :start AND :end',
        ExpressionAttributeNames:  { '#source': 'source' },
        ExpressionAttributeValues: {
          ':source': source,
          ':start':  startDate,
          ':end':    endDate
        },
        Select: 'COUNT' 
      };

      if (lastKey) params.ExclusiveStartKey = lastKey;

      const result = await docClient.send(new QueryCommand(params));
      totalCount += result.Count || 0;
      lastKey     = result.LastEvaluatedKey;
    } while (lastKey);

    return totalCount;
  }

  static async findBySource(source, options = {}) {
    const {
      limit = 100,
      startDate,
      endDate,
      sortAscending = false,
      lastEvaluatedKey
    } = options;

    // Default to wide range if no dates given (avoids full partition scan)
    const effectiveStart = startDate || '1970-01-01T00:00:00.000Z';
    const effectiveEnd   = endDate   || new Date().toISOString();

    const params = {
      TableName:    TABLE_NAME,
      IndexName:    'source-createdAt-index',
      KeyConditionExpression:    '#source = :source AND createdAt BETWEEN :start AND :end',
      ExpressionAttributeNames:  { '#source': 'source' },
      ExpressionAttributeValues: {
        ':source': source,
        ':start':  effectiveStart,
        ':end':    effectiveEnd
      },
      ScanIndexForward: sortAscending,
      Limit: limit
    };

    if (lastEvaluatedKey) params.ExclusiveStartKey = lastEvaluatedKey;

    const result = await docClient.send(new QueryCommand(params));
    return {
      items:            result.Items || [],
      lastEvaluatedKey: result.LastEvaluatedKey
    };
  }

  static async getStats(source = null, startDate = null, endDate = null) {
    const startTime = Date.now();

    if (!startDate || !endDate) {
      const now = new Date();
      endDate   = now.toISOString();
      startDate = new Date(now.getTime() - 7 * 24 * 60 * 60 * 1000).toISOString();
    }

    console.log(`[${TABLE_NAME}] getStats: source=${source}, ${startDate} → ${endDate}`);

    const sources  = source ? [source] : ALL_SOURCES;

    // ✅ All sources fetched in parallel
    const allItems = await this._fetchAllSources(sources, startDate, endDate);

    console.log(`✅ Total items fetched: ${allItems.length} in ${Date.now() - startTime}ms`);

    const stats = this._calculateStats(allItems, source, startDate, endDate);
    stats.processingTimeMs = Date.now() - startTime;
    return stats;
  }

  static _calculateStats(items, source, startDate, endDate) {
    const stats = {
      totalLeads:     items.length,
      source,
      dateRange:      { start: startDate, end: endDate },
      sourceBreakdown: {},
      lenderStats:    {},
      totalAccepted:  0,
      totalRejected:  0
    };

    LENDERS.forEach(lender => {
      stats.lenderStats[lender] = { accepted: 0, rejected: 0, acceptanceRate: '0%' };
    });

    items.forEach(item => {
      const src = item.source || 'unknown';

      if (!stats.sourceBreakdown[src]) {
        stats.sourceBreakdown[src] = { total: 0, lenderAcceptance: {} };
        LENDERS.forEach(l => {
          stats.sourceBreakdown[src].lenderAcceptance[l] = { accepted: 0, rejected: 0 };
        });
      }

      stats.sourceBreakdown[src].total++;

      LENDERS.forEach(lender => {
        if (item[lender] === true) {
          stats.lenderStats[lender].accepted++;
          stats.sourceBreakdown[src].lenderAcceptance[lender].accepted++;
          stats.totalAccepted++;
        } else {
          stats.lenderStats[lender].rejected++;
          stats.sourceBreakdown[src].lenderAcceptance[lender].rejected++;
          stats.totalRejected++;
        }
      });
    });

    LENDERS.forEach(lender => {
      const { accepted, rejected } = stats.lenderStats[lender];
      const total = accepted + rejected;
      if (total > 0) {
        stats.lenderStats[lender].acceptanceRate = `${(accepted / total * 100).toFixed(2)}%`;
      }
    });

    return stats;
  }

  static async getLeadList(source = null, startDate = null, endDate = null) {
    const startTime = Date.now();

    if (!startDate || !endDate) {
      const now = new Date();
      endDate   = now.toISOString();
      startDate = new Date(now.getTime() - 7 * 24 * 60 * 60 * 1000).toISOString();
    }

    const sources  = source ? [source] : ALL_SOURCES;
    const allItems = await this._fetchAllSources(sources, startDate, endDate);

    const leadList = allItems.map(item => {
      const acceptedCount = LENDERS.reduce((n, l) => n + (item[l] === true ? 1 : 0), 0);
      return {
        id:       `${item.phone}_${item.fullName}`.replace(/\s+/g, '_'),
        name:     item.fullName  || 'N/A',
        mobile:   item.phone     || 'N/A',
        pan:      item.panNumber || 'N/A',
        accepted: acceptedCount,
        dateSent: item.createdAt,
        source:   item.source    || 'unknown'
      };
    });

    leadList.sort((a, b) => b.accepted - a.accepted);

    return {
      leads:           leadList,
      total:           leadList.length,
      dateRange:       { start: startDate, end: endDate },
      source:          source || 'all',
      processingTimeMs: Date.now() - startTime
    };
  }

  static async getSourceWiseStats(startDate = null, endDate = null) {
    const startTime = Date.now();

    if (!startDate || !endDate) {
      const now = new Date();
      endDate   = now.toISOString();
      startDate = new Date(now.getTime() - 7 * 24 * 60 * 60 * 1000).toISOString();
    }

    // ✅ Fetch all sources in parallel
    const perSourceResults = await Promise.all(
      ALL_SOURCES.map(async src => {
        const items = await this._fetchAllBySource(src, startDate, endDate);
        return { src, items };
      })
    );

    const sourceStats = {};

    perSourceResults.forEach(({ src, items }) => {
      const leads = items.map(item => {
        const acceptedCount = LENDERS.reduce((n, l) => n + (item[l] === true ? 1 : 0), 0);
        return {
          id:       `${item.phone}_${item.fullName}`.replace(/\s+/g, '_'),
          name:     item.fullName  || 'N/A',
          mobile:   item.phone     || 'N/A',
          pan:      item.panNumber || 'N/A',
          accepted: acceptedCount,
          dateSent: item.createdAt
        };
      });

      leads.sort((a, b) => b.accepted - a.accepted);
      sourceStats[src] = { totalLeads: items.length, leads };
      console.log(`  ✅ ${src}: ${items.length} leads`);
    });

    return {
      dateRange:        { start: startDate, end: endDate },
      sourceStats,
      processingTimeMs: Date.now() - startTime
    };
  }

  static async getQuickStats(source = null, startDate = null, endDate = null) {
    const startTime = Date.now();

    if (!startDate || !endDate) {
      const now = new Date();
      endDate   = now.toISOString();
      startDate = new Date(now.getTime() - 7 * 24 * 60 * 60 * 1000).toISOString();
    }

    const sources = source ? [source] : ALL_SOURCES;

    // ✅ Count all sources in parallel using SELECT: COUNT
    const counts = await Promise.all(
      sources.map(src => this._countBySource(src.trim(), startDate, endDate))
    );

    const sourceBreakdown = {};
    let totalCount = 0;

    sources.forEach((src, i) => {
      sourceBreakdown[src.trim()] = counts[i];
      totalCount += counts[i];
    });

    return {
      totalLeads:      totalCount,
      source:          source || null,
      sourceBreakdown,
      dateRange:       { start: startDate, end: endDate },
      scannedInMs:     Date.now() - startTime,
      method:          'gsi-sort-key-count'
    };
  }
}

module.exports = LeadSuccess;
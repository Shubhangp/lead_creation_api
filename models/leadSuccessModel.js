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

class LeadSuccess {

  static async create(successData) {
    const item = {
      successId: uuidv4(),
      leadId: successData.leadId || null,
      source: successData.source || null,
      phone: successData.phone || null,
      email: successData.email || null,
      panNumber: successData.panNumber || null,
      fullName: successData.fullName || null,
      OVLY: successData.OVLY || false,
      FREO: successData.FREO || false,
      LendingPlate: successData.LendingPlate || false,
      ZYPE: successData.ZYPE || false,
      FINTIFI: successData.FINTIFI || false,
      FATAKPAY: successData.FATAKPAY || false,
      RAMFINCROP: successData.RAMFINCROP || false,
      MyMoneyMantra: successData.MyMoneyMantra || false,
      INDIALENDS: successData.INDIALENDS || false,
      CRMPaisa: successData.CRMPaisa || false,
      SML: successData.SML || false,
      MPOKKET: successData.MPOKKET || false,
      createdAt: new Date().toISOString()
    };

    await docClient.send(new PutCommand({
      TableName: TABLE_NAME,
      Item: item
    }));

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

  // ✅ OPTIMIZED: Find by source using GSI (no Scan)
  static async findBySource(source, options = {}) {
    const { limit = 100, startDate, endDate, sortAscending = false, lastEvaluatedKey } = options;

    let keyConditionExpression = 'source = :source';
    const expressionAttributeValues = { ':source': source };

    if (startDate && endDate) {
      keyConditionExpression += ' AND createdAt BETWEEN :startDate AND :endDate';
      expressionAttributeValues[':startDate'] = startDate;
      expressionAttributeValues[':endDate'] = endDate;
    } else if (startDate) {
      keyConditionExpression += ' AND createdAt >= :startDate';
      expressionAttributeValues[':startDate'] = startDate;
    } else if (endDate) {
      keyConditionExpression += ' AND createdAt <= :endDate';
      expressionAttributeValues[':endDate'] = endDate;
    }

    const params = {
      TableName: TABLE_NAME,
      IndexName: 'source-createdAt-index',
      KeyConditionExpression: keyConditionExpression,
      ExpressionAttributeValues: expressionAttributeValues,
      ScanIndexForward: sortAscending,
      Limit: limit
    };

    if (lastEvaluatedKey) {
      params.ExclusiveStartKey = lastEvaluatedKey;
    }

    const result = await docClient.send(new QueryCommand(params));

    return {
      items: result.Items || [],
      lastEvaluatedKey: result.LastEvaluatedKey
    };
  }

  // ✅ NEW: Fetch all items for a source (for stats)
  static async _fetchAllBySource(source, startDate, endDate) {
    let allItems = [];
    let lastKey = null;

    do {
      const { items, lastEvaluatedKey } = await this.findBySource(source, {
        limit: 1000,
        startDate,
        endDate,
        lastEvaluatedKey: lastKey
      });

      allItems = allItems.concat(items);
      lastKey = lastEvaluatedKey;
    } while (lastKey);

    return allItems;
  }

  static async updateLenderStatus(successId, lenderName, status) {
    const validLenders = [
      'OVLY', 'FREO', 'LendingPlate', 'ZYPE', 'FINTIFI', 
      'FATAKPAY', 'RAMFINCROP', 'MyMoneyMantra', 'INDIALENDS', 'CRMPaisa', 'SML', 'MPOKKET'
    ];

    if (!validLenders.includes(lenderName)) {
      throw new Error(`Invalid lender name: ${lenderName}`);
    }

    const result = await docClient.send(new UpdateCommand({
      TableName: TABLE_NAME,
      Key: { successId },
      UpdateExpression: `SET #lender = :status`,
      ExpressionAttributeNames: {
        '#lender': lenderName
      },
      ExpressionAttributeValues: {
        ':status': status
      },
      ReturnValues: 'ALL_NEW'
    }));

    return result.Attributes;
  }

  static async updateByLeadId(leadId, updates) {
    const existing = await this.findByLeadId(leadId);
    
    if (!existing) {
      throw new Error('Lead success record not found');
    }

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
    
    if (existing) {
      return { record: existing, created: false };
    }

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

  static async getStats(source = null, startDate = null, endDate = null) {
    const startTime = Date.now();

    // ✅ Default to last 7 days if no dates provided
    if (!startDate || !endDate) {
      const now = new Date();
      endDate = now.toISOString();
      const sevenDaysAgo = new Date(now.getTime() - 7 * 24 * 60 * 60 * 1000);
      startDate = sevenDaysAgo.toISOString();
    }

    console.log(`[${TABLE_NAME}] Getting stats: source=${source}, ${startDate} to ${endDate}`);

    try {
      let allItems = [];

      if (!source) {
        // ✅ Get all known sources
        const sources = ['CashKuber', 'FREO', 'BatterySmart', 'Ratecut', 'VFC'];
        
        console.log(`[${TABLE_NAME}] Fetching from ${sources.length} sources...`);

        for (const src of sources) {
          const items = await this._fetchAllBySource(src.trim(), startDate, endDate);
          allItems = allItems.concat(items);
          console.log(`  ✅ ${src}: ${items.length} items`);
        }
      } else {
        // Single source
        allItems = await this._fetchAllBySource(source, startDate, endDate);
      }

      console.log(`✅ Total items fetched: ${allItems.length}`);

      // Calculate stats
      const stats = this._calculateStats(allItems, source, startDate, endDate);
      stats.processingTimeMs = Date.now() - startTime;

      return stats;
    } catch (error) {
      console.error('Error in getStats:', error);
      throw error;
    }
  }

  /**
   * ✅ NEW: Calculate comprehensive statistics
   */
  static _calculateStats(items, source, startDate, endDate) {
    const lenders = [
      'OVLY', 'FREO', 'LendingPlate', 'ZYPE', 'FINTIFI', 
      'FATAKPAY', 'RAMFINCROP', 'MyMoneyMantra', 'INDIALENDS', 'CRMPaisa', 'SML', 'MPOKKET'
    ];

    const stats = {
      totalLeads: items.length,
      source: source,
      dateRange: { start: startDate, end: endDate },
      
      // Source-wise breakdown
      sourceBreakdown: {},
      
      // Lender-wise stats
      lenderStats: {},
      
      // Overall lender counts
      totalAccepted: 0,
      totalRejected: 0
    };

    // Initialize lender stats
    lenders.forEach(lender => {
      stats.lenderStats[lender] = {
        accepted: 0,
        rejected: 0,
        acceptanceRate: '0%'
      };
    });

    // Process each item
    items.forEach(item => {
      const itemSource = item.source || 'unknown';
      
      // Source breakdown
      if (!stats.sourceBreakdown[itemSource]) {
        stats.sourceBreakdown[itemSource] = {
          total: 0,
          lenderAcceptance: {}
        };
        
        lenders.forEach(lender => {
          stats.sourceBreakdown[itemSource].lenderAcceptance[lender] = {
            accepted: 0,
            rejected: 0
          };
        });
      }
      
      stats.sourceBreakdown[itemSource].total++;

      // Count lender acceptances
      lenders.forEach(lender => {
        if (item[lender] === true) {
          stats.lenderStats[lender].accepted++;
          stats.sourceBreakdown[itemSource].lenderAcceptance[lender].accepted++;
          stats.totalAccepted++;
        } else {
          stats.lenderStats[lender].rejected++;
          stats.sourceBreakdown[itemSource].lenderAcceptance[lender].rejected++;
          stats.totalRejected++;
        }
      });
    });

    // Calculate acceptance rates
    lenders.forEach(lender => {
      const total = stats.lenderStats[lender].accepted + stats.lenderStats[lender].rejected;
      if (total > 0) {
        const rate = (stats.lenderStats[lender].accepted / total * 100).toFixed(2);
        stats.lenderStats[lender].acceptanceRate = `${rate}%`;
      }
    });

    return stats;
  }

  /**
   * ✅ NEW: Get detailed lead list with acceptance counts
   * Returns format: { id, name, mobile, pan, accepted, dateSent }
   */
  static async getLeadList(source = null, startDate = null, endDate = null) {
    const startTime = Date.now();

    // ✅ Default to last 7 days
    if (!startDate || !endDate) {
      const now = new Date();
      endDate = now.toISOString();
      const sevenDaysAgo = new Date(now.getTime() - 7 * 24 * 60 * 60 * 1000);
      startDate = sevenDaysAgo.toISOString();
    }

    console.log(`[${TABLE_NAME}] Getting lead list: source=${source}, ${startDate} to ${endDate}`);

    try {
      let allItems = [];

      if (!source) {
        const sources = ['CashKuber', 'FREO', 'BatterySmart', 'Ratecut', 'VFC'];

        for (const src of sources) {
          const items = await this._fetchAllBySource(src.trim(), startDate, endDate);
          allItems = allItems.concat(items);
        }
      } else {
        allItems = await this._fetchAllBySource(source, startDate, endDate);
      }

      // Transform to required format
      const lenders = [
        'OVLY', 'FREO', 'LendingPlate', 'ZYPE', 'FINTIFI', 
        'FATAKPAY', 'RAMFINCROP', 'MyMoneyMantra', 'INDIALENDS', 'CRMPaisa', 'SML', 'MPOKKET'
      ];

      const leadList = allItems.map(item => {
        // Count how many lenders accepted
        const acceptedCount = lenders.reduce((count, lender) => {
          return count + (item[lender] === true ? 1 : 0);
        }, 0);

        return {
          id: `${item.phone}_${item.fullName}`.replace(/\s+/g, '_'), // Combination of mobile and name
          name: item.fullName || 'N/A',
          mobile: item.phone || 'N/A',
          pan: item.panNumber || 'N/A',
          accepted: acceptedCount,
          dateSent: item.createdAt,
          source: item.source || 'unknown'
        };
      });

      // Sort by accepted count (descending)
      leadList.sort((a, b) => b.accepted - a.accepted);

      const elapsed = Date.now() - startTime;

      return {
        leads: leadList,
        total: leadList.length,
        dateRange: { start: startDate, end: endDate },
        source: source || 'all',
        processingTimeMs: elapsed
      };
    } catch (error) {
      console.error('Error in getLeadList:', error);
      throw error;
    }
  }

  /**
   * ✅ NEW: Get source-wise detailed stats (no lender names)
   */
  static async getSourceWiseStats(startDate = null, endDate = null) {
    const startTime = Date.now();

    // Default to last 7 days
    if (!startDate || !endDate) {
      const now = new Date();
      endDate = now.toISOString();
      const sevenDaysAgo = new Date(now.getTime() - 7 * 24 * 60 * 60 * 1000);
      startDate = sevenDaysAgo.toISOString();
    }

    console.log(`[${TABLE_NAME}] Getting source-wise stats: ${startDate} to ${endDate}`);

    try {
      const sources = ['CashKuber', 'FREO', 'BatterySmart', 'Ratecut', 'VFC'];

      const sourceStats = {};

      for (const source of sources) {
        const trimmedSource = source.trim();
        const items = await this._fetchAllBySource(trimmedSource, startDate, endDate);

        const lenders = [
          'OVLY', 'FREO', 'LendingPlate', 'ZYPE', 'FINTIFI', 
          'FATAKPAY', 'RAMFINCROP', 'MyMoneyMantra', 'INDIALENDS', 'CRMPaisa', 'SML', 'MPOKKET'
        ];

        const leads = items.map(item => {
          const acceptedCount = lenders.reduce((count, lender) => {
            return count + (item[lender] === true ? 1 : 0);
          }, 0);

          return {
            id: `${item.phone}_${item.fullName}`.replace(/\s+/g, '_'),
            name: item.fullName || 'N/A',
            mobile: item.phone || 'N/A',
            pan: item.panNumber || 'N/A',
            accepted: acceptedCount,
            dateSent: item.createdAt
          };
        });

        // Sort by accepted count
        leads.sort((a, b) => b.accepted - a.accepted);

        sourceStats[trimmedSource] = {
          totalLeads: items.length,
          leads: leads
        };

        console.log(`  ✅ ${trimmedSource}: ${items.length} leads`);
      }

      const elapsed = Date.now() - startTime;

      return {
        dateRange: { start: startDate, end: endDate },
        sourceStats,
        processingTimeMs: elapsed
      };
    } catch (error) {
      console.error('Error in getSourceWiseStats:', error);
      throw error;
    }
  }

  /**
   * ✅ NEW: Quick count by source (for quick stats)
   */
  static async getQuickStats(source = null, startDate = null, endDate = null) {
    const startTime = Date.now();

    // Default to last 7 days
    if (!startDate || !endDate) {
      const now = new Date();
      endDate = now.toISOString();
      const sevenDaysAgo = new Date(now.getTime() - 7 * 24 * 60 * 60 * 1000);
      startDate = sevenDaysAgo.toISOString();
    }

    try {
      if (!source) {
        // Count all sources
        const sources = ['CashKuber', 'FREO', 'BatterySmart', 'Ratecut', 'VFC'];

        let totalCount = 0;
        const sourceBreakdown = {};

        for (const src of sources) {
          const trimmedSource = src.trim();
          const count = await this._countBySource(trimmedSource, startDate, endDate);
          sourceBreakdown[trimmedSource] = count;
          totalCount += count;
        }

        const elapsed = Date.now() - startTime;

        return {
          totalLeads: totalCount,
          sourceBreakdown,
          dateRange: { start: startDate, end: endDate },
          scannedInMs: elapsed,
          method: 'gsi-query-count'
        };
      } else {
        // Single source
        const count = await this._countBySource(source, startDate, endDate);
        const elapsed = Date.now() - startTime;

        return {
          totalLeads: count,
          source,
          sourceBreakdown: { [source]: count },
          dateRange: { start: startDate, end: endDate },
          scannedInMs: elapsed,
          method: 'gsi-query-count'
        };
      }
    } catch (error) {
      console.error('Error in getQuickStats:', error);
      throw error;
    }
  }

  /**
   * Helper: Count by source
   */
  static async _countBySource(source, startDate, endDate) {
    let totalCount = 0;
    let lastKey = null;

    do {
      const params = {
        TableName: TABLE_NAME,
        IndexName: 'source-createdAt-index',
        KeyConditionExpression: 'source = :source AND createdAt BETWEEN :start AND :end',
        ExpressionAttributeValues: {
          ':source': source,
          ':start': startDate,
          ':end': endDate
        },
        Select: 'COUNT'
      };

      if (lastKey) {
        params.ExclusiveStartKey = lastKey;
      }

      const result = await docClient.send(new QueryCommand(params));
      totalCount += result.Count || 0;
      lastKey = result.LastEvaluatedKey;
    } while (lastKey);

    return totalCount;
  }
}

module.exports = LeadSuccess;
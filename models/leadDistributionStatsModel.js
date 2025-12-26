const { docClient } = require('../dynamodb');
const { 
  PutCommand, 
  GetCommand, 
  QueryCommand, 
  UpdateCommand,
  ScanCommand 
} = require('@aws-sdk/lib-dynamodb');
const { v4: uuidv4 } = require('uuid');

const TABLE_NAME = 'lead_distribution_stats';

class LeadDistributionStats {
  /**
   * Create a new distribution batch record
   */
  static async createBatch(batchData) {
    const item = {
      batchId: uuidv4(),
      lender: batchData.lender,
      filters: batchData.filters || {},
      totalLeads: 0,
      successfulLeads: 0,
      failedLeads: 0,
      processedLeads: 0,
      status: 'PROCESSING', // PROCESSING, COMPLETED, FAILED, PARTIAL
      leadIds: [], // Array to track which leads were sent
      startedAt: new Date().toISOString(),
      completedAt: null,
      errors: [],
      createdAt: new Date().toISOString()
    };

    await docClient.send(new PutCommand({
      TableName: TABLE_NAME,
      Item: item
    }));

    return item;
  }

  /**
   * Update batch statistics
   */
  static async updateBatchStats(batchId, updates) {
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
      Key: { batchId },
      UpdateExpression: `SET ${updateExpression.join(', ')}`,
      ExpressionAttributeNames: expressionAttributeNames,
      ExpressionAttributeValues: expressionAttributeValues,
      ReturnValues: 'ALL_NEW'
    }));

    return result.Attributes;
  }

  /**
   * Increment counters atomically
   */
  static async incrementCounters(batchId, counters) {
    const updateExpression = [];
    const expressionAttributeNames = {};
    const expressionAttributeValues = {};

    Object.keys(counters).forEach((key, index) => {
      updateExpression.push(`#field${index} = #field${index} + :value${index}`);
      expressionAttributeNames[`#field${index}`] = key;
      expressionAttributeValues[`:value${index}`] = counters[key];
    });

    const result = await docClient.send(new UpdateCommand({
      TableName: TABLE_NAME,
      Key: { batchId },
      UpdateExpression: `SET ${updateExpression.join(', ')}`,
      ExpressionAttributeNames: expressionAttributeNames,
      ExpressionAttributeValues: expressionAttributeValues,
      ReturnValues: 'ALL_NEW'
    }));

    return result.Attributes;
  }

  /**
   * Add lead ID to batch
   */
  static async addLeadToBatch(batchId, leadId, success = true) {
    const result = await docClient.send(new UpdateCommand({
      TableName: TABLE_NAME,
      Key: { batchId },
      UpdateExpression: 'SET leadIds = list_append(if_not_exists(leadIds, :empty_list), :leadId), processedLeads = processedLeads + :inc, #success = #success + :inc',
      ExpressionAttributeNames: {
        '#success': success ? 'successfulLeads' : 'failedLeads'
      },
      ExpressionAttributeValues: {
        ':leadId': [leadId],
        ':empty_list': [],
        ':inc': 1
      },
      ReturnValues: 'ALL_NEW'
    }));

    return result.Attributes;
  }

  /**
   * Add error to batch
   */
  static async addError(batchId, error) {
    await docClient.send(new UpdateCommand({
      TableName: TABLE_NAME,
      Key: { batchId },
      UpdateExpression: 'SET errors = list_append(if_not_exists(errors, :empty_list), :error)',
      ExpressionAttributeValues: {
        ':error': [{
          message: error.message,
          timestamp: new Date().toISOString()
        }],
        ':empty_list': []
      }
    }));
  }

  /**
   * Get batch by ID
   */
  static async findById(batchId) {
    const result = await docClient.send(new GetCommand({
      TableName: TABLE_NAME,
      Key: { batchId }
    }));

    return result.Item || null;
  }

  /**
   * Get batches by lender
   */
  static async findByLender(lender, options = {}) {
    const { limit = 50 } = options;

    const result = await docClient.send(new QueryCommand({
      TableName: TABLE_NAME,
      IndexName: 'lender-createdAt-index',
      KeyConditionExpression: 'lender = :lender',
      ExpressionAttributeValues: { ':lender': lender },
      ScanIndexForward: false, // Most recent first
      Limit: limit
    }));

    return result.Items || [];
  }

  /**
   * Get all batches (paginated)
   */
  static async findAll(options = {}) {
    const { limit = 50, lastEvaluatedKey } = options;

    const params = {
      TableName: TABLE_NAME,
      Limit: limit
    };

    if (lastEvaluatedKey) {
      params.ExclusiveStartKey = lastEvaluatedKey;
    }

    const result = await docClient.send(new ScanCommand(params));

    return {
      items: result.Items || [],
      lastEvaluatedKey: result.LastEvaluatedKey
    };
  }

  /**
   * Get statistics summary for a lender
   */
  static async getLenderSummary(lender, startDate, endDate) {
    let keyConditionExpression = 'lender = :lender';
    const expressionAttributeValues = { ':lender': lender };

    if (startDate && endDate) {
      keyConditionExpression += ' AND createdAt BETWEEN :startDate AND :endDate';
      expressionAttributeValues[':startDate'] = startDate;
      expressionAttributeValues[':endDate'] = endDate;
    }

    const result = await docClient.send(new QueryCommand({
      TableName: TABLE_NAME,
      IndexName: 'lender-createdAt-index',
      KeyConditionExpression: keyConditionExpression,
      ExpressionAttributeValues: expressionAttributeValues
    }));

    const batches = result.Items || [];
    
    return {
      lender,
      totalBatches: batches.length,
      totalLeadsSent: batches.reduce((sum, b) => sum + (b.totalLeads || 0), 0),
      totalSuccessful: batches.reduce((sum, b) => sum + (b.successfulLeads || 0), 0),
      totalFailed: batches.reduce((sum, b) => sum + (b.failedLeads || 0), 0),
      batches: batches
    };
  }
}

module.exports = LeadDistributionStats;
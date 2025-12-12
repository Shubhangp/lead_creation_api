const { docClient } = require('../dynamodb');
const { PutCommand, GetCommand, QueryCommand, ScanCommand } = require('@aws-sdk/lib-dynamodb');
const { v4: uuidv4 } = require('uuid');

const TABLE_NAME = 'mpokket_response_logs';

class MpokketResponseLog {
  // Create log entry
  static async create(logData) {
    const item = {
      logId: uuidv4(),
      leadId: logData.leadId,
      source: logData.source || null,
      correlationId: logData.correlationId || null,
      requestPayload: logData.requestPayload || null,
      responseStatus: logData.responseStatus || null,
      responseBody: logData.responseBody || null,
      errorDetails: logData.errorDetails || null,
      step: logData.step || null,
      status: logData.status || null,
      createdAt: new Date().toISOString()
    };

    await docClient.send(new PutCommand({
      TableName: TABLE_NAME,
      Item: item
    }));

    return item;
  }

  // Find by ID
  static async findById(logId) {
    const result = await docClient.send(new GetCommand({
      TableName: TABLE_NAME,
      Key: { logId }
    }));

    return result.Item || null;
  }

  // Find by leadId
  static async findByLeadId(leadId) {
    const result = await docClient.send(new QueryCommand({
      TableName: TABLE_NAME,
      IndexName: 'leadId-index',
      KeyConditionExpression: 'leadId = :leadId',
      ExpressionAttributeValues: { ':leadId': leadId }
    }));

    return result.Items || [];
  }

  // Find by source with date range
  static async findBySource(source, options = {}) {
    const { limit = 100, startDate, endDate, sortAscending = false } = options;

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

    const result = await docClient.send(new QueryCommand({
      TableName: TABLE_NAME,
      IndexName: 'source-createdAt-index',
      KeyConditionExpression: keyConditionExpression,
      ExpressionAttributeValues: expressionAttributeValues,
      ScanIndexForward: sortAscending,
      Limit: limit
    }));

    return result.Items || [];
  }

  // Find by status
  static async findByStatus(status, options = {}) {
    const { limit = 100, startDate, endDate, sortAscending = false } = options;

    let keyConditionExpression = 'status = :status';
    const expressionAttributeValues = { ':status': status };

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

    const result = await docClient.send(new QueryCommand({
      TableName: TABLE_NAME,
      IndexName: 'status-index',
      KeyConditionExpression: keyConditionExpression,
      ExpressionAttributeValues: expressionAttributeValues,
      ScanIndexForward: sortAscending,
      Limit: limit
    }));

    return result.Items || [];
  }

  // Find all logs (paginated)
  static async findAll(options = {}) {
    const { limit = 100, lastEvaluatedKey } = options;

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
}

module.exports = MpokketResponseLog;
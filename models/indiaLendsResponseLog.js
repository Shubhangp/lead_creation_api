const { docClient } = require('../dynamodb');
const { PutCommand, GetCommand, QueryCommand, ScanCommand } = require('@aws-sdk/lib-dynamodb');
const { v4: uuidv4 } = require('uuid');

const TABLE_NAME = 'indialends_response_logs';

class IndiaLendsResponseLog {
  // Create log entry
  static async create(logData) {
    const responseStatus = logData.responseStatus || 500;
    const isSuccess = responseStatus >= 200 && responseStatus < 300;

    const item = {
      logId: uuidv4(),
      leadId: logData.leadId,
      source: logData.source,
      accessToken: logData.accessToken || null,
      dedupCheck: logData.dedupCheck || null,
      isDuplicate: String(logData.isDuplicate || false), // String for GSI
      duplicateStatus: logData.duplicateStatus || '0',
      requestPayload: logData.requestPayload,
      responseStatus: responseStatus,
      responseBody: logData.responseBody,
      errorDetails: logData.errorDetails || null,
      retryCount: logData.retryCount || 0,
      isSuccess: String(isSuccess), // String for GSI
      createdAt: new Date().toISOString(),
      updatedAt: new Date().toISOString()
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

  // Find by leadId with date sorting
  static async findByLeadId(leadId, options = {}) {
    const { limit = 100, sortAscending = false } = options;

    const result = await docClient.send(new QueryCommand({
      TableName: TABLE_NAME,
      IndexName: 'leadId-createdAt-index',
      KeyConditionExpression: 'leadId = :leadId',
      ExpressionAttributeValues: { ':leadId': leadId },
      ScanIndexForward: sortAscending,
      Limit: limit
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

  // Get duplicate leads
  static async getDuplicateLeads(startDate, endDate, options = {}) {
    const { limit = 100 } = options;

    const result = await docClient.send(new QueryCommand({
      TableName: TABLE_NAME,
      IndexName: 'isDuplicate-isSuccess-index',
      KeyConditionExpression: 'isDuplicate = :isDuplicate',
      FilterExpression: 'createdAt BETWEEN :startDate AND :endDate',
      ExpressionAttributeValues: {
        ':isDuplicate': 'true',
        ':startDate': startDate,
        ':endDate': endDate
      },
      Limit: limit
    }));

    return result.Items || [];
  }

  // Get success rate
  static async getSuccessRate(startDate, endDate) {
    // Get all logs in date range
    const allLogsParams = {
      TableName: TABLE_NAME,
      FilterExpression: 'createdAt BETWEEN :startDate AND :endDate',
      ExpressionAttributeValues: {
        ':startDate': startDate,
        ':endDate': endDate
      }
    };

    const allLogs = await docClient.send(new ScanCommand(allLogsParams));
    const total = allLogs.Items?.length || 0;

    // Get successful logs
    const successfulLogsParams = {
      TableName: TABLE_NAME,
      FilterExpression: 'createdAt BETWEEN :startDate AND :endDate AND isSuccess = :isSuccess AND isDuplicate = :isDuplicate',
      ExpressionAttributeValues: {
        ':startDate': startDate,
        ':endDate': endDate,
        ':isSuccess': 'true',
        ':isDuplicate': 'false'
      }
    };

    const successfulLogs = await docClient.send(new ScanCommand(successfulLogsParams));
    const successful = successfulLogs.Items?.length || 0;

    return {
      total,
      successful,
      successRate: total > 0 ? (successful / total) * 100 : 0
    };
  }

  // Check if lead was successful
  static isLeadSuccessful(log) {
    return (
      log.isSuccess === 'true' &&
      log.isDuplicate === 'false' &&
      log.responseBody?.info?.status === 100
    );
  }

  // Check if verification was sent
  static verificationSent(log) {
    return log.responseBody?.info?.message?.includes('Verification code sent');
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

module.exports = IndiaLendsResponseLog;
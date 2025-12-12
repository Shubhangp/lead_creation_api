const { docClient } = require('../dynamodb');
const { PutCommand, GetCommand, QueryCommand, ScanCommand, DeleteCommand, UpdateCommand, BatchWriteCommand } = require('@aws-sdk/lib-dynamodb');
const { v4: uuidv4 } = require('uuid');

const RCS_QUEUE_TABLE = 'rcs_queue';

class RCSQueue {
  // Create RCS queue entry
  static async create(queueData) {
    const item = {
      queueId: uuidv4(),
      leadId: queueData.leadId,
      phone: queueData.phone,
      rcsType: queueData.rcsType,
      lenderName: queueData.lenderName || null,
      priority: queueData.priority || null,
      scheduledTime: queueData.scheduledTime instanceof Date 
        ? queueData.scheduledTime.toISOString() 
        : queueData.scheduledTime,
      status: queueData.status || 'PENDING',
      attempts: queueData.attempts || 0,
      sentAt: queueData.sentAt || null,
      failureReason: queueData.failureReason || null,
      rcsPayload: queueData.rcsPayload || null,
      rcsResponse: queueData.rcsResponse || null,
      createdAt: new Date().toISOString(),
      updatedAt: new Date().toISOString()
    };

    await docClient.send(new PutCommand({
      TableName: RCS_QUEUE_TABLE,
      Item: item
    }));

    return item;
  }

  // Find by ID
  static async findById(queueId) {
    const result = await docClient.send(new GetCommand({
      TableName: RCS_QUEUE_TABLE,
      Key: { queueId }
    }));

    return result.Item || null;
  }

  // Find by leadId (requires GSI: leadId-rcsType-index)
  static async findByLeadId(leadId, options = {}) {
    const { limit = 100, rcsType } = options;

    let keyConditionExpression = 'leadId = :leadId';
    const expressionAttributeValues = { ':leadId': leadId };

    if (rcsType) {
      keyConditionExpression += ' AND rcsType = :rcsType';
      expressionAttributeValues[':rcsType'] = rcsType;
    }

    const result = await docClient.send(new QueryCommand({
      TableName: RCS_QUEUE_TABLE,
      IndexName: 'leadId-rcsType-index',
      KeyConditionExpression: keyConditionExpression,
      ExpressionAttributeValues: expressionAttributeValues,
      Limit: limit,
      ScanIndexForward: false
    }));

    return result.Items || [];
  }

  // Find by status and scheduled time (requires GSI: status-scheduledTime-index)
  static async findByStatusAndScheduledTime(status, scheduledTimeBefore, options = {}) {
    const { limit = 100 } = options;

    const result = await docClient.send(new QueryCommand({
      TableName: RCS_QUEUE_TABLE,
      IndexName: 'status-scheduledTime-index',
      KeyConditionExpression: '#status = :status AND scheduledTime <= :scheduledTime',
      ExpressionAttributeNames: {
        '#status': 'status'
      },
      ExpressionAttributeValues: {
        ':status': status,
        ':scheduledTime': scheduledTimeBefore instanceof Date 
          ? scheduledTimeBefore.toISOString() 
          : scheduledTimeBefore
      },
      Limit: limit
    }));

    return result.Items || [];
  }

  // Count documents by status
  static async countByStatus(status) {
    const result = await docClient.send(new QueryCommand({
      TableName: RCS_QUEUE_TABLE,
      IndexName: 'status-scheduledTime-index',
      KeyConditionExpression: '#status = :status',
      ExpressionAttributeNames: {
        '#status': 'status'
      },
      ExpressionAttributeValues: {
        ':status': status
      },
      Select: 'COUNT'
    }));

    return result.Count || 0;
  }

  // Get aggregate stats (simulating MongoDB aggregate)
  static async getAggregateStats() {
    const statuses = ['PENDING', 'SENT', 'FAILED', 'CANCELLED'];
    const stats = [];

    for (const status of statuses) {
      const count = await this.countByStatus(status);
      if (count > 0) {
        stats.push({
          _id: status,
          count: count
        });
      }
    }

    return stats;
  }

  // Find recent messages
  static async findRecent(limit = 10) {
    const result = await docClient.send(new ScanCommand({
      TableName: RCS_QUEUE_TABLE,
      Limit: limit,
      ProjectionExpression: 'queueId, leadId, rcsType, #status, createdAt',
      ExpressionAttributeNames: {
        '#status': 'status'
      }
    }));

    // Sort by createdAt descending
    const items = result.Items || [];
    items.sort((a, b) => new Date(b.createdAt) - new Date(a.createdAt));

    return items.slice(0, limit);
  }

  // Update queue entry
  static async update(queueId, updates) {
    const updateExpressions = [];
    const expressionAttributeNames = {};
    const expressionAttributeValues = {};

    // Add updatedAt
    updates.updatedAt = new Date().toISOString();

    Object.keys(updates).forEach((key, index) => {
      const placeholder = `#field${index}`;
      const valuePlaceholder = `:value${index}`;
      updateExpressions.push(`${placeholder} = ${valuePlaceholder}`);
      
      // Handle reserved keywords
      if (key === 'status') {
        expressionAttributeNames[placeholder] = 'status';
      } else {
        expressionAttributeNames[placeholder] = key;
      }
      
      expressionAttributeValues[valuePlaceholder] = updates[key];
    });

    const result = await docClient.send(new UpdateCommand({
      TableName: RCS_QUEUE_TABLE,
      Key: { queueId },
      UpdateExpression: `SET ${updateExpressions.join(', ')}`,
      ExpressionAttributeNames: expressionAttributeNames,
      ExpressionAttributeValues: expressionAttributeValues,
      ReturnValues: 'ALL_NEW'
    }));

    return result.Attributes;
  }

  // Update many by leadId and status
  static async updateManyByLeadIdAndStatus(leadId, currentStatus, updates) {
    // First, find all matching items
    const items = await this.findByLeadId(leadId);
    const matchingItems = items.filter(item => item.status === currentStatus);

    let modifiedCount = 0;
    
    // Update each item
    for (const item of matchingItems) {
      await this.update(item.queueId, updates);
      modifiedCount++;
    }

    return { modifiedCount };
  }

  // Update many by status and attempts
  static async updateManyByStatusAndAttempts(status, maxAttempts, updates) {
    // Scan to find matching items
    const result = await docClient.send(new ScanCommand({
      TableName: RCS_QUEUE_TABLE,
      FilterExpression: '#status = :status AND attempts < :maxAttempts',
      ExpressionAttributeNames: {
        '#status': 'status'
      },
      ExpressionAttributeValues: {
        ':status': status,
        ':maxAttempts': maxAttempts
      }
    }));

    const items = result.Items || [];
    let modifiedCount = 0;

    // Update each item
    for (const item of items) {
      await this.update(item.queueId, updates);
      modifiedCount++;
    }

    return { modifiedCount };
  }

  // Delete queue entry
  static async delete(queueId) {
    await docClient.send(new DeleteCommand({
      TableName: RCS_QUEUE_TABLE,
      Key: { queueId }
    }));

    return { deleted: true };
  }

  // Find all (paginated)
  static async findAll(options = {}) {
    const { limit = 100, lastEvaluatedKey } = options;

    const params = {
      TableName: RCS_QUEUE_TABLE,
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

module.exports = {
  RCSQueue
};
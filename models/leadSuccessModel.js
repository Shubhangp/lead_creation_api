// models/leadSuccessModel.js
const { docClient } = require('../dynamodb');
const { 
  PutCommand, 
  GetCommand, 
  QueryCommand, 
  UpdateCommand, 
  DeleteCommand, 
  ScanCommand
} = require('@aws-sdk/lib-dynamodb');
const { v4: uuidv4 } = require('uuid');

const TABLE_NAME = 'lead_success';

class LeadSuccess {
  // Create lead success entry
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
      createdAt: new Date().toISOString()
    };

    await docClient.send(new PutCommand({
      TableName: TABLE_NAME,
      Item: item
    }));

    return item;
  }

  // Find by ID
  static async findById(successId) {
    const result = await docClient.send(new GetCommand({
      TableName: TABLE_NAME,
      Key: { successId }
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

    return result.Items?.[0] || null;
  }

  // Find by phone
  static async findByPhone(phone) {
    const result = await docClient.send(new QueryCommand({
      TableName: TABLE_NAME,
      IndexName: 'phone-index',
      KeyConditionExpression: 'phone = :phone',
      ExpressionAttributeValues: { ':phone': phone }
    }));

    return result.Items || [];
  }

  // Find by PAN number
  static async findByPanNumber(panNumber) {
    const result = await docClient.send(new QueryCommand({
      TableName: TABLE_NAME,
      IndexName: 'panNumber-index',
      KeyConditionExpression: 'panNumber = :panNumber',
      ExpressionAttributeValues: { ':panNumber': panNumber }
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

  // Update lender status
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

  // Update by leadId (find first, then update)
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

  // Find or create by leadId
  static async findOrCreate(leadData) {
    // Try to find existing record
    const existing = await this.findByLeadId(leadData.leadId);
    
    if (existing) {
      return { record: existing, created: false };
    }

    // Create new record
    const newRecord = await this.create(leadData);
    return { record: newRecord, created: true };
  }

  // Get statistics by lender
  static async getLenderStats(lenderName, startDate, endDate) {
    const validLenders = [
      'OVLY', 'FREO', 'LendingPlate', 'ZYPE', 'FINTIFI', 
      'FATAKPAY', 'RAMFINCROP', 'MyMoneyMantra', 'INDIALENDS', 'CRMPaisa', 'SML', 'MPOKKET'
    ];

    if (!validLenders.includes(lenderName)) {
      throw new Error(`Invalid lender name: ${lenderName}`);
    }

    const params = {
      TableName: TABLE_NAME,
      FilterExpression: `#lender = :true AND createdAt BETWEEN :startDate AND :endDate`,
      ExpressionAttributeNames: {
        '#lender': lenderName
      },
      ExpressionAttributeValues: {
        ':true': true,
        ':startDate': startDate,
        ':endDate': endDate
      }
    };

    const result = await docClient.send(new ScanCommand(params));
    
    return {
      lender: lenderName,
      successCount: result.Items?.length || 0,
      leads: result.Items || []
    };
  }

  // Get all lender statistics
  static async getAllLenderStats(startDate, endDate) {
    const lenders = [
      'OVLY', 'FREO', 'LendingPlate', 'ZYPE', 'FINTIFI', 
      'FATAKPAY', 'RAMFINCROP', 'MyMoneyMantra', 'INDIALENDS', 'CRMPaisa', 'SML', 'MPOKKET'
    ];

    const stats = {};

    for (const lender of lenders) {
      const lenderStats = await this.getLenderStats(lender, startDate, endDate);
      stats[lender] = lenderStats.successCount;
    }

    return stats;
  }

  // Find all success records (paginated)
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

  // Delete by ID
  static async deleteById(successId) {
    await docClient.send(new DeleteCommand({
      TableName: TABLE_NAME,
      Key: { successId }
    }));

    return { deleted: true };
  }

  // Count successes by date range
  static async countByDateRange(startDate, endDate) {
    const params = {
      TableName: TABLE_NAME,
      FilterExpression: 'createdAt BETWEEN :startDate AND :endDate',
      ExpressionAttributeValues: {
        ':startDate': startDate,
        ':endDate': endDate
      },
      Select: 'COUNT'
    };

    const result = await docClient.send(new ScanCommand(params));
    return result.Count || 0;
  }
}

module.exports = LeadSuccess;
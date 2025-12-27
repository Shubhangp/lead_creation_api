// models/leadModel.js
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

const TABLE_NAME = 'leads';

class Lead {
  // Validation helper
  static validate(data) {
    const errors = [];

    // Required fields
    if (!data.source) errors.push('Source is required');
    if (!data.fullName) errors.push('Full name is required');
    if (!data.phone) errors.push('Phone is required');
    if (!data.email) errors.push('Email is required');
    if (!data.panNumber) errors.push('PAN number is required');
    // if (data.consent !== true && data.consent !== false) {
    //   errors.push('Consent is required');
    // }

    // String length validations
    if (data.fullName && (data.fullName.length < 1 || data.fullName.length > 100)) {
      errors.push('Full name must be between 1 and 100 characters');
    }
    if (data.firstName && (data.firstName.length < 1 || data.firstName.length > 50)) {
      errors.push('First name must be between 1 and 50 characters');
    }
    if (data.lastName && (data.lastName.length < 1 || data.lastName.length > 50)) {
      errors.push('Last name must be between 1 and 50 characters');
    }

    // Email validation
    const emailRegex = /[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z0-9]{2,4}$/;
    if (data.email && !emailRegex.test(data.email)) {
      errors.push('Invalid email format');
    }

    // PAN number validation
    const panRegex = /^[A-Z]{5}[0-9]{4}[A-Z]$/;
    if (data.panNumber && !panRegex.test(data.panNumber)) {
      errors.push('Invalid PAN number format (e.g., ABCDE1234F)');
    }

    // Age validation
    if (data.age !== undefined && (data.age < 18 || data.age > 120)) {
      errors.push('Age must be between 18 and 120');
    }

    // Date of birth validation
    if (data.dateOfBirth) {
      const dob = new Date(data.dateOfBirth);
      if (dob > new Date()) {
        errors.push('Date of birth cannot be in the future');
      }
    }

    // Credit score validation
    if (data.creditScore !== undefined && (data.creditScore < 300 || data.creditScore > 900)) {
      errors.push('Credit score must be between 300 and 900');
    }

    if (errors.length > 0) {
      const error = new Error('Validation failed');
      error.errors = errors;
      throw error;
    }
  }

  // Create lead with uniqueness check
  static async create(leadData) {
    // Validate data
    this.validate(leadData);

    // Check if phone already exists
    const existingPhone = await this.findByPhone(leadData.phone);
    if (existingPhone) {
      const error = new Error('Phone number already exists');
      error.code = 'DUPLICATE_PHONE';
      throw error;
    }

    // Check if PAN already exists
    const existingPan = await this.findByPanNumber(leadData.panNumber);
    if (existingPan) {
      const error = new Error('PAN number already exists');
      error.code = 'DUPLICATE_PAN';
      throw error;
    }

    const item = {
      leadId: uuidv4(),
      source: leadData.source,
      fullName: leadData.fullName,
      firstName: leadData.firstName || null,
      lastName: leadData.lastName || null,
      phone: leadData.phone,
      email: leadData.email,
      age: leadData.age || null,
      dateOfBirth: leadData.dateOfBirth ? new Date(leadData.dateOfBirth).toISOString() : null,
      gender: leadData.gender || null,
      panNumber: leadData.panNumber,
      jobType: leadData.jobType || null,
      businessType: leadData.businessType || null,
      salary: leadData.salary || null,
      creditScore: leadData.creditScore || null,
      cibilScore: leadData.cibilScore || null,
      address: leadData.address || null,
      pincode: leadData.pincode || null,
      consent: leadData.consent,
      createdAt: new Date().toISOString()
    };

    await docClient.send(new PutCommand({
      TableName: TABLE_NAME,
      Item: item
    }));

    return item;
  }

  // Find by ID
  static async findById(leadId) {
    const result = await docClient.send(new GetCommand({
      TableName: TABLE_NAME,
      Key: { leadId }
    }));

    return result.Item || null;
  }

  // Find by phone (GSI)
  static async findByPhone(phone) {
    const result = await docClient.send(new QueryCommand({
      TableName: TABLE_NAME,
      IndexName: 'phone-index',
      KeyConditionExpression: 'phone = :phone',
      ExpressionAttributeValues: { ':phone': phone },
      Limit: 1
    }));

    return result.Items?.[0] || null;
  }

  // Find by PAN number (GSI)
  static async findByPanNumber(panNumber) {
    const result = await docClient.send(new QueryCommand({
      TableName: TABLE_NAME,
      IndexName: 'panNumber-index',
      KeyConditionExpression: 'panNumber = :panNumber',
      ExpressionAttributeValues: { ':panNumber': panNumber },
      Limit: 1
    }));

    return result.Items?.[0] || null;
  }

  // Find by source with date sorting
  static async findBySource(source, options = {}) {
    const {
      limit = 100,
      startDate,
      endDate,
      sortAscending = false,
      lastEvaluatedKey = null
    } = options;

    // Use ExpressionAttributeNames to handle 'source' reserved keyword
    let keyConditionExpression = '#source = :source';
    const expressionAttributeNames = {
      '#source': 'source'
    };
    const expressionAttributeValues = { ':source': source };

    // Add date range if provided
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
      ExpressionAttributeNames: expressionAttributeNames,
      ExpressionAttributeValues: expressionAttributeValues,
      ScanIndexForward: sortAscending,
      Limit: limit
    };

    // Add pagination token if provided
    if (lastEvaluatedKey) {
      params.ExclusiveStartKey = lastEvaluatedKey;
    }

    const result = await docClient.send(new QueryCommand(params));

    // Return both items and pagination token
    return {
      items: result.Items || [],
      lastEvaluatedKey: result.LastEvaluatedKey || null,
      count: result.Count || 0
    };
  }

  // Find all leads (paginated)
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

  // Update lead
  static async updateById(leadId, updates) {
    // Validate updates if they contain validatable fields
    if (Object.keys(updates).length > 0) {
      // Get existing lead for validation
      const existingLead = await this.findById(leadId);
      if (!existingLead) {
        throw new Error('Lead not found');
      }

      // Merge and validate
      const mergedData = { ...existingLead, ...updates };
      this.validate(mergedData);

      // Check uniqueness if phone or PAN is being updated
      if (updates.phone && updates.phone !== existingLead.phone) {
        const existingPhone = await this.findByPhone(updates.phone);
        if (existingPhone && existingPhone.leadId !== leadId) {
          const error = new Error('Phone number already exists');
          error.code = 'DUPLICATE_PHONE';
          throw error;
        }
      }

      if (updates.panNumber && updates.panNumber !== existingLead.panNumber) {
        const existingPan = await this.findByPanNumber(updates.panNumber);
        if (existingPan && existingPan.leadId !== leadId) {
          const error = new Error('PAN number already exists');
          error.code = 'DUPLICATE_PAN';
          throw error;
        }
      }
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
      Key: { leadId },
      UpdateExpression: `SET ${updateExpression.join(', ')}`,
      ExpressionAttributeNames: expressionAttributeNames,
      ExpressionAttributeValues: expressionAttributeValues,
      ReturnValues: 'ALL_NEW'
    }));

    return result.Attributes;
  }

  static async updateByIdNoValidation(leadId, updates) {
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
      Key: { leadId },
      UpdateExpression: `SET ${updateExpression.join(', ')}`,
      ExpressionAttributeNames: expressionAttributeNames,
      ExpressionAttributeValues: expressionAttributeValues,
      ReturnValues: 'ALL_NEW'
    }));

    return result.Attributes;
  }

  // Delete lead
  static async deleteById(leadId) {
    await docClient.send(new DeleteCommand({
      TableName: TABLE_NAME,
      Key: { leadId }
    }));

    return { deleted: true };
  }

  // Query by multiple filters (uses Scan - use sparingly)
  static async findByFilters(filters = {}, options = {}) {
    const { limit = 100 } = options;

    const filterExpressions = [];
    const expressionAttributeNames = {};
    const expressionAttributeValues = {};

    Object.keys(filters).forEach((key, index) => {
      filterExpressions.push(`#field${index} = :value${index}`);
      expressionAttributeNames[`#field${index}`] = key;
      expressionAttributeValues[`:value${index}`] = filters[key];
    });

    const params = {
      TableName: TABLE_NAME,
      Limit: limit
    };

    if (filterExpressions.length > 0) {
      params.FilterExpression = filterExpressions.join(' AND ');
      params.ExpressionAttributeNames = expressionAttributeNames;
      params.ExpressionAttributeValues = expressionAttributeValues;
    }

    const result = await docClient.send(new ScanCommand(params));

    return result.Items || [];
  }

  // Count leads by source
  static async countBySource(source) {
    const result = await docClient.send(new QueryCommand({
      TableName: TABLE_NAME,
      IndexName: 'source-createdAt-index',
      KeyConditionExpression: '#source = :source',
      ExpressionAttributeNames: {
        '#source': 'source'
      },
      ExpressionAttributeValues: { ':source': source },
      Select: 'COUNT'
    }));

    return result.Count || 0;
  }
}

module.exports = Lead;
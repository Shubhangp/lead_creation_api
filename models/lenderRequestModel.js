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

const TABLE_NAME = 'lender_requests';

class LenderRequest {
  // Validation helper
  static validate(data) {
    const errors = [];

    // Required fields
    if (!data.companyName) errors.push('Company name is required');
    if (!data.email) errors.push('Email is required');
    if (!data.contactNumber) errors.push('Contact number is required');

    // String length validations
    if (data.companyName && (data.companyName.length < 2 || data.companyName.length > 200)) {
      errors.push('Company name must be between 2 and 200 characters');
    }

    // Email validation
    const emailRegex = /^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$/;
    if (data.email && !emailRegex.test(data.email)) {
      errors.push('Invalid email format');
    }

    // Contact number validation (10 digits)
    const phoneRegex = /^[0-9]{10}$/;
    if (data.contactNumber && !phoneRegex.test(data.contactNumber)) {
      errors.push('Contact number must be 10 digits');
    }

    // Website validation (optional)
    if (data.website && data.website.length > 0) {
      const urlRegex = /^(https?:\/\/)?([\da-z\.-]+)\.([a-z\.]{2,6})([\/\w \.-]*)*\/?$/;
      if (!urlRegex.test(data.website)) {
        errors.push('Invalid website URL format');
      }
    }

    if (errors.length > 0) {
      const error = new Error('Validation failed');
      error.errors = errors;
      throw error;
    }
  }

  // Create lender request with uniqueness check
  static async create(requestData) {
    this.validate(requestData);

    // Check if email already exists
    const existingEmail = await this.findByEmail(requestData.email);
    if (existingEmail) {
      const error = new Error('Email already exists');
      error.code = 'DUPLICATE_EMAIL';
      throw error;
    }

    const item = {
      lenderRequestId: uuidv4(),
      companyName: requestData.companyName,
      website: requestData.website || null,
      email: requestData.email,
      contactNumber: requestData.contactNumber,
      status: 'pending', // pending, approved, rejected
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
  static async findById(lenderRequestId) {
    const result = await docClient.send(new GetCommand({
      TableName: TABLE_NAME,
      Key: { lenderRequestId }
    }));

    return result.Item || null;
  }

  // Find by email (GSI)
  static async findByEmail(email) {
    const result = await docClient.send(new QueryCommand({
      TableName: TABLE_NAME,
      IndexName: 'email-index',
      KeyConditionExpression: 'email = :email',
      ExpressionAttributeValues: { ':email': email },
      Limit: 1
    }));

    return result.Items?.[0] || null;
  }

  // Find all lender requests (paginated)
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

  // Update lender request
  static async updateById(lenderRequestId, updates) {
    if (Object.keys(updates).length > 0) {
      const existingRequest = await this.findById(lenderRequestId);
      if (!existingRequest) {
        throw new Error('Lender request not found');
      }

      const mergedData = { ...existingRequest, ...updates };
      this.validate(mergedData);

      // Check uniqueness if email is being updated
      if (updates.email && updates.email !== existingRequest.email) {
        const existingEmail = await this.findByEmail(updates.email);
        if (existingEmail && existingEmail.lenderRequestId !== lenderRequestId) {
          const error = new Error('Email already exists');
          error.code = 'DUPLICATE_EMAIL';
          throw error;
        }
      }
    }

    // Add updatedAt timestamp
    updates.updatedAt = new Date().toISOString();

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
      Key: { lenderRequestId },
      UpdateExpression: `SET ${updateExpression.join(', ')}`,
      ExpressionAttributeNames: expressionAttributeNames,
      ExpressionAttributeValues: expressionAttributeValues,
      ReturnValues: 'ALL_NEW'
    }));

    return result.Attributes;
  }

  // Delete lender request
  static async deleteById(lenderRequestId) {
    await docClient.send(new DeleteCommand({
      TableName: TABLE_NAME,
      Key: { lenderRequestId }
    }));

    return { deleted: true };
  }

  // Query by status
  static async findByStatus(status, options = {}) {
    const { limit = 100 } = options;

    const result = await docClient.send(new QueryCommand({
      TableName: TABLE_NAME,
      IndexName: 'status-index',
      KeyConditionExpression: '#status = :status',
      ExpressionAttributeNames: { '#status': 'status' },
      ExpressionAttributeValues: { ':status': status },
      Limit: limit
    }));

    return result.Items || [];
  }

  // Query by filters (uses Scan - use sparingly)
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

  // Update status
  static async updateStatus(lenderRequestId, status) {
    if (!['pending', 'approved', 'rejected'].includes(status)) {
      throw new Error('Invalid status. Must be: pending, approved, or rejected');
    }

    return await this.updateById(lenderRequestId, { status });
  }
}

module.exports = LenderRequest;
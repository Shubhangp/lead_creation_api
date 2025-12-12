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

const TABLE_NAME = 'leads_uat';

class LeadUAT {
  // Validation helper
  static validate(data) {
    const errors = [];

    // Required fields
    if (!data.source) errors.push('Source is required');
    if (!data.fullName) errors.push('Full name is required');
    if (!data.phone) errors.push('Phone is required');
    if (!data.email) errors.push('Email is required');
    if (!data.panNumber) errors.push('PAN number is required');
    if (data.consent !== true && data.consent !== false) {
      errors.push('Consent is required');
    }

    // String length validations
    if (data.fullName && (data.fullName.length < 2 || data.fullName.length > 100)) {
      errors.push('Full name must be between 2 and 100 characters');
    }
    if (data.firstName && (data.firstName.length < 2 || data.firstName.length > 50)) {
      errors.push('First name must be between 2 and 50 characters');
    }
    if (data.lastName && (data.lastName.length < 2 || data.lastName.length > 50)) {
      errors.push('Last name must be between 2 and 50 characters');
    }

    // Email validation
    const emailRegex = /[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,4}$/;
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

    // Credit score validation (UAT uses 300-850 range)
    if (data.creditScore !== undefined && (data.creditScore < 300 || data.creditScore > 850)) {
      errors.push('Credit score must be between 300 and 850');
    }

    if (errors.length > 0) {
      const error = new Error('Validation failed');
      error.errors = errors;
      throw error;
    }
  }

  // Create UAT lead with uniqueness check
  static async create(leadData) {
    this.validate(leadData);

    // Check if PAN already exists
    const existingPan = await this.findByPanNumber(leadData.panNumber);
    if (existingPan) {
      const error = new Error('PAN number already exists');
      error.code = 'DUPLICATE_PAN';
      throw error;
    }

    const item = {
      leadUATId: uuidv4(),
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
  static async findById(leadUATId) {
    const result = await docClient.send(new GetCommand({
      TableName: TABLE_NAME,
      Key: { leadUATId }
    }));

    return result.Item || null;
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

  // Find all UAT leads (paginated)
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

  // Update UAT lead
  static async updateById(leadUATId, updates) {
    if (Object.keys(updates).length > 0) {
      const existingLead = await this.findById(leadUATId);
      if (!existingLead) {
        throw new Error('UAT lead not found');
      }

      const mergedData = { ...existingLead, ...updates };
      this.validate(mergedData);

      // Check uniqueness if PAN is being updated
      if (updates.panNumber && updates.panNumber !== existingLead.panNumber) {
        const existingPan = await this.findByPanNumber(updates.panNumber);
        if (existingPan && existingPan.leadUATId !== leadUATId) {
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
      Key: { leadUATId },
      UpdateExpression: `SET ${updateExpression.join(', ')}`,
      ExpressionAttributeNames: expressionAttributeNames,
      ExpressionAttributeValues: expressionAttributeValues,
      ReturnValues: 'ALL_NEW'
    }));

    return result.Attributes;
  }

  // Delete UAT lead
  static async deleteById(leadUATId) {
    await docClient.send(new DeleteCommand({
      TableName: TABLE_NAME,
      Key: { leadUATId }
    }));

    return { deleted: true };
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
}

module.exports = LeadUAT;
// models/excelLeadModel.js
const { docClient } = require('../dynamodb');
const { 
  PutCommand, 
  GetCommand, 
  QueryCommand, 
  UpdateCommand, 
  DeleteCommand, 
  ScanCommand,
  BatchWriteCommand 
} = require('@aws-sdk/lib-dynamodb');
const { v4: uuidv4 } = require('uuid');

const TABLE_NAME = 'excel_leads';

class ExcelLead {
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

  // Create single excel lead with uniqueness check
  static async create(leadData) {
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
      excelLeadId: uuidv4(),
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

  // Batch create excel leads (for bulk imports)
  static async createBulk(leadsArray) {
    const results = {
      successful: [],
      failed: []
    };

    // Process in batches of 25 (DynamoDB limit)
    const batchSize = 25;
    
    for (let i = 0; i < leadsArray.length; i += batchSize) {
      const batch = leadsArray.slice(i, i + batchSize);
      const putRequests = [];

      for (const leadData of batch) {
        try {
          // Validate each lead
          this.validate(leadData);

          // Check for duplicates (you might want to skip this for performance in bulk imports)
          const existingPhone = await this.findByPhone(leadData.phone);
          const existingPan = await this.findByPanNumber(leadData.panNumber);

          if (existingPhone || existingPan) {
            results.failed.push({
              data: leadData,
              error: existingPhone ? 'Duplicate phone' : 'Duplicate PAN'
            });
            continue;
          }

          const item = {
            excelLeadId: uuidv4(),
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

          putRequests.push({
            PutRequest: { Item: item }
          });

          results.successful.push(item);

        } catch (err) {
          results.failed.push({
            data: leadData,
            error: err.message
          });
        }
      }

      // Execute batch write
      if (putRequests.length > 0) {
        try {
          await docClient.send(new BatchWriteCommand({
            RequestItems: {
              [TABLE_NAME]: putRequests
            }
          }));
        } catch (err) {
          console.error('Batch write error:', err);
          // Mark all items in this batch as failed
          putRequests.forEach(req => {
            const item = req.PutRequest.Item;
            results.failed.push({
              data: item,
              error: 'Batch write failed'
            });
            // Remove from successful
            const index = results.successful.findIndex(s => s.excelLeadId === item.excelLeadId);
            if (index > -1) results.successful.splice(index, 1);
          });
        }
      }
    }

    return results;
  }

  // Find by ID
  static async findById(excelLeadId) {
    const result = await docClient.send(new GetCommand({
      TableName: TABLE_NAME,
      Key: { excelLeadId }
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

  // Find all excel leads (paginated)
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

  // Update excel lead
  static async updateById(excelLeadId, updates) {
    if (Object.keys(updates).length > 0) {
      const existingLead = await this.findById(excelLeadId);
      if (!existingLead) {
        throw new Error('Excel lead not found');
      }

      const mergedData = { ...existingLead, ...updates };
      this.validate(mergedData);

      // Check uniqueness if phone or PAN is being updated
      if (updates.phone && updates.phone !== existingLead.phone) {
        const existingPhone = await this.findByPhone(updates.phone);
        if (existingPhone && existingPhone.excelLeadId !== excelLeadId) {
          const error = new Error('Phone number already exists');
          error.code = 'DUPLICATE_PHONE';
          throw error;
        }
      }

      if (updates.panNumber && updates.panNumber !== existingLead.panNumber) {
        const existingPan = await this.findByPanNumber(updates.panNumber);
        if (existingPan && existingPan.excelLeadId !== excelLeadId) {
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
      Key: { excelLeadId },
      UpdateExpression: `SET ${updateExpression.join(', ')}`,
      ExpressionAttributeNames: expressionAttributeNames,
      ExpressionAttributeValues: expressionAttributeValues,
      ReturnValues: 'ALL_NEW'
    }));

    return result.Attributes;
  }

  // Delete excel lead
  static async deleteById(excelLeadId) {
    await docClient.send(new DeleteCommand({
      TableName: TABLE_NAME,
      Key: { excelLeadId }
    }));

    return { deleted: true };
  }

  // Batch delete
  static async deleteBulk(excelLeadIds) {
    const batchSize = 25;
    const results = { successful: [], failed: [] };

    for (let i = 0; i < excelLeadIds.length; i += batchSize) {
      const batch = excelLeadIds.slice(i, i + batchSize);
      const deleteRequests = batch.map(id => ({
        DeleteRequest: { Key: { excelLeadId: id } }
      }));

      try {
        await docClient.send(new BatchWriteCommand({
          RequestItems: {
            [TABLE_NAME]: deleteRequests
          }
        }));
        results.successful.push(...batch);
      } catch (err) {
        console.error('Batch delete error:', err);
        results.failed.push(...batch);
      }
    }

    return results;
  }

  // Count leads by source
  static async countBySource(source) {
    const result = await docClient.send(new QueryCommand({
      TableName: TABLE_NAME,
      IndexName: 'source-createdAt-index',
      KeyConditionExpression: 'source = :source',
      ExpressionAttributeValues: { ':source': source },
      Select: 'COUNT'
    }));

    return result.Count || 0;
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
}

module.exports = ExcelLead;
const { docClient } = require('../dynamodb');
const {
  PutCommand,
  GetCommand,
  QueryCommand,
  UpdateCommand,
  DeleteCommand,
  BatchWriteCommand,
  TransactWriteCommand,
} = require('@aws-sdk/lib-dynamodb');
const { v4: uuidv4 } = require('uuid');

const TABLE_NAME = 'excel_leads';
const CONSTRAINTS_TABLE = 'excel_leads_unique_constraints';

class ExcelLead {

  static validate(data) {
    const errors = [];

    if (!data.source)    errors.push('Source is required');
    if (!data.fullName)  errors.push('Full name is required');
    if (!data.phone)     errors.push('Phone is required');
    if (!data.email)     errors.push('Email is required');
    if (!data.panNumber) errors.push('PAN number is required');
    if (data.consent !== true && data.consent !== false)
      errors.push('Consent is required');

    if (data.fullName && (data.fullName.length < 1 || data.fullName.length > 100))
      errors.push('Full name must be between 1 and 100 characters');
    if (data.firstName && (data.firstName.length < 1 || data.firstName.length > 50))
      errors.push('First name must be between 1 and 50 characters');
    if (data.lastName && (data.lastName.length < 1 || data.lastName.length > 50))
      errors.push('Last name must be between 1 and 50 characters');

    const emailRegex = /[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z0-9]{2,4}$/;
    if (data.email && !emailRegex.test(data.email))
      errors.push('Invalid email format');

    const panRegex = /^[A-Z]{5}[0-9]{4}[A-Z]$/;
    if (data.panNumber && !panRegex.test(data.panNumber))
      errors.push('Invalid PAN number format (e.g., ABCDE1234F)');

    if (data.age !== undefined && (data.age < 18 || data.age > 120))
      errors.push('Age must be between 18 and 120');

    if (data.dateOfBirth && new Date(data.dateOfBirth) > new Date())
      errors.push('Date of birth cannot be in the future');

    if (errors.length > 0) {
      const error = new Error('Validation failed');
      error.errors = errors;
      throw error;
    }
  }

  // ─── Build item ──────────────────────────────────────────────────────────────

  static _buildItem(leadData, excelLeadId = uuidv4()) {
    return {
      excelLeadId,
      source:       leadData.source,
      fullName:     leadData.fullName,
      firstName:    leadData.firstName    || null,
      lastName:     leadData.lastName     || null,
      phone:        leadData.phone,
      email:        leadData.email,
      age:          leadData.age          || null,
      dateOfBirth:  leadData.dateOfBirth  ? new Date(leadData.dateOfBirth).toISOString() : null,
      gender:       leadData.gender       || null,
      panNumber:    leadData.panNumber,
      jobType:      leadData.jobType      || null,
      businessType: leadData.businessType || null,
      salary:       leadData.salary       || null,
      creditScore:  leadData.creditScore  || null,
      cibilScore:   leadData.cibilScore   || null,
      address:      leadData.address      || null,
      pincode:      leadData.pincode      || null,
      consent:      leadData.consent,
      createdAt:    new Date().toISOString()
    };
  }

  // ─── Sentinel helpers ────────────────────────────────────────────────────────

  static async _reserveConstraints(phone, panNumber, excelLeadId) {
    try {
      await docClient.send(new TransactWriteCommand({
        TransactItems: [
          {
            Put: {
              TableName: CONSTRAINTS_TABLE,
              Item: { constraintKey: `phone#${phone}`, excelLeadId, createdAt: new Date().toISOString() },
              ConditionExpression: 'attribute_not_exists(constraintKey)'
            }
          },
          {
            Put: {
              TableName: CONSTRAINTS_TABLE,
              Item: { constraintKey: `pan#${panNumber}`, excelLeadId, createdAt: new Date().toISOString() },
              ConditionExpression: 'attribute_not_exists(constraintKey)'
            }
          }
        ]
      }));
    } catch (err) {
      if (err.name === 'TransactionCanceledException') {
        const reasons = err.CancellationReasons || [];
        const phoneDup = reasons[0]?.Code === 'ConditionalCheckFailed';
        const message  = phoneDup ? 'Phone number already exists' : 'PAN number already exists';
        const code     = phoneDup ? 'DUPLICATE_PHONE' : 'DUPLICATE_PAN';
        throw Object.assign(new Error(message), { code });
      }
      throw err;
    }
  }

  static async _releaseConstraints(phone, panNumber) {
    await docClient.send(new BatchWriteCommand({
      RequestItems: {
        [CONSTRAINTS_TABLE]: [
          { DeleteRequest: { Key: { constraintKey: `phone#${phone}` } } },
          { DeleteRequest: { Key: { constraintKey: `pan#${panNumber}` } } }
        ]
      }
    }));
  }

  static async _constraintExists(key) {
    const result = await docClient.send(new GetCommand({
      TableName: CONSTRAINTS_TABLE,
      Key: { constraintKey: key }
    }));
    return result.Item || null;
  }

  // ─── Create single ───────────────────────────────────────────────────────────

  static async create(leadData) {
    this.validate(leadData);
    const item = this._buildItem(leadData);

    await this._reserveConstraints(item.phone, item.panNumber, item.excelLeadId);

    try {
      await docClient.send(new PutCommand({
        TableName: TABLE_NAME,
        Item: item,
        ConditionExpression: 'attribute_not_exists(excelLeadId)'
      }));
    } catch (err) {
      await this._releaseConstraints(item.phone, item.panNumber).catch(() => {});
      throw err;
    }

    return item;
  }

  // ─── Bulk create ─────────────────────────────────────────────────────────────

  static async createBulk(leadsArray) {
    const results = { successful: [], failed: [] };

    // Step 1 — Validate all synchronously (free, no I/O)
    const validLeads = [];
    for (const lead of leadsArray) {
      try {
        this.validate(lead);
        validLeads.push({ lead, item: this._buildItem(lead) });
      } catch (err) {
        results.failed.push({ data: lead, error: err.message, errors: err.errors });
      }
    }

    if (validLeads.length === 0) return results;

    // Step 2 — Reserve constraints in parallel (zero reads, atomic writes)
    const reservationResults = await Promise.allSettled(
      validLeads.map(({ item }) =>
        this._reserveConstraints(item.phone, item.panNumber, item.excelLeadId)
      )
    );

    // Step 3 — Separate passed vs failed
    const toInsert = [];
    validLeads.forEach(({ lead, item }, i) => {
      const r = reservationResults[i];
      if (r.status === 'fulfilled') {
        toInsert.push(item);
      } else {
        results.failed.push({ data: lead, error: r.reason?.message || 'Constraint check failed' });
      }
    });

    if (toInsert.length === 0) return results;

    // Step 4 — Batch write in chunks of 25 with unprocessed retry
    const BATCH_SIZE = 25;
    for (let i = 0; i < toInsert.length; i += BATCH_SIZE) {
      const chunk = toInsert.slice(i, i + BATCH_SIZE);

      try {
        let response = await docClient.send(new BatchWriteCommand({
          RequestItems: { [TABLE_NAME]: chunk.map(item => ({ PutRequest: { Item: item } })) }
        }));

        // Retry unprocessed with exponential backoff
        let unprocessed = response.UnprocessedItems?.[TABLE_NAME];
        let attempt = 0;
        while (unprocessed?.length > 0 && attempt < 5) {
          await new Promise(r => setTimeout(r, Math.pow(2, attempt) * 100));
          response = await docClient.send(new BatchWriteCommand({
            RequestItems: { [TABLE_NAME]: unprocessed }
          }));
          unprocessed = response.UnprocessedItems?.[TABLE_NAME];
          attempt++;
        }

        if (unprocessed?.length > 0) {
          const failedIds = new Set(unprocessed.map(r => r.PutRequest.Item.excelLeadId));
          for (const req of unprocessed) {
            const item = req.PutRequest.Item;
            await this._releaseConstraints(item.phone, item.panNumber).catch(() => {});
            results.failed.push({ data: item, error: 'Batch write failed after retries' });
          }
          results.successful.push(...chunk.filter(item => !failedIds.has(item.excelLeadId)));
        } else {
          results.successful.push(...chunk);
        }

      } catch (err) {
        console.error('Batch write error:', err);
        await Promise.allSettled(chunk.map(item => this._releaseConstraints(item.phone, item.panNumber)));
        chunk.forEach(item => results.failed.push({ data: item, error: 'Batch write failed' }));
      }
    }

    return results;
  }

  // ─── Read by ID ──────────────────────────────────────────────────────────────

  static async findById(excelLeadId) {
    const result = await docClient.send(new GetCommand({
      TableName: TABLE_NAME,
      Key: { excelLeadId }
    }));
    return result.Item || null;
  }

  // ─── Read by phone (KEYS_ONLY GSI → GetCommand for full item) ───────────────

  static async findByPhone(phone, { fullItem = true } = {}) {
    const result = await docClient.send(new QueryCommand({
      TableName: TABLE_NAME,
      IndexName: 'phone-index',
      KeyConditionExpression: 'phone = :phone',
      ExpressionAttributeValues: { ':phone': phone },
      Limit: 1
    }));
    const key = result.Items?.[0];
    if (!key) return null;
    if (!fullItem) return key;
    return this.findById(key.excelLeadId);
  }

  // ─── Read by PAN (KEYS_ONLY GSI → GetCommand for full item) ─────────────────

  static async findByPanNumber(panNumber, { fullItem = true } = {}) {
    const result = await docClient.send(new QueryCommand({
      TableName: TABLE_NAME,
      IndexName: 'panNumber-index',
      KeyConditionExpression: 'panNumber = :panNumber',
      ExpressionAttributeValues: { ':panNumber': panNumber },
      Limit: 1
    }));
    const key = result.Items?.[0];
    if (!key) return null;
    if (!fullItem) return key;
    return this.findById(key.excelLeadId);
  }

  // ─── Read by source with pagination (INCLUDE GSI) ───────────────────────────
  // Primary "list all" method — always query by source, never scan.

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
      Limit: Math.min(limit, 1000)
    };

    if (lastEvaluatedKey) params.ExclusiveStartKey = lastEvaluatedKey;

    const result = await docClient.send(new QueryCommand(params));
    return {
      items: result.Items || [],
      lastEvaluatedKey: result.LastEvaluatedKey || null,
      count: result.Count || 0
    };
  }

  // ─── Count by source ─────────────────────────────────────────────────────────

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

  // ─── Update ──────────────────────────────────────────────────────────────────

  static async updateById(excelLeadId, updates) {
    if (!updates || Object.keys(updates).length === 0)
      throw new Error('No updates provided');

    const existingLead = await this.findById(excelLeadId);
    if (!existingLead) throw new Error('Excel lead not found');

    const mergedData = { ...existingLead, ...updates };
    this.validate(mergedData);

    if (updates.phone && updates.phone !== existingLead.phone) {
      const existing = await this._constraintExists(`phone#${updates.phone}`);
      if (existing && existing.excelLeadId !== excelLeadId)
        throw Object.assign(new Error('Phone number already exists'), { code: 'DUPLICATE_PHONE' });

      await docClient.send(new TransactWriteCommand({
        TransactItems: [
          { Delete: { TableName: CONSTRAINTS_TABLE, Key: { constraintKey: `phone#${existingLead.phone}` } } },
          {
            Put: {
              TableName: CONSTRAINTS_TABLE,
              Item: { constraintKey: `phone#${updates.phone}`, excelLeadId, createdAt: new Date().toISOString() },
              ConditionExpression: 'attribute_not_exists(constraintKey)'
            }
          }
        ]
      }));
    }

    if (updates.panNumber && updates.panNumber !== existingLead.panNumber) {
      const existing = await this._constraintExists(`pan#${updates.panNumber}`);
      if (existing && existing.excelLeadId !== excelLeadId)
        throw Object.assign(new Error('PAN number already exists'), { code: 'DUPLICATE_PAN' });

      await docClient.send(new TransactWriteCommand({
        TransactItems: [
          { Delete: { TableName: CONSTRAINTS_TABLE, Key: { constraintKey: `pan#${existingLead.panNumber}` } } },
          {
            Put: {
              TableName: CONSTRAINTS_TABLE,
              Item: { constraintKey: `pan#${updates.panNumber}`, excelLeadId, createdAt: new Date().toISOString() },
              ConditionExpression: 'attribute_not_exists(constraintKey)'
            }
          }
        ]
      }));
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

  // ─── Delete ──────────────────────────────────────────────────────────────────

  static async deleteById(excelLeadId) {
    const existing = await this.findById(excelLeadId);
    if (!existing) return { deleted: false };

    await Promise.all([
      docClient.send(new DeleteCommand({ TableName: TABLE_NAME, Key: { excelLeadId } })),
      this._releaseConstraints(existing.phone, existing.panNumber)
    ]);

    return { deleted: true };
  }

  static async deleteBulk(excelLeadIds) {
    const results = { successful: [], failed: [] };
    const BATCH_SIZE = 25;

    const fetchResults = await Promise.allSettled(
      excelLeadIds.map(id => this.findById(id))
    );

    const validItems = [];
    excelLeadIds.forEach((id, i) => {
      const r = fetchResults[i];
      if (r.status === 'fulfilled' && r.value) validItems.push(r.value);
      else results.failed.push(id);
    });

    await Promise.allSettled(
      validItems.map(item => this._releaseConstraints(item.phone, item.panNumber))
    );

    for (let i = 0; i < validItems.length; i += BATCH_SIZE) {
      const chunk = validItems.slice(i, i + BATCH_SIZE);
      try {
        await docClient.send(new BatchWriteCommand({
          RequestItems: {
            [TABLE_NAME]: chunk.map(item => ({
              DeleteRequest: { Key: { excelLeadId: item.excelLeadId } }
            }))
          }
        }));
        results.successful.push(...chunk.map(item => item.excelLeadId));
      } catch (err) {
        console.error('Batch delete error:', err);
        results.failed.push(...chunk.map(item => item.excelLeadId));
      }
    }

    return results;
  }
}

module.exports = ExcelLead;
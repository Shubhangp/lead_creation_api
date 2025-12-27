// models/leadDistributionStatsModel.js
const { docClient } = require('../dynamodb');
const { 
  PutCommand, 
  GetCommand, 
  QueryCommand, 
  UpdateCommand,
  ScanCommand,
  BatchWriteCommand
} = require('@aws-sdk/lib-dynamodb');
const { v4: uuidv4 } = require('uuid');

const TABLE_NAME = 'lead_distribution_stats';
const PROCESSING_TABLE_NAME = 'lead_distribution_processing';

class LeadDistributionStats {
  /**
   * Create a new distribution batch record
   */
  static async createBatch(batchData) {
    const item = {
      batchId: uuidv4(),
      lender: batchData.lender,
      
      // Store applied filters for reference
      appliedFilters: {
        sources: batchData.filters.sources || [],
        dateRange: {
          startDate: batchData.filters.startDate || null,
          endDate: batchData.filters.endDate || null
        },
        ageRange: {
          minAge: batchData.filters.minAge || null,
          maxAge: batchData.filters.maxAge || null
        },
        salaryRange: {
          minSalary: batchData.filters.minSalary || null,
          maxSalary: batchData.filters.maxSalary || null
        },
        jobTypes: batchData.filters.jobTypes || [],
        creditScoreRange: {
          minCreditScore: batchData.filters.minCreditScore || null,
          maxCreditScore: batchData.filters.maxCreditScore || null
        },
        gender: batchData.filters.gender || null,
        pincodes: batchData.filters.pincodes || []
      },
      
      totalLeads: 0,
      successfulLeads: 0,
      failedLeads: 0,
      processedLeads: 0,
      status: 'PROCESSING', // PROCESSING, COMPLETED, FAILED, PARTIAL
      startedAt: new Date().toISOString(),
      completedAt: null,
      lastUpdatedAt: new Date().toISOString(),
      errorSummary: {
        totalErrors: 0,
        sampleErrors: [] // Store up to 10 sample errors
      },
      createdAt: new Date().toISOString()
    };

    await docClient.send(new PutCommand({
      TableName: TABLE_NAME,
      Item: item
    }));

    return item;
  }

  /**
   * Update batch statistics (throttle-safe)
   */
  static async updateBatchStats(batchId, updates) {
    // Add lastUpdatedAt to track when stats were last modified
    updates.lastUpdatedAt = new Date().toISOString();
    
    const updateExpression = [];
    const expressionAttributeNames = {};
    const expressionAttributeValues = {};

    Object.keys(updates).forEach((key, index) => {
      updateExpression.push(`#field${index} = :value${index}`);
      expressionAttributeNames[`#field${index}`] = key;
      expressionAttributeValues[`:value${index}`] = updates[key];
    });

    try {
      const result = await docClient.send(new UpdateCommand({
        TableName: TABLE_NAME,
        Key: { batchId },
        UpdateExpression: `SET ${updateExpression.join(', ')}`,
        ExpressionAttributeNames: expressionAttributeNames,
        ExpressionAttributeValues: expressionAttributeValues,
        ReturnValues: 'ALL_NEW'
      }));

      return result.Attributes;
    } catch (error) {
      if (error.name === 'ProvisionedThroughputExceededException') {
        console.warn(`[${batchId}] Throughput exceeded on batch update, retrying...`);
        await new Promise(resolve => setTimeout(resolve, 1000));
        return this.updateBatchStats(batchId, updates);
      }
      throw error;
    }
  }

  /**
   * Increment counters atomically with retry logic
   */
  static async incrementCounters(batchId, counters) {
    const updateExpression = [];
    const expressionAttributeNames = {};
    const expressionAttributeValues = {};

    Object.keys(counters).forEach((key, index) => {
      updateExpression.push(`#field${index} = if_not_exists(#field${index}, :zero) + :value${index}`);
      expressionAttributeNames[`#field${index}`] = key;
      expressionAttributeValues[`:value${index}`] = counters[key];
    });
    
    expressionAttributeValues[':zero'] = 0;
    expressionAttributeValues[':now'] = new Date().toISOString();
    updateExpression.push('#lastUpdatedAt = :now');
    expressionAttributeNames['#lastUpdatedAt'] = 'lastUpdatedAt';

    try {
      const result = await docClient.send(new UpdateCommand({
        TableName: TABLE_NAME,
        Key: { batchId },
        UpdateExpression: `SET ${updateExpression.join(', ')}`,
        ExpressionAttributeNames: expressionAttributeNames,
        ExpressionAttributeValues: expressionAttributeValues,
        ReturnValues: 'ALL_NEW'
      }));

      return result.Attributes;
    } catch (error) {
      if (error.name === 'ProvisionedThroughputExceededException') {
        console.warn(`[${batchId}] Throughput exceeded on counter increment, retrying...`);
        await new Promise(resolve => setTimeout(resolve, 500));
        return this.incrementCounters(batchId, counters);
      }
      throw error;
    }
  }

  /**
   * Add error to batch (limited to 10 samples to avoid item size limits)
   */
  static async addError(batchId, error) {
    try {
      await docClient.send(new UpdateCommand({
        TableName: TABLE_NAME,
        Key: { batchId },
        UpdateExpression: `
          SET errorSummary.totalErrors = if_not_exists(errorSummary.totalErrors, :zero) + :one,
              errorSummary.sampleErrors = list_append(
                if_not_exists(errorSummary.sampleErrors, :empty_list), 
                :error
              ),
              lastUpdatedAt = :now
        `,
        ConditionExpression: 'size(errorSummary.sampleErrors) < :maxErrors',
        ExpressionAttributeValues: {
          ':error': [{
            message: error.message.substring(0, 500), // Limit error message length
            timestamp: new Date().toISOString()
          }],
          ':empty_list': [],
          ':zero': 0,
          ':one': 1,
          ':maxErrors': 10,
          ':now': new Date().toISOString()
        }
      }));
    } catch (conditionError) {
      // If we've hit the limit of sample errors, just increment the counter
      if (conditionError.name === 'ConditionalCheckFailedException') {
        await this.incrementCounters(batchId, { 'errorSummary.totalErrors': 1 });
      } else if (conditionError.name === 'ProvisionedThroughputExceededException') {
        console.warn(`[${batchId}] Throughput exceeded on error add, skipping...`);
      } else {
        throw conditionError;
      }
    }
  }

  /**
   * Track individual lead processing in separate table (to avoid main table throughput issues)
   */
  static async recordLeadProcessing(batchId, leadId, status, errorMessage = null) {
    const item = {
      processingId: `${batchId}#${leadId}`,
      batchId: batchId,
      leadId: leadId,
      status: status, // 'success' or 'failed'
      errorMessage: errorMessage,
      processedAt: new Date().toISOString()
    };

    try {
      await docClient.send(new PutCommand({
        TableName: PROCESSING_TABLE_NAME,
        Item: item
      }));
    } catch (error) {
      if (error.name === 'ProvisionedThroughputExceededException') {
        console.warn(`[${batchId}] Throughput exceeded on lead tracking, will retry...`);
        await new Promise(resolve => setTimeout(resolve, 200));
        return this.recordLeadProcessing(batchId, leadId, status, errorMessage);
      }
      // Don't throw - this is optional tracking
      console.error(`Failed to record lead processing: ${error.message}`);
    }
  }

  /**
   * Batch record lead processing (more efficient)
   */
  static async recordLeadProcessingBatch(records) {
    if (records.length === 0) return;

    // DynamoDB BatchWrite supports max 25 items
    const chunks = [];
    for (let i = 0; i < records.length; i += 25) {
      chunks.push(records.slice(i, i + 25));
    }

    for (const chunk of chunks) {
      const requests = chunk.map(record => ({
        PutRequest: {
          Item: {
            processingId: `${record.batchId}#${record.leadId}`,
            batchId: record.batchId,
            leadId: record.leadId,
            status: record.status,
            errorMessage: record.errorMessage || null,
            processedAt: new Date().toISOString()
          }
        }
      }));

      try {
        await docClient.send(new BatchWriteCommand({
          RequestItems: {
            [PROCESSING_TABLE_NAME]: requests
          }
        }));
      } catch (error) {
        if (error.name === 'ProvisionedThroughputExceededException') {
          console.warn(`Throughput exceeded on batch write, waiting...`);
          await new Promise(resolve => setTimeout(resolve, 1000));
          // Retry this chunk
          await this.recordLeadProcessingBatch(chunk);
        } else {
          console.error(`Failed to batch record leads: ${error.message}`);
        }
      }

      // Small delay between chunks
      await new Promise(resolve => setTimeout(resolve, 100));
    }
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

  /**
   * Get processed leads for a batch from processing table
   */
  static async getBatchProcessedLeads(batchId, options = {}) {
    const { limit = 100, lastEvaluatedKey } = options;

    const params = {
      TableName: PROCESSING_TABLE_NAME,
      IndexName: 'batchId-processedAt-index',
      KeyConditionExpression: 'batchId = :batchId',
      ExpressionAttributeValues: { ':batchId': batchId },
      Limit: limit
    };

    if (lastEvaluatedKey) {
      params.ExclusiveStartKey = lastEvaluatedKey;
    }

    const result = await docClient.send(new QueryCommand(params));

    return {
      items: result.Items || [],
      lastEvaluatedKey: result.LastEvaluatedKey
    };
  }
}

module.exports = LeadDistributionStats;
const { docClient } = require('../dynamodb');
const { PutCommand, GetCommand, QueryCommand, ScanCommand, DeleteCommand, UpdateCommand } = require('@aws-sdk/lib-dynamodb');
const { v4: uuidv4 } = require('uuid');

const TABLE_NAME = 'distribution_rules';

class DistributionRule {
  // Create distribution rule entry
  static async create(ruleData) {
    // Sort lender priority before saving
    if (ruleData.rcsConfig && ruleData.rcsConfig.lenderPriority) {
      ruleData.rcsConfig.lenderPriority.sort((a, b) => a.priority - b.priority);
    }

    const item = {
      ruleId: uuidv4(),
      source: ruleData.source,
      active: ruleData.active !== undefined ? ruleData.active : true,
      rules: ruleData.rules,
      rcsConfig: ruleData.rcsConfig || {
        enabled: true,
        lenderPriority: [],
        zetCampaign: {
          enabled: true,
          dayDelay: 1
        },
        operatingHours: {
          startTime: '10:00',
          endTime: '19:00',
          timezone: 'Asia/Kolkata'
        }
      },
      lastUpdated: new Date().toISOString(),
      lastUpdatedBy: ruleData.lastUpdatedBy || null,
      createdAt: new Date().toISOString(),
      updatedAt: new Date().toISOString()
    };

    await docClient.send(new PutCommand({
      TableName: TABLE_NAME,
      Item: item,
      ConditionExpression: 'attribute_not_exists(ruleId)'
    }));

    return item;
  }

  // Find by source (requires GSI: source-index)
  static async findBySource(source) {
    const result = await docClient.send(new QueryCommand({
      TableName: TABLE_NAME,
      IndexName: 'source-index',
      KeyConditionExpression: '#src = :source',  // FIXED: Use alias
      ExpressionAttributeNames: {
        '#src': 'source'  // FIXED: Map reserved keyword
      },
      ExpressionAttributeValues: { ':source': source }
    }));

    return result.Items && result.Items.length > 0 ? result.Items[0] : null;
  }

  // Find active rule by source
  static async findActiveBySource(source) {
    const result = await docClient.send(new QueryCommand({
      TableName: TABLE_NAME,
      IndexName: 'source-index',
      KeyConditionExpression: '#src = :source',
      FilterExpression: '#active = :active',
      ExpressionAttributeNames: {
        '#src': 'source',
        '#active': 'active'
      },
      ExpressionAttributeValues: {
        ':source': source,
        ':active': true
      }
    }));

    return result.Items?.[0] || null;
  }

  // Find by ID
  static async findById(ruleId) {
    const result = await docClient.send(new GetCommand({
      TableName: TABLE_NAME,
      Key: { ruleId }
    }));

    return result.Item || null;
  }

  // Find all distribution rules (paginated)
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

  // Update distribution rule by source
  static async updateBySource(source, updates) {
    // First find the item
    const existing = await this.findBySource(source);

    if (!existing) {  // FIXED: Changed condition
      throw new Error('Distribution rule not found');
    }

    const ruleId = existing.ruleId;  // FIXED: Use ruleId, not distributionRuleId

    // Then update by ID
    const updateExpression = [];
    const expressionAttributeNames = {};
    const expressionAttributeValues = {};

    Object.keys(updates).forEach((key, index) => {
      updateExpression.push(`#field${index} = :value${index}`);
      expressionAttributeNames[`#field${index}`] = key;
      expressionAttributeValues[`:value${index}`] = updates[key];
    });

    // Add timestamp
    updateExpression.push(`#updatedAt = :updatedAt`);
    expressionAttributeNames[`#updatedAt`] = 'updatedAt';
    expressionAttributeValues[`:updatedAt`] = new Date().toISOString();

    const result = await docClient.send(new UpdateCommand({
      TableName: TABLE_NAME,
      Key: { ruleId },  // FIXED: Use ruleId
      UpdateExpression: `SET ${updateExpression.join(', ')}`,
      ExpressionAttributeNames: expressionAttributeNames,
      ExpressionAttributeValues: expressionAttributeValues,
      ReturnValues: 'ALL_NEW'
    }));

    return result.Attributes;
  }

  // Delete distribution rule by source
  static async deleteBySource(source) {
    // First, find the rule by source
    const existingRule = await this.findBySource(source);

    if (!existingRule) {
      return null;
    }

    await docClient.send(new DeleteCommand({
      TableName: TABLE_NAME,
      Key: { ruleId: existingRule.ruleId }
    }));

    return existingRule;
  }

  // Delete by ID
  static async delete(ruleId) {
    await docClient.send(new DeleteCommand({
      TableName: TABLE_NAME,
      Key: { ruleId }
    }));

    return { deleted: true };
  }
}

module.exports = DistributionRule;
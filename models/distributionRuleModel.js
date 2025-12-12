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
      KeyConditionExpression: 'source = :source',
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
    // First, find the rule by source
    const existingRule = await this.findBySource(source);

    if (!existingRule) {
      return null;
    }

    // Sort lender priority if updating rcsConfig
    if (updates.rcsConfig && updates.rcsConfig.lenderPriority) {
      updates.rcsConfig.lenderPriority.sort((a, b) => a.priority - b.priority);
    }

    const updateExpressions = [];
    const expressionAttributeNames = {};
    const expressionAttributeValues = {};

    // Add updatedAt and lastUpdated
    updates.updatedAt = new Date().toISOString();
    updates.lastUpdated = new Date().toISOString();

    Object.keys(updates).forEach((key, index) => {
      const placeholder = `#field${index}`;
      const valuePlaceholder = `:value${index}`;
      updateExpressions.push(`${placeholder} = ${valuePlaceholder}`);
      expressionAttributeNames[placeholder] = key;
      expressionAttributeValues[valuePlaceholder] = updates[key];
    });

    const result = await docClient.send(new UpdateCommand({
      TableName: TABLE_NAME,
      Key: { ruleId: existingRule.ruleId },
      UpdateExpression: `SET ${updateExpressions.join(', ')}`,
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
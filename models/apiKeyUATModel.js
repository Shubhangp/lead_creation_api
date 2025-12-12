const { docClient } = require('../dynamodb');
const { PutCommand, GetCommand, QueryCommand, ScanCommand, DeleteCommand, UpdateCommand } = require('@aws-sdk/lib-dynamodb');
const { v4: uuidv4 } = require('uuid');

const TABLE_NAME = 'api_keys_uat';

class ApiKeyUAT {
  // Create API key entry
  static async create(apiKeyData) {
    const item = {
      apiKeyUATId: uuidv4(),
      sourceName: apiKeyData.sourceName,
      apiKey: apiKeyData.apiKey,
      createdAt: new Date().toISOString()
    };

    await docClient.send(new PutCommand({
      TableName: TABLE_NAME,
      Item: item,
      ConditionExpression: 'attribute_not_exists(apiKeyUATId)'
    }));

    return item;
  }

  // Find by sourceName (requires GSI: sourceName-index)
  static async findBySourceName(sourceName) {
    const result = await docClient.send(new QueryCommand({
      TableName: TABLE_NAME,
      IndexName: 'sourceName-index',
      KeyConditionExpression: 'sourceName = :sourceName',
      ExpressionAttributeValues: { ':sourceName': sourceName }
    }));

    return result.Items && result.Items.length > 0 ? result.Items[0] : null;
  }

  // Find by apiKey (requires GSI: apiKey-index)
  static async findByApiKey(apiKey) {
    const result = await docClient.send(new QueryCommand({
      TableName: TABLE_NAME,
      IndexName: 'apiKey-index',
      KeyConditionExpression: 'apiKey = :apiKey',
      ExpressionAttributeValues: { ':apiKey': apiKey }
    }));

    return result.Items && result.Items.length > 0 ? result.Items[0] : null;
  }

  // Find by ID
  static async findById(apiKeyUATId) {
    const result = await docClient.send(new GetCommand({
      TableName: TABLE_NAME,
      Key: { apiKeyUATId }
    }));

    return result.Item || null;
  }

  // Find all API keys (paginated)
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

  // Delete API key
  static async delete(apiKeyUATId) {
    await docClient.send(new DeleteCommand({
      TableName: TABLE_NAME,
      Key: { apiKeyUATId }
    }));

    return { deleted: true };
  }

  // Update API key
  static async update(apiKeyUATId, updates) {
    const updateExpressions = [];
    const expressionAttributeNames = {};
    const expressionAttributeValues = {};

    Object.keys(updates).forEach((key, index) => {
      const placeholder = `#field${index}`;
      const valuePlaceholder = `:value${index}`;
      updateExpressions.push(`${placeholder} = ${valuePlaceholder}`);
      expressionAttributeNames[placeholder] = key;
      expressionAttributeValues[valuePlaceholder] = updates[key];
    });

    const result = await docClient.send(new UpdateCommand({
      TableName: TABLE_NAME,
      Key: { apiKeyUATId },
      UpdateExpression: `SET ${updateExpressions.join(', ')}`,
      ExpressionAttributeNames: expressionAttributeNames,
      ExpressionAttributeValues: expressionAttributeValues,
      ReturnValues: 'ALL_NEW'
    }));

    return result.Attributes;
  }
}

module.exports = ApiKeyUAT;
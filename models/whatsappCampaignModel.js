'use strict';

const { v4: uuidv4 } = require('uuid');
const { docClient } = require('../dynamodb');
const {
  PutCommand,
  GetCommand,
  QueryCommand,
  UpdateCommand,
} = require('@aws-sdk/lib-dynamodb');
const { LEAD_SOURCES_DEFAULT } = require('../config/registry');

const TABLE_NAME = 'whatsapp_campaigns';

class WhatsappCampaign {
  static async create(data) {
    const now = new Date().toISOString();
    const item = {
      campaignId:       data.campaignId || uuidv4(),
      source:           data.source,
      fileName:         data.fileName,
      fileKey:          data.fileKey,
      fileSize:         data.fileSize || 0,
      totalRecords:     data.totalRecords || 0,
      messagesSent:     data.messagesSent || 0,
      delivered:        data.delivered || 0,
      ctr:              data.ctr || 0,
      status:           data.status || 'Processing',
      uploadedByUserId: data.uploadedByUserId || null,
      uploadedByName:   data.uploadedByName || null,
      errorMessage:     data.errorMessage || null,
      createdAt:        now,
      updatedAt:        now,
    };

    Object.keys(item).forEach(k => {
      if (item[k] === null || item[k] === undefined) delete item[k];
    });

    await docClient.send(new PutCommand({ TableName: TABLE_NAME, Item: item }));
    return item;
  }

  static async findById(campaignId) {
    const result = await docClient.send(new GetCommand({
      TableName: TABLE_NAME,
      Key: { campaignId },
    }));
    return result.Item || null;
  }

  static async findBySource(source) {
    const result = await docClient.send(new QueryCommand({
      TableName: TABLE_NAME,
      IndexName: 'source-createdAt-index',
      KeyConditionExpression: '#src = :source',
      ExpressionAttributeNames: { '#src': 'source' },
      ExpressionAttributeValues: { ':source': source },
      ScanIndexForward: false, // newest first
    }));
    return result.Items || [];
  }

  static async findAll() {
    const results = await Promise.all(
      LEAD_SOURCES_DEFAULT.map(source => WhatsappCampaign.findBySource(source))
    );
    return results
      .flat()
      .sort((a, b) => (b.createdAt || '').localeCompare(a.createdAt || ''));
  }


  static async update(campaignId, updates) {
    const now = new Date().toISOString();
    const allowed = ['messagesSent', 'delivered', 'ctr', 'status', 'errorMessage'];

    const sets = ['updatedAt = :now'];
    const names = {};
    const values = { ':now': now };

    for (const key of allowed) {
      if (updates[key] !== undefined) {
        const nameKey = `#${key}`;
        const valueKey = `:${key}`;
        sets.push(`${nameKey} = ${valueKey}`);
        names[nameKey] = key;
        values[valueKey] = updates[key];
      }
    }

    const result = await docClient.send(new UpdateCommand({
      TableName: TABLE_NAME,
      Key: { campaignId },
      UpdateExpression: `SET ${sets.join(', ')}`,
      ExpressionAttributeNames: names,
      ExpressionAttributeValues: values,
      ReturnValues: 'ALL_NEW',
    }));

    return result.Attributes;
  }
}

module.exports = WhatsappCampaign;

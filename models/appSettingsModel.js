'use strict';

/**
 * Small key/value settings store (table `app_settings`, PK `key`) for values
 * admins change from the dashboard without a code deploy — e.g. lender rate
 * limits. The table is created automatically the first time a setting is saved.
 */

const { docClient } = require('../dynamodb');
const { GetCommand, PutCommand } = require('@aws-sdk/lib-dynamodb');
const {
  DynamoDBClient, CreateTableCommand, DescribeTableCommand,
} = require('@aws-sdk/client-dynamodb');

const TABLE_NAME = process.env.APP_SETTINGS_TABLE || 'app_settings';
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

let tableReady = null;

async function ensureTable() {
  if (tableReady) return tableReady;
  tableReady = (async () => {
    const ddb = new DynamoDBClient({
      region: process.env.AWS_REGION || 'ap-south-1',
      credentials: process.env.AWS_ACCESS_KEY_ID ? {
        accessKeyId: process.env.AWS_ACCESS_KEY_ID, secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY,
      } : undefined,
    });
    try {
      await ddb.send(new DescribeTableCommand({ TableName: TABLE_NAME }));
      return;
    } catch (e) {
      if (e.name !== 'ResourceNotFoundException') throw e;
    }
    console.log(`[AppSettings] creating table ${TABLE_NAME}`);
    try {
      await ddb.send(new CreateTableCommand({
        TableName: TABLE_NAME,
        BillingMode: 'PAY_PER_REQUEST',
        AttributeDefinitions: [{ AttributeName: 'key', AttributeType: 'S' }],
        KeySchema: [{ AttributeName: 'key', KeyType: 'HASH' }],
      }));
    } catch (e) {
      if (e.name !== 'ResourceInUseException') throw e;
    }
    for (let i = 0; i < 60; i++) {
      const d = await ddb.send(new DescribeTableCommand({ TableName: TABLE_NAME }));
      if (d.Table.TableStatus === 'ACTIVE') return;
      await sleep(2000);
    }
    throw new Error(`${TABLE_NAME} did not become ACTIVE`);
  })().catch((e) => { tableReady = null; throw e; });
  return tableReady;
}

class AppSettings {
  /** Value for `key`, or null when not set / table not created yet. */
  static async get(key) {
    try {
      const r = await docClient.send(new GetCommand({ TableName: TABLE_NAME, Key: { key }, ConsistentRead: true }));
      return r.Item ? r.Item.value : null;
    } catch (e) {
      if (e.name === 'ResourceNotFoundException') return null;
      throw e;
    }
  }

  static async set(key, value, meta = {}) {
    await ensureTable();
    const item = { key, value, updatedAt: new Date().toISOString(), ...meta };
    await docClient.send(new PutCommand({ TableName: TABLE_NAME, Item: item }));
    return item;
  }
}

module.exports = AppSettings;

const { DynamoDBClient } = require('@aws-sdk/client-dynamodb');
const { DynamoDBDocumentClient } = require('@aws-sdk/lib-dynamodb');
const dotenv = require('dotenv');

dotenv.config({ path: './config.env' });

// Configure DynamoDB client
const client = new DynamoDBClient({
  region: process.env.AWS_REGION || 'ap-south-1',
  credentials: {
    accessKeyId: process.env.AWS_ACCESS_KEY_ID,
    secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY
  },
  maxAttempts: 3,
  requestTimeout: 10000
});

// Create document client for easier operations
const docClient = DynamoDBDocumentClient.from(client, {
  marshallOptions: {
    removeUndefinedValues: true,
    convertEmptyValues: true
  }
});

// Test connection
const testConnection = async () => {
  try {
    const { ListTablesCommand } = require('@aws-sdk/client-dynamodb');
    await client.send(new ListTablesCommand({}));
    console.log('DynamoDB connection successful');
    return true;
  } catch (err) {
    console.error('DynamoDB connection error:', err.message);
    throw err;
  }
};

module.exports = { docClient, testConnection };
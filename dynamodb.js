const { DynamoDBClient } = require('@aws-sdk/client-dynamodb');
const { DynamoDBDocumentClient } = require('@aws-sdk/lib-dynamodb');
const { NodeHttpHandler } = require('@smithy/node-http-handler');
const https = require('https');
const dotenv = require('dotenv');

dotenv.config({ path: './config.env' });

// Configure DynamoDB client
//
// NOTE ON maxSockets: the AWS SDK v3's default NodeHttpHandler caps the HTTP
// connection pool at 50 sockets. Under real production lead-ingestion volume
// this pool saturates (seen in prod as "@smithy/node-http-handler:WARN -
// socket usage at capacity=50 and N additional requests are enqueued"),
// which backs up outgoing DynamoDB calls and surfaces upstream as nginx 499s
// (client gave up waiting). Raising maxSockets and enabling keep-alive (so
// connections are reused instead of re-established per request) gives the
// client enough headroom for real traffic. This is a client-side HTTP
// setting only — it has no AWS billing impact.
const client = new DynamoDBClient({
  region: process.env.AWS_REGION || 'ap-south-1',
  credentials: {
    accessKeyId: process.env.AWS_ACCESS_KEY_ID,
    secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY
  },
  maxAttempts: 3,
  requestTimeout: 10000,
  requestHandler: new NodeHttpHandler({
    connectionTimeout: 5000,
    socketTimeout: 10000,
    httpsAgent: new https.Agent({
      maxSockets: 200,
      keepAlive: true,
    }),
  }),
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
const dotenv = require('dotenv');

dotenv.config({ path: './config.env' });

const app = require('./app');
const { testConnection } = require('./dynamodb');

// Test DynamoDB connection
testConnection()
  .then(() => {
    const port = process.env.PORT || 1203;

    const server = app.listen(port, () => {
      console.log(`App running on port ${port}...`);
    });

    process.on('unhandledRejection', (err) => {
      console.error('UNHANDLED REJECTION! Shutting down...', err);
      server.close(() => process.exit(1));
    });
  })
  .catch((err) => {
    console.error('Failed to connect to DynamoDB:', err.message);
    process.exit(1);
  });
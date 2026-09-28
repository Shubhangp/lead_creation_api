'use strict';

// utils/s3Upload.js
//
// S3 helper for the WhatsApp Campaigns feature. Reuses the SAME AWS
// credentials already configured for DynamoDB (dynamodb.js) — no separate
// IAM user is required. The bucket itself is created out-of-band by an
// operator; this module simply reads its name from AWS_S3_BUCKET_NAME.
//
// Required env vars (config.env):
//   AWS_S3_BUCKET_NAME   - name of the S3 bucket to upload campaign files to
//   AWS_REGION           - already used by dynamodb.js (default ap-south-1)
//   AWS_ACCESS_KEY_ID    - already used by dynamodb.js
//   AWS_SECRET_ACCESS_KEY - already used by dynamodb.js

const dotenv = require('dotenv');
dotenv.config({ path: './config.env' });

const { S3Client, PutObjectCommand, GetObjectCommand } = require('@aws-sdk/client-s3');
const { getSignedUrl } = require('@aws-sdk/s3-request-presigner');

const s3Client = new S3Client({
  region: process.env.AWS_REGION || 'ap-south-1',
  credentials: {
    accessKeyId: process.env.AWS_ACCESS_KEY_ID,
    secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY,
  },
});

const BUCKET_NAME = process.env.AWS_S3_BUCKET_NAME;

function isS3Configured() {
  return !!BUCKET_NAME;
}

/**
 * Upload a file buffer to S3.
 * @param {Buffer} buffer
 * @param {string} key - S3 object key, e.g. `whatsapp-campaigns/<source>/<uuid>-<filename>`
 * @param {string} contentType
 * @returns {Promise<{ key: string }>}
 */
async function uploadFileToS3(buffer, key, contentType) {
  if (!isS3Configured()) {
    throw new Error('S3 is not configured. Set AWS_S3_BUCKET_NAME in config.env.');
  }

  await s3Client.send(new PutObjectCommand({
    Bucket: BUCKET_NAME,
    Key: key,
    Body: buffer,
    ContentType: contentType,
  }));

  return { key };
}

/**
 * Generate a presigned GET URL for downloading/viewing a campaign file.
 * @param {string} key - S3 object key
 * @param {number} [expiresInSeconds=900] - default 15 minutes
 */
async function getPresignedDownloadUrl(key, expiresInSeconds = 15 * 60) {
  if (!isS3Configured()) {
    throw new Error('S3 is not configured. Set AWS_S3_BUCKET_NAME in config.env.');
  }

  const command = new GetObjectCommand({
    Bucket: BUCKET_NAME,
    Key: key,
  });

  return getSignedUrl(s3Client, command, { expiresIn: expiresInSeconds });
}

module.exports = {
  isS3Configured,
  uploadFileToS3,
  getPresignedDownloadUrl,
};

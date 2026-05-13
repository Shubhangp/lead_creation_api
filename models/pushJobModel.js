'use strict';

/**
 * pushJobModel.js
 *
 * Stores push job state in DynamoDB using the SAME process_leads table.
 * Job records live under the key prefix  "JOB#<jobId>"  so they never
 * collide with lead records and need no extra table or extra cost.
 *
 * A job record looks like:
 * {
 *   processLeadId : "JOB#abc-123",          ← partition key
 *   jobId         : "abc-123",
 *   type          : "PUSH_JOB",
 *   status        : "processing" | "completed" | "failed",
 *   source        : "FREO",
 *   startDate     : "2025-01-01T00:00:00.000Z",
 *   endDate       : "2025-01-31T23:59:59.999Z",
 *   lenders       : ["SML","ZYPE"],
 *   totalFetched  : 5000,
 *   savedToLeads  : 4800,
 *   failedToSave  : 200,
 *   lenderResults : { SML: {...}, ZYPE: {...} },
 *   lastKey       : <DynamoDB pagination token as JSON string | null>,
 *   startedAt     : "2025-01-15T10:00:00.000Z",
 *   completedAt   : null | "2025-01-15T10:05:00.000Z",
 *   ttl           : <unix epoch + 7 days>   ← DynamoDB auto-deletes old jobs
 * }
 */

const { docClient } = require('../dynamodb');
const {
    PutCommand,
    GetCommand,
    UpdateCommand,
} = require('@aws-sdk/lib-dynamodb');

const TABLE_NAME = 'process_leads';
const JOB_TTL_DAYS = 7;

function jobKey(jobId) {
    return `JOB#${jobId}`;
}

function ttlEpoch() {
    return Math.floor(Date.now() / 1000) + JOB_TTL_DAYS * 86400;
}

class PushJob {

    static async create(jobId, meta) {
        const item = {
            processLeadId: jobKey(jobId),
            jobId,
            type: 'PUSH_JOB',
            status: 'processing',
            startedAt: new Date().toISOString(),
            completedAt: null,
            totalFetched: 0,
            savedToLeads: 0,
            failedToSave: 0,
            lenderResults: {},
            lastKey: null,   // DynamoDB pagination checkpoint
            errors: [],
            ttl: ttlEpoch(),
            ...meta,
        };

        await docClient.send(new PutCommand({ TableName: TABLE_NAME, Item: item }));
        return item;
    }

    static async get(jobId) {
        const result = await docClient.send(new GetCommand({
            TableName: TABLE_NAME,
            Key: { processLeadId: jobKey(jobId) },
        }));
        return result.Item || null;
    }

    // Generic field updater — only writes the fields you pass
    static async update(jobId, fields) {
        const keys = Object.keys(fields);
        if (keys.length === 0) return;

        const exprParts = [];
        const exprNames = {};
        const exprValues = {};

        keys.forEach((k, i) => {
            exprParts.push(`#f${i} = :v${i}`);
            exprNames[`#f${i}`] = k;
            exprValues[`:v${i}`] = fields[k];
        });

        await docClient.send(new UpdateCommand({
            TableName: TABLE_NAME,
            Key: { processLeadId: jobKey(jobId) },
            UpdateExpression: `SET ${exprParts.join(', ')}`,
            ExpressionAttributeNames: exprNames,
            ExpressionAttributeValues: exprValues,
        }));
    }

    // Atomic counter increment — avoids race conditions on savedToLeads / failedToSave
    static async increment(jobId, field, delta = 1) {
        await docClient.send(new UpdateCommand({
            TableName: TABLE_NAME,
            Key: { processLeadId: jobKey(jobId) },
            UpdateExpression: 'ADD #f :d',
            ExpressionAttributeNames: { '#f': field },
            ExpressionAttributeValues: { ':d': delta },
        }));
    }

    static async markCompleted(jobId, lenderResults, failedToSave = []) {
        await this.update(jobId, {
            status: 'completed',
            completedAt: new Date().toISOString(),
            lastKey: null,
            lenderResults,
            failedToSave: failedToSave.slice(0, 100),
        });
    }

    static async markFailed(jobId, errorMessage) {
        await this.update(jobId, {
            status: 'failed',
            completedAt: new Date().toISOString(),
            errors: [errorMessage],
        });
    }

    // Save the DynamoDB pagination token so the job can resume after a restart
    static async checkpoint(jobId, lastKey, savedToLeads, failedToSave) {
        await this.update(jobId, {
            lastKey,
            savedToLeads,
            failedToSave,
        });
    }
}

module.exports = PushJob;
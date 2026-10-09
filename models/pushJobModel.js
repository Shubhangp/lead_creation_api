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
 *   status        : "processing" | "completed" | "failed" | "cancelled",
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
    ScanCommand,
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
            stoppedLenders: [],      // lenders an admin stopped mid-job
            stoppedLendersAt: {},
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

    // Like update(), but only applies while the job is still "processing".
    // Used for the terminal writes so a job an admin cancelled mid-run is
    // never flipped back to completed/failed by the loop finishing up.
    // Returns false when the job was no longer processing.
    static async updateIfProcessing(jobId, fields) {
        const keys = Object.keys(fields);
        const exprParts = [];
        const exprNames = { '#status': 'status' };
        const exprValues = { ':processing': 'processing' };

        keys.forEach((k, i) => {
            exprParts.push(`#f${i} = :v${i}`);
            exprNames[`#f${i}`] = k;
            exprValues[`:v${i}`] = fields[k];
        });

        try {
            await docClient.send(new UpdateCommand({
                TableName: TABLE_NAME,
                Key: { processLeadId: jobKey(jobId) },
                UpdateExpression: `SET ${exprParts.join(', ')}`,
                ConditionExpression: '#status = :processing',
                ExpressionAttributeNames: exprNames,
                ExpressionAttributeValues: exprValues,
            }));
            return true;
        } catch (err) {
            if (err.name === 'ConditionalCheckFailedException') return false;
            throw err;
        }
    }

    static async markCompleted(jobId, lenderResults, failedToSave = []) {
        return this.updateIfProcessing(jobId, {
            status: 'completed',
            completedAt: new Date().toISOString(),
            lastKey: null,
            lenderResults,
            failedToSave: failedToSave.slice(0, 100),
        });
    }

    static async markFailed(jobId, errorMessage) {
        return this.updateIfProcessing(jobId, {
            status: 'failed',
            completedAt: new Date().toISOString(),
            errors: [errorMessage],
        });
    }

    // Admin stop. Flips a "processing" job to "cancelled"; the background loop
    // sees it and stops before sending the next lender batch. Returns the
    // updated job, or null if the job wasn't processing (already finished).
    static async markCancelled(jobId) {
        const now = new Date().toISOString();
        const ok = await this.updateIfProcessing(jobId, {
            status: 'cancelled',
            cancelledAt: now,
            completedAt: now,
        });
        return ok ? this.get(jobId) : null;
    }

    // Admin stop for ONE lender of a running job. Appends the lender to
    // stoppedLenders (the other lenders keep receiving leads). Returns the
    // updated job, or null if the job isn't processing / lender not in job /
    // lender already stopped.
    static async stopLender(jobId, lender) {
        try {
            const result = await docClient.send(new UpdateCommand({
                TableName: TABLE_NAME,
                Key: { processLeadId: jobKey(jobId) },
                UpdateExpression: 'SET #sl = list_append(if_not_exists(#sl, :empty), :one), #sla.#ln = :now',
                ConditionExpression: '#status = :processing AND contains(#lenders, :lender) AND (attribute_not_exists(#sl) OR NOT contains(#sl, :lender))',
                ExpressionAttributeNames: {
                    '#sl': 'stoppedLenders', '#sla': 'stoppedLendersAt', '#ln': lender,
                    '#status': 'status', '#lenders': 'lenders',
                },
                ExpressionAttributeValues: {
                    ':empty': [], ':one': [lender], ':lender': lender,
                    ':processing': 'processing', ':now': new Date().toISOString(),
                },
                ReturnValues: 'ALL_NEW',
            }));
            return result.Attributes;
        } catch (err) {
            if (err.name === 'ConditionalCheckFailedException') return null;
            // stoppedLendersAt map doesn't exist yet on older jobs → create it first, retry once.
            if (err.name === 'ValidationException' && /document path/i.test(err.message)) {
                await docClient.send(new UpdateCommand({
                    TableName: TABLE_NAME,
                    Key: { processLeadId: jobKey(jobId) },
                    UpdateExpression: 'SET stoppedLendersAt = if_not_exists(stoppedLendersAt, :m)',
                    ExpressionAttributeValues: { ':m': {} },
                }));
                return this.stopLender(jobId, lender);
            }
            throw err;
        }
    }

    // Save the DynamoDB pagination token so the job can resume after a restart
    static async checkpoint(jobId, lastKey, savedToLeads, failedToSave) {
        await this.update(jobId, {
            lastKey,
            savedToLeads,
            failedToSave,
        });
    }

    // Find every job still marked "processing" — used on server boot to
    // auto-resume push jobs that were interrupted by a crash/restart.
    // process_leads holds real lead rows too, so we must filter on
    // type === 'PUSH_JOB' as well as status; paginated internally since it's
    // a full-table scan (run once at startup, so cost is acceptable).
    static async findActiveJobs() {
        const items = [];
        let exclusiveStartKey;

        do {
            const result = await docClient.send(new ScanCommand({
                TableName: TABLE_NAME,
                FilterExpression: '#type = :type AND #status = :status',
                ExpressionAttributeNames: { '#type': 'type', '#status': 'status' },
                ExpressionAttributeValues: { ':type': 'PUSH_JOB', ':status': 'processing' },
                ...(exclusiveStartKey ? { ExclusiveStartKey: exclusiveStartKey } : {}),
            }));

            items.push(...(result.Items || []));
            exclusiveStartKey = result.LastEvaluatedKey;
        } while (exclusiveStartKey);

        return items;
    }
}

module.exports = PushJob;
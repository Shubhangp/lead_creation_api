'use strict';

/**
 * pushJobModel.js
 *
 * Push jobs and upload records for admin/lead_upload live in their OWN small
 * table `push_jobs` (PK jobId). They used to live inside process_leads, which
 * meant every "list running jobs" call (the dashboard polls it every 10 s) was a
 * full Scan of the whole process_leads table — slow and expensive. Scanning
 * push_jobs only touches the job records themselves.
 *
 * A push job record:
 * {
 *   jobId, type: 'PUSH_JOB',
 *   status        : 'processing' | 'completed' | 'failed' | 'cancelled',
 *   source, startDate, endDate, lenders[],
 *   totalFetched  : rows in process_leads for the range,
 *   savedToLeads, reusedLeads, failedToSave,
 *   queuedForLenders : rows prepared for dispatch so far,
 *   saveDone      : true once every page is prepared,
 *   lastKey, nextSeq            : save-loop checkpoint,
 *   lenderCursors : { ZYPE: 1200, ... }  each lender's own progress,
 *   lenderResults : { ZYPE: { status, total, successCount, failCount, categories{}, startedAt, finishedAt } },
 *   stoppedLenders[], stoppedLendersAt{},
 *   startedAt, completedAt, cancelledAt, errors[], ttl
 * }
 *
 * An upload record (type 'UPLOAD'): jobId = uploadBatch, source, fileName,
 * total / succeeded / failed / skipped, startedAt, completedAt, status.
 */

const { docClient } = require('../dynamodb');
const {
    PutCommand,
    GetCommand,
    UpdateCommand,
    ScanCommand,
} = require('@aws-sdk/lib-dynamodb');

const TABLE_NAME = process.env.PUSH_JOBS_TABLE || 'push_jobs';
const LEGACY_TABLE = process.env.PROCESS_LEADS_TABLE || 'process_leads';
const JOB_TTL_DAYS = 180; // history kept ~6 months

function ttlEpoch() {
    return Math.floor(Date.now() / 1000) + JOB_TTL_DAYS * 86400;
}

async function scanAll(params) {
    const items = [];
    let lastKey;
    do {
        const r = await docClient.send(new ScanCommand({ ...params, ExclusiveStartKey: lastKey }));
        items.push(...(r.Items || []));
        lastKey = r.LastEvaluatedKey;
    } while (lastKey);
    return items;
}

class PushJob {

    static TABLE_NAME = TABLE_NAME;

    static async create(jobId, meta) {
        const item = {
            jobId,
            type: 'PUSH_JOB',
            status: 'processing',
            startedAt: new Date().toISOString(),
            completedAt: null,
            totalFetched: 0,
            savedToLeads: 0,
            reusedLeads: 0,
            failedToSave: 0,
            queuedForLenders: 0,
            saveDone: false,
            nextSeq: 0,
            lastKey: null,
            lenderCursors: {},
            lenderResults: {},
            stoppedLenders: [],
            stoppedLendersAt: {},
            errors: [],
            ttl: ttlEpoch(),
            ...meta,
        };
        (item.lenders || []).forEach((l) => {
            if (!item.lenderResults[l]) {
                item.lenderResults[l] = { lender: l, status: 'running', total: 0, successCount: 0, failCount: 0, categories: {} };
            }
            if (item.lenderCursors[l] === undefined) item.lenderCursors[l] = -1; // last seq sent; -1 = none yet
        });
        await docClient.send(new PutCommand({ TableName: TABLE_NAME, Item: item }));
        return item;
    }

    static async get(jobId) {
        const result = await docClient.send(new GetCommand({ TableName: TABLE_NAME, Key: { jobId } }));
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
            Key: { jobId },
            UpdateExpression: `SET ${exprParts.join(', ')}`,
            ExpressionAttributeNames: exprNames,
            ExpressionAttributeValues: exprValues,
        }));
    }

    /**
     * One lender's progress: its result object and its cursor, written as
     * nested paths so lenders running in parallel never overwrite each other.
     */
    static async updateLender(jobId, lender, result, cursor) {
        const names = { '#lr': 'lenderResults', '#lc': 'lenderCursors', '#l': lender };
        const values = { ':r': result };
        let expr = 'SET #lr.#l = :r';
        if (cursor !== undefined) {
            expr += ', #lc.#l = :c';
            values[':c'] = cursor;
        }
        try {
            await docClient.send(new UpdateCommand({
                TableName: TABLE_NAME, Key: { jobId },
                UpdateExpression: expr, ExpressionAttributeNames: names, ExpressionAttributeValues: values,
            }));
        } catch (err) {
            // Older records may lack the parent maps — create them, then retry once.
            if (err.name === 'ValidationException' && /document path/i.test(err.message)) {
                await docClient.send(new UpdateCommand({
                    TableName: TABLE_NAME, Key: { jobId },
                    UpdateExpression: 'SET lenderResults = if_not_exists(lenderResults, :m), lenderCursors = if_not_exists(lenderCursors, :m)',
                    ExpressionAttributeValues: { ':m': {} },
                }));
                return this.updateLender(jobId, lender, result, cursor);
            }
            throw err;
        }
    }

    // Like update(), but only applies while the job is still "processing", so a
    // job an admin cancelled is never flipped back to completed/failed.
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
                Key: { jobId },
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

    static async markCompleted(jobId, lenderResults) {
        return this.updateIfProcessing(jobId, {
            status: 'completed',
            completedAt: new Date().toISOString(),
            lastKey: null,
            lenderResults,
        });
    }

    static async markFailed(jobId, errorMessage) {
        return this.updateIfProcessing(jobId, {
            status: 'failed',
            completedAt: new Date().toISOString(),
            errors: [errorMessage],
        });
    }

    // Admin stop for the whole job. Returns the updated job, or null if it had
    // already finished.
    static async markCancelled(jobId) {
        const now = new Date().toISOString();
        const ok = await this.updateIfProcessing(jobId, { status: 'cancelled', cancelledAt: now, completedAt: now });
        return ok ? this.get(jobId) : null;
    }

    // Admin stop for ONE lender of a running job.
    static async stopLender(jobId, lender) {
        try {
            const result = await docClient.send(new UpdateCommand({
                TableName: TABLE_NAME,
                Key: { jobId },
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
            if (err.name === 'ValidationException' && /document path/i.test(err.message)) {
                await docClient.send(new UpdateCommand({
                    TableName: TABLE_NAME,
                    Key: { jobId },
                    UpdateExpression: 'SET stoppedLendersAt = if_not_exists(stoppedLendersAt, :m)',
                    ExpressionAttributeValues: { ':m': {} },
                }));
                return this.stopLender(jobId, lender);
            }
            throw err;
        }
    }

    // Save-loop checkpoint so the job can resume after a restart.
    static async checkpoint(jobId, fields) {
        await this.update(jobId, fields);
    }

    // Running push jobs (small table scan — only job records live here).
    static async findActiveJobs() {
        return scanAll({
            TableName: TABLE_NAME,
            FilterExpression: '#type = :type AND #status = :status',
            ExpressionAttributeNames: { '#type': 'type', '#status': 'status' },
            ExpressionAttributeValues: { ':type': 'PUSH_JOB', ':status': 'processing' },
        });
    }

    /** History: newest first. type = 'PUSH_JOB' | 'UPLOAD' | undefined (both). */
    static async list({ type, limit = 200 } = {}) {
        const params = {
            TableName: TABLE_NAME,
            // lastKey / lenderCursors are internal; leave them out of the list
            ProjectionExpression: 'jobId, #type, #status, #source, startDate, endDate, lenders, totalFetched, savedToLeads, reusedLeads, failedToSave, queuedForLenders, lenderResults, stoppedLenders, stoppedLendersAt, startedAt, completedAt, cancelledAt, errors, fileName, #total, succeeded, failed, skipped',
            ExpressionAttributeNames: { '#type': 'type', '#status': 'status', '#source': 'source', '#total': 'total' },
        };
        if (type) {
            params.FilterExpression = '#type = :type';
            params.ExpressionAttributeValues = { ':type': type };
        }
        const items = await scanAll(params);
        items.sort((a, b) => String(b.startedAt || '').localeCompare(String(a.startedAt || '')));
        return items.slice(0, limit);
    }

    /** Record one Step-2 upload for the history list. */
    static async recordUpload(uploadBatch, fields) {
        await docClient.send(new PutCommand({
            TableName: TABLE_NAME,
            Item: { jobId: uploadBatch, type: 'UPLOAD', ttl: ttlEpoch(), ...fields },
        }));
    }

    // ── migration helper (scripts/pushJobs/setup.js) ────────────────────────
    // Job records written before this table existed live in process_leads
    // under processLeadId "JOB#<id>". Full scan — run once, by the script only.
    static async findLegacyJobs() {
        return scanAll({
            TableName: LEGACY_TABLE,
            FilterExpression: '#type = :type',
            ExpressionAttributeNames: { '#type': 'type' },
            ExpressionAttributeValues: { ':type': 'PUSH_JOB' },
        });
    }
}

module.exports = PushJob;

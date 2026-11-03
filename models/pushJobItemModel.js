'use strict';

/**
 * pushJobItemModel.js — the dispatch queue of one push job.
 *
 * Table `push_job_items`  (PK jobId, SK seq: Number, TTL on `exp`)
 *
 * The push job's save loop turns process_leads rows into leads-table rows and
 * appends each dispatchable lead here with an increasing `seq`. Every lender
 * then reads this queue on its own, from its own cursor, at its own rate limit.
 * A slow lender no longer holds up the fast ones, and after a restart each
 * lender continues exactly where it stopped. Items expire after 7 days.
 */

const { docClient } = require('../dynamodb');
const { QueryCommand, BatchWriteCommand } = require('@aws-sdk/lib-dynamodb');

const TABLE_NAME = process.env.PUSH_JOB_ITEMS_TABLE || 'push_job_items';
const ITEM_TTL_DAYS = 7;
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

class PushJobItem {

    static TABLE_NAME = TABLE_NAME;

    /**
     * Append leads with consecutive seq numbers starting at `firstSeq`.
     * Re-writing the same seq (a page re-run after a crash) overwrites, never duplicates.
     * Returns the number of items that could not be written.
     */
    static async appendMany(jobId, firstSeq, leads) {
        const exp = Math.floor(Date.now() / 1000) + ITEM_TTL_DAYS * 86400;
        const items = leads.map((lead, i) => ({ jobId, seq: firstSeq + i, lead, exp }));
        const chunks = [];
        for (let i = 0; i < items.length; i += 25) chunks.push(items.slice(i, i + 25));

        let failed = 0;
        await Promise.all(chunks.map(async (chunk) => {
            let pending = chunk.map((Item) => ({ PutRequest: { Item } }));
            for (let attempt = 0; pending.length && attempt < 10; attempt++) {
                if (attempt) await sleep(Math.min(3000, 100 * 2 ** attempt));
                try {
                    const r = await docClient.send(new BatchWriteCommand({ RequestItems: { [TABLE_NAME]: pending } }));
                    pending = (r.UnprocessedItems && r.UnprocessedItems[TABLE_NAME]) || [];
                } catch (err) {
                    console.error(`[PushJobItem] batch write error (attempt ${attempt + 1}):`, err.message);
                }
            }
            failed += pending.length;
        }));
        return failed;
    }

    /** Up to `limit` items with seq > afterSeq, in order (strongly consistent). */
    static async readAfter(jobId, afterSeq, limit) {
        const r = await docClient.send(new QueryCommand({
            TableName: TABLE_NAME,
            KeyConditionExpression: 'jobId = :j AND seq > :s',
            ExpressionAttributeValues: { ':j': jobId, ':s': afterSeq },
            Limit: limit,
            ConsistentRead: true,
        }));
        return r.Items || [];
    }
}

module.exports = PushJobItem;

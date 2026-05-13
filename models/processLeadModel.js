const { docClient } = require('../dynamodb');
const {
    PutCommand,
    GetCommand,
    QueryCommand,
    UpdateCommand,
    BatchWriteCommand,
} = require('@aws-sdk/lib-dynamodb');
const { v4: uuidv4 } = require('uuid');

const TABLE_NAME = process.env.PROCESS_LEADS_TABLE || 'process_leads';
const DYNAMO_BATCH_LIMIT = 25;

// ─── Batch writer with auto-retry on unprocessed items ────────────────────────
async function batchWriteItems(items) {
    const failed = [];

    for (let i = 0; i < items.length; i += DYNAMO_BATCH_LIMIT) {
        const chunk = items.slice(i, i + DYNAMO_BATCH_LIMIT);
        let unprocessed = {
            [TABLE_NAME]: chunk.map((Item) => ({ PutRequest: { Item } })),
        };

        let attempt = 0;
        while (Object.keys(unprocessed).length > 0 && attempt < 5) {
            if (attempt > 0) await new Promise((r) => setTimeout(r, 100 * 2 ** attempt));
            try {
                const result = await docClient.send(
                    new BatchWriteCommand({ RequestItems: unprocessed })
                );
                unprocessed = result.UnprocessedItems || {};
            } catch (err) {
                console.error('[ProcessLead] BatchWrite error:', err.message);
                chunk.forEach((item) => failed.push({ item, reason: err.message }));
                break;
            }
            attempt++;
        }

        if (unprocessed[TABLE_NAME]?.length) {
            unprocessed[TABLE_NAME].forEach(({ PutRequest }) =>
                failed.push({ item: PutRequest.Item, reason: 'Unprocessed after retries' })
            );
        }
    }

    return failed;
}

// ─── In-memory dedup within one upload batch ──────────────────────────────────
function dedupeInMemory(leads) {
    const seenPhones = new Set();
    const seenPans = new Set();
    const unique = [];
    const duplicates = [];

    for (const lead of leads) {
        const phone = (lead.phone || '').trim();
        const pan = (lead.panNumber || '').trim().toUpperCase();

        if ((phone && seenPhones.has(phone)) || (pan && seenPans.has(pan))) {
            duplicates.push({ lead, reason: 'Duplicate within file (phone or PAN)' });
            continue;
        }
        if (phone) seenPhones.add(phone);
        if (pan) seenPans.add(pan);
        unique.push(lead);
    }

    return { unique, duplicates };
}

class ProcessLead {

    // ============================================================================
    // HELPERS
    // ============================================================================

    static getDatePartition(date) {
        const d = new Date(date);
        return `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, '0')}`;
    }

    static getMonthPartitions(startDate, endDate) {
        const start = new Date(startDate);
        const end = new Date(endDate);
        const parts = [];
        let cur = new Date(start.getFullYear(), start.getMonth(), 1);
        const endMonth = new Date(end.getFullYear(), end.getMonth(), 1);
        while (cur <= endMonth) {
            parts.push(this.getDatePartition(cur));
            cur.setMonth(cur.getMonth() + 1);
        }
        return parts;
    }

    static _buildItem(data, now) {
        const createdAt = data.createdAt || now;
        return {
            processLeadId: uuidv4(),
            source: data.source || 'UPLOAD',
            fullName: data.fullName || '',
            firstName: data.firstName || null,
            lastName: data.lastName || null,
            phone: (data.phone || '').trim(),
            email: (data.email || '').trim(),
            dateOfBirth: data.dateOfBirth
                ? new Date(data.dateOfBirth).toISOString()
                : null,
            gender: data.gender || null,
            panNumber: (data.panNumber || '').trim().toUpperCase(),
            jobType: data.jobType || null,
            businessType: data.businessType || null,
            salary: data.salary || null,
            creditScore: data.creditScore || null,
            cibilScore: data.cibilScore || null,
            address: data.address || null,
            pincode: data.pincode || null,
            consent: data.consent !== undefined ? data.consent : true,
            uploadBatch: data.uploadBatch || null,   // track which upload this came from
            createdAt,
            datePartition: this.getDatePartition(createdAt),
        };
    }

    // ============================================================================
    // BULK CREATE  (primary path — used by upload controller)
    // ============================================================================

    static async createBulk(leadsData) {
        if (!leadsData?.length) return { successful: [], failed: [] };

        const now = new Date().toISOString();
        const { unique, duplicates } = dedupeInMemory(leadsData);
        const items = unique.map((lead) => this._buildItem(lead, now));

        const failedItems = await batchWriteItems(items);

        const failedIds = new Set(failedItems.map((f) => f.item?.processLeadId));
        const successful = items.filter((i) => !failedIds.has(i.processLeadId));
        const failed = [
            ...duplicates,
            ...failedItems.map(({ item, reason }) => ({ lead: item, reason })),
        ];

        return { successful, failed };
    }

    // ============================================================================
    // READ — GSI queries only, never Scan
    // ============================================================================

    /** Direct PK lookup */
    static async findById(processLeadId) {
        const result = await docClient.send(
            new GetCommand({ TableName: TABLE_NAME, Key: { processLeadId } })
        );
        return result.Item || null;
    }

    /**
     * Query by source + createdAt date range  (GSI-1: source-createdAt-index)
     *
     * This is the main query used by the "push" endpoint.
     * Handles multi-month ranges by querying the GSI directly on the SK (createdAt).
     * Since source is the PK and createdAt is the SK on this GSI, DynamoDB can
     * filter the range efficiently without a Scan.
     *
     * @param {string}  source
     * @param {string}  startDate  ISO string  e.g. "2025-01-01T00:00:00.000Z"
     * @param {string}  endDate    ISO string  e.g. "2025-01-31T23:59:59.999Z"
     * @param {number}  limit      max items per page (default 1000)
     * @param {object}  lastKey    pagination token
     * @returns {{ items, lastEvaluatedKey, count }}
     */
    static async findBySourceAndDateRange(source, startDate, endDate, { limit = 1000, lastKey = null } = {}) {
        const allItems = [];
        let lastEvaluatedKey = lastKey;

        do {
            const params = {
                TableName: TABLE_NAME,
                IndexName: 'source-createdAt-index',
                KeyConditionExpression: '#src = :source AND createdAt BETWEEN :start AND :end',
                ExpressionAttributeNames: { '#src': 'source' },
                ExpressionAttributeValues: {
                    ':source': source,
                    ':start': startDate,
                    ':end': endDate,
                },
                ScanIndexForward: true,
                Limit: Math.min(limit, 1000),
            };

            if (lastEvaluatedKey) params.ExclusiveStartKey = lastEvaluatedKey;

            const result = await docClient.send(new QueryCommand(params));
            allItems.push(...(result.Items || []));
            lastEvaluatedKey = result.LastEvaluatedKey;

            // If caller passed a hard limit, stop when reached
            if (limit && allItems.length >= limit) break;
        } while (lastEvaluatedKey);

        return {
            items: allItems,
            lastEvaluatedKey: lastEvaluatedKey || null,
            count: allItems.length,
        };
    }

    /**
     * Count leads by source + date range without fetching item data.
     * Uses SELECT: COUNT — reads consume capacity units but no data transfer.
     */
    static async countBySourceAndDateRange(source, startDate, endDate) {
        let count = 0;
        let lastKey = null;

        do {
            const params = {
                TableName: TABLE_NAME,
                IndexName: 'source-createdAt-index',
                KeyConditionExpression: '#src = :source AND createdAt BETWEEN :start AND :end',
                ExpressionAttributeNames: { '#src': 'source' },
                ExpressionAttributeValues: {
                    ':source': source,
                    ':start': startDate,
                    ':end': endDate,
                },
                Select: 'COUNT',
            };
            if (lastKey) params.ExclusiveStartKey = lastKey;

            const result = await docClient.send(new QueryCommand(params));
            count += result.Count || 0;
            lastKey = result.LastEvaluatedKey;
        } while (lastKey);

        return count;
    }

    /** Dedup check by phone (GSI-2) */
    static async findByPhone(phone) {
        const result = await docClient.send(
            new QueryCommand({
                TableName: TABLE_NAME,
                IndexName: 'phone-index',
                KeyConditionExpression: 'phone = :phone',
                ExpressionAttributeValues: { ':phone': phone },
                Limit: 1,
            })
        );
        return result.Items?.[0] || null;
    }

    /** Dedup check by PAN (GSI-3) */
    static async findByPanNumber(panNumber) {
        const result = await docClient.send(
            new QueryCommand({
                TableName: TABLE_NAME,
                IndexName: 'pan-index',
                KeyConditionExpression: 'panNumber = :pan',
                ExpressionAttributeValues: { ':pan': panNumber },
                Limit: 1,
            })
        );
        return result.Items?.[0] || null;
    }

    // ============================================================================
    // ATOMIC COUNTER (one write per source per upload, same pattern as Lead model)
    // ============================================================================

    static async incrementCounterBy(source, delta) {
        if (!delta || delta <= 0) return;
        await docClient.send(
            new UpdateCommand({
                TableName: TABLE_NAME,
                Key: { processLeadId: `COUNT#${source}` },
                UpdateExpression: 'ADD #count :inc',
                ExpressionAttributeNames: { '#count': 'count' },
                ExpressionAttributeValues: { ':inc': delta },
            })
        );
    }

    static async countBySource(source) {
        const result = await docClient.send(
            new GetCommand({
                TableName: TABLE_NAME,
                Key: { processLeadId: `COUNT#${source}` },
            })
        );
        return result.Item?.count || 0;
    }
}

module.exports = ProcessLead;
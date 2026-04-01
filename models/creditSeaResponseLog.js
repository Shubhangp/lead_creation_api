const { docClient } = require('../dynamodb');
const { PutCommand, GetCommand, QueryCommand } = require('@aws-sdk/lib-dynamodb');
const { v4: uuidv4 } = require('uuid');

const TABLE_NAME = 'credit_sea_response_logs';

class CreditSeaResponseLog {

    // ─── Create log entry ──────────────────────────────────────────────────────

    static async create(logData) {
        const item = {
            logId: uuidv4(),
            leadId: logData.leadId,
            source: logData.source || null,
            requestPayload: logData.requestPayload || null,
            responseStatus: logData.responseStatus || null,
            responseBody: logData.responseBody || null,
            createdAt: new Date().toISOString()
        };

        await docClient.send(new PutCommand({ TableName: TABLE_NAME, Item: item }));
        return item;
    }

    // ─── Find by ID ────────────────────────────────────────────────────────────

    static async findById(logId) {
        const result = await docClient.send(new GetCommand({
            TableName: TABLE_NAME,
            Key: { logId }
        }));
        return result.Item || null;
    }

    // ─── Find by leadId ────────────────────────────────────────────────────────

    static async findByLeadId(leadId) {
        const result = await docClient.send(new QueryCommand({
            TableName: TABLE_NAME,
            IndexName: 'leadId-index',
            KeyConditionExpression: 'leadId = :leadId',
            ExpressionAttributeValues: { ':leadId': leadId }
        }));
        return result.Items || [];
    }

    // ─── getQuickStats ─────────────────────────────────────────────────────────

    static async getQuickStats(source = null, startDate = null, endDate = null) {
        const startTime = Date.now();

        try {
            if (!source) {
                const sources = ['CashKuber', 'FREO', 'BatterySmart', 'Ratecut', 'VFC'];

                const results = await Promise.all(
                    sources.map(async (src) => ({
                        source: src,
                        count: await this._countSource(src, startDate, endDate)
                    }))
                );

                const sourceBreakdown = {};
                let totalCount = 0;

                results.forEach(({ source: src, count }) => {
                    sourceBreakdown[src] = count;
                    totalCount += count;
                });

                return {
                    totalLogs: totalCount,
                    sourceBreakdown,
                    sources,
                    dateRange: startDate && endDate ? { start: startDate, end: endDate } : null,
                    scannedInMs: Date.now() - startTime,
                    method: 'query-count-all-sources',
                    indexUsed: 'source-createdAt-index'
                };
            }

            const count = await this._countSource(source, startDate, endDate);

            return {
                totalLogs: count,
                source,
                sourceBreakdown: { [source]: count },
                dateRange: startDate && endDate ? { start: startDate, end: endDate } : null,
                scannedInMs: Date.now() - startTime,
                method: 'query-count',
                indexUsed: 'source-createdAt-index'
            };
        } catch (error) {
            console.error('Error in getQuickStats:', error);
            throw error;
        }
    }

    // ─── Helper: Count a single source ────────────────────────────────────────

    static async _countSource(source, startDate = null, endDate = null) {
        const params = {
            TableName: TABLE_NAME,
            IndexName: 'source-createdAt-index',
            KeyConditionExpression: '#source = :source',
            ExpressionAttributeNames: { '#source': 'source' },
            ExpressionAttributeValues: { ':source': source },
            Select: 'COUNT'
        };

        if (startDate && endDate) {
            params.KeyConditionExpression += ' AND #createdAt BETWEEN :startDate AND :endDate';
            params.ExpressionAttributeNames['#createdAt'] = 'createdAt';
            params.ExpressionAttributeValues[':startDate'] = startDate;
            params.ExpressionAttributeValues[':endDate'] = endDate;
        }

        let totalCount = 0;
        let lastKey = null;

        do {
            if (lastKey) params.ExclusiveStartKey = lastKey;
            const result = await docClient.send(new QueryCommand(params));
            totalCount += result.Count || 0;
            lastKey = result.LastEvaluatedKey;
            delete params.ExclusiveStartKey;
        } while (lastKey);

        return totalCount;
    }

    // ─── Helper: Fetch items for a single source ──────────────────────────────

    static async _fetchItemsBySource(source, startDate, endDate) {
        let allItems = [];
        let lastKey = null;

        const params = {
            TableName: TABLE_NAME,
            IndexName: 'source-createdAt-index',
            KeyConditionExpression: '#source = :source',
            ExpressionAttributeNames: { '#source': 'source' },
            ExpressionAttributeValues: { ':source': source },
            ScanIndexForward: false
        };

        if (startDate && endDate) {
            params.KeyConditionExpression += ' AND #createdAt BETWEEN :startDate AND :endDate';
            params.ExpressionAttributeNames['#createdAt'] = 'createdAt';
            params.ExpressionAttributeValues[':startDate'] = startDate;
            params.ExpressionAttributeValues[':endDate'] = endDate;
        }

        do {
            if (lastKey) params.ExclusiveStartKey = lastKey;
            const result = await docClient.send(new QueryCommand(params));
            allItems = allItems.concat(result.Items || []);
            lastKey = result.LastEvaluatedKey;
            delete params.ExclusiveStartKey;
        } while (lastKey);

        console.log(`  ✅ ${source}: ${allItems.length} items`);
        return allItems;
    }

    // ─── getStats ──────────────────────────────────────────────────────────────

    static async getStats(source = null, startDate = null, endDate = null) {
        const startTime = Date.now();

        try {
            if (!source) {
                const sources = ['CashKuber', 'FREO', 'BatterySmart', 'Ratecut', 'VFC'];
                let allItems = [];

                for (const src of sources) {
                    const items = await this._fetchItemsBySource(src, startDate, endDate);
                    allItems = allItems.concat(items);
                }

                const stats = this._calculateStats(allItems, null, startDate, endDate);
                stats.processingTimeMs = Date.now() - startTime;
                stats.method = 'query-all-sources';
                stats.indexUsed = 'source-createdAt-index';
                return stats;
            }

            const allItems = await this._fetchItemsBySource(source, startDate, endDate);
            const stats = this._calculateStats(allItems, source, startDate, endDate);
            stats.processingTimeMs = Date.now() - startTime;
            stats.method = 'query';
            stats.indexUsed = 'source-createdAt-index';
            return stats;
        } catch (error) {
            console.error('Error in getStats:', error);
            throw error;
        }
    }

    // ─── _calculateStats ───────────────────────────────────────────────────────

    static _calculateStats(items, source, startDate, endDate) {
        const stats = {
            totalLogs: items.length,
            source,
            dateRange: { start: startDate, end: endDate },
            responseStatusBreakdown: {},
            sourceBreakdown: {},
            statusCategoryBreakdown: { Success: 0, Fail: 0, Duplicate: 0, null: 0, other: 0 },
            sourceWiseStats: {},
            successRateBySource: {},
            // CreditSea specific
            dedupeRejectedCount: 0,
            leadCreatedCount: 0,
            successRate: '0%'
        };

        items.forEach(item => {
            const actualStatus = this._extractStatus(item);
            const src = item.source || 'unknown';

            stats.responseStatusBreakdown[actualStatus] =
                (stats.responseStatusBreakdown[actualStatus] || 0) + 1;

            stats.sourceBreakdown[src] = (stats.sourceBreakdown[src] || 0) + 1;

            if (!stats.sourceWiseStats[src]) {
                stats.sourceWiseStats[src] = {
                    totalLogs: 0, success: 0, fail: 0,
                    duplicate: 0, error: 0, null: 0, other: 0
                };
            }

            stats.sourceWiseStats[src].totalLogs++;

            if (actualStatus === 'LEAD_CREATED') {
                stats.statusCategoryBreakdown['Success']++;
                stats.sourceWiseStats[src].success++;
                stats.leadCreatedCount++;
            } else if (actualStatus === 'DUPLICATE') {
                stats.statusCategoryBreakdown['Duplicate']++;
                stats.sourceWiseStats[src].duplicate++;
                stats.dedupeRejectedCount++;
            } else if (actualStatus === 'FAILED' || actualStatus === 'Error') {
                stats.statusCategoryBreakdown['Fail']++;
                stats.sourceWiseStats[src].fail++;
            } else if (!actualStatus || actualStatus === 'null') {
                stats.statusCategoryBreakdown['null']++;
                stats.sourceWiseStats[src].null++;
            } else {
                stats.statusCategoryBreakdown['other']++;
                stats.sourceWiseStats[src].other++;
            }
        });

        const successCount = stats.statusCategoryBreakdown['Success'];
        stats.successRate = items.length > 0
            ? ((successCount / items.length) * 100).toFixed(2) + '%'
            : '0%';

        Object.keys(stats.sourceWiseStats).forEach(src => {
            const s = stats.sourceWiseStats[src];
            s.successRate = s.totalLogs > 0
                ? ((s.success / s.totalLogs) * 100).toFixed(2) + '%'
                : '0%';
            stats.successRateBySource[src] = s.successRate;
        });

        return stats;
    }

    // ─── Helper: Extract status ────────────────────────────────────────────────

    static _extractStatus(item) {
        if (item.responseStatus) return item.responseStatus;
        if (!item.responseBody) return 'null';

        try {
            let body = typeof item.responseBody === 'string'
                ? JSON.parse(item.responseBody)
                : item.responseBody;

            // CreditSea create-lead success: { message: "Lead generated successfully", leadId: "..." }
            if (body.leadId) return 'LEAD_CREATED';

            // Dedupe hit: logged as DUPLICATE
            if (body.isPresent === true) return 'DUPLICATE';

            // Failure responses
            if (body.message) return body.message;

            return 'null';
        } catch {
            return 'null';
        }
    }

    // ─── getStatsByDate ────────────────────────────────────────────────────────

    static async getStatsByDate(sourceOrStartDate, startDateOrEndDate, endDate) {
        try {
            let source, startDate, actualEndDate;

            if (endDate) {
                source = sourceOrStartDate;
                startDate = startDateOrEndDate;
                actualEndDate = endDate;
            } else {
                source = null;
                startDate = sourceOrStartDate;
                actualEndDate = startDateOrEndDate;
            }

            if (!source) {
                const sources = ['CashKuber', 'FREO', 'BatterySmart', 'Ratecut', 'VFC'];
                let allItems = [];

                for (const src of sources) {
                    const items = await this._fetchItemsBySource(src, startDate, actualEndDate);
                    allItems = allItems.concat(items);
                }

                const statsByDate = this._groupByDate(allItems);
                return Object.values(statsByDate).sort((a, b) => a.date.localeCompare(b.date));
            }

            const allItems = await this._fetchItemsBySource(source, startDate, actualEndDate);
            const statsByDate = this._groupByDate(allItems);
            return Object.values(statsByDate).sort((a, b) => a.date.localeCompare(b.date));
        } catch (error) {
            console.error('Error in getStatsByDate:', error);
            throw error;
        }
    }

    // ─── _groupByDate ──────────────────────────────────────────────────────────

    static _groupByDate(items) {
        const statsByDate = {};

        items.forEach(item => {
            const date = item.createdAt.split('T')[0];
            const actualStatus = this._extractStatus(item);
            const src = item.source || 'unknown';

            if (!statsByDate[date]) {
                statsByDate[date] = {
                    date,
                    total: 0,
                    statusBreakdown: {},
                    statusCategories: { Success: 0, Fail: 0, Duplicate: 0, null: 0, other: 0 },
                    sourceBreakdown: {},
                    bySource: {}
                };
            }

            statsByDate[date].total++;
            statsByDate[date].statusBreakdown[actualStatus] =
                (statsByDate[date].statusBreakdown[actualStatus] || 0) + 1;

            if (actualStatus === 'LEAD_CREATED') {
                statsByDate[date].statusCategories['Success']++;
            } else if (actualStatus === 'DUPLICATE') {
                statsByDate[date].statusCategories['Duplicate']++;
            } else if (actualStatus === 'FAILED' || actualStatus === 'Error') {
                statsByDate[date].statusCategories['Fail']++;
            } else if (!actualStatus || actualStatus === 'null') {
                statsByDate[date].statusCategories['null']++;
            } else {
                statsByDate[date].statusCategories['other']++;
            }

            statsByDate[date].sourceBreakdown[src] =
                (statsByDate[date].sourceBreakdown[src] || 0) + 1;

            if (!statsByDate[date].bySource[src]) {
                statsByDate[date].bySource[src] = {
                    total: 0, success: 0, fail: 0, duplicate: 0, null: 0, other: 0
                };
            }

            statsByDate[date].bySource[src].total++;

            if (actualStatus === 'LEAD_CREATED') statsByDate[date].bySource[src].success++;
            else if (actualStatus === 'DUPLICATE') statsByDate[date].bySource[src].duplicate++;
            else if (actualStatus === 'FAILED' || actualStatus === 'Error') statsByDate[date].bySource[src].fail++;
            else if (!actualStatus || actualStatus === 'null') statsByDate[date].bySource[src].null++;
            else statsByDate[date].bySource[src].other++;
        });

        return statsByDate;
    }
}

module.exports = CreditSeaResponseLog;
'use strict';
/**
 * DynamoDB reads/writes for the `stats_daily` table.
 * Works with any DocumentClient (the app's, the Lambda's, a script's).
 */

const {
  GetCommand, PutCommand, UpdateCommand, QueryCommand, BatchGetCommand, TransactWriteCommand,
} = require('@aws-sdk/lib-dynamodb');
const { hllNew, hllMerge, toBuffer } = require('./core');

const STATS_TABLE = process.env.STATS_TABLE || 'stats_daily';
const ATTRS_PER_UPDATE = 200;           // keeps UpdateExpression well under the 4 KB limit
const IDEMPOTENCY_TTL_SEC = 3 * 24 * 3600;

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

function chunk(arr, n) {
  const out = [];
  for (let i = 0; i < arr.length; i += n) out.push(arr.slice(i, i + n));
  return out;
}

// ─── live counters (stream Lambda) ───────────────────────────────────────────

/**
 * ADD `attrs` onto the live item (tbl, day) exactly once per `token`.
 * Each chunk is a transaction: [idempotency marker (must not exist), ADD update].
 * A retried Lambda batch carries the same token, so already-applied chunks are
 * skipped instead of being counted twice.
 */
async function applyDeltas(docClient, tbl, day, attrs, token) {
  const entries = Object.entries(attrs).filter(([, v]) => v);
  if (!entries.length) return { applied: 0, skipped: 0 };
  const now = new Date().toISOString();
  const exp = Math.floor(Date.now() / 1000) + IDEMPOTENCY_TTL_SEC;
  let applied = 0, skipped = 0;

  const parts = chunk(entries, ATTRS_PER_UPDATE);
  for (let p = 0; p < parts.length; p++) {
    const names = { '#u': '_u' };
    const values = { ':u': now };
    const adds = parts[p].map(([k, v], i) => {
      names[`#a${i}`] = k;
      values[`:a${i}`] = v;
      return `#a${i} :a${i}`;
    });
    const params = {
      TransactItems: [
        {
          Put: {
            TableName: STATS_TABLE,
            Item: { tbl: `_idem#${tbl}`, sk: `${token}|${day}|${p}`, exp },
            ConditionExpression: 'attribute_not_exists(sk)',
          },
        },
        {
          Update: {
            TableName: STATS_TABLE,
            Key: { tbl, sk: day },
            UpdateExpression: `ADD ${adds.join(', ')} SET #u = :u`,
            ExpressionAttributeNames: names,
            ExpressionAttributeValues: values,
          },
        },
      ],
    };

    for (let attempt = 0; ; attempt++) {
      try {
        await docClient.send(new TransactWriteCommand(params));
        applied++;
        break;
      } catch (err) {
        const reasons = (err && err.CancellationReasons) || [];
        if (err.name === 'TransactionCanceledException' && reasons[0] && reasons[0].Code === 'ConditionalCheckFailed') {
          skipped++; // this chunk was applied by an earlier attempt of the same batch
          break;
        }
        const retryable = err.name === 'TransactionCanceledException' || err.name === 'TransactionConflictException'
          || err.name === 'ProvisionedThroughputExceededException' || err.name === 'ThrottlingException'
          || err.name === 'TransactionInProgressException';
        if (!retryable || attempt >= 8) throw err;
        await sleep(Math.min(2000, 50 * 2 ** attempt) + Math.random() * 50);
      }
    }
  }
  return { applied, skipped };
}

// ─── unique counts (HyperLogLog registers) ───────────────────────────────────

/** Merge registers into (tbl, `${day}#u#${name}`). Max-merge, so safe to repeat. */
async function mergeHll(docClient, tbl, day, name, reg) {
  const key = { tbl, sk: `${day}#u#${name}` };
  for (let attempt = 0; attempt < 10; attempt++) {
    const cur = await docClient.send(new GetCommand({ TableName: STATS_TABLE, Key: key, ConsistentRead: true }));
    const existing = cur.Item ? toBuffer(cur.Item.R) : null;
    const merged = existing ? Buffer.from(existing) : hllNew();
    const changed = hllMerge(merged, reg);
    if (cur.Item && !changed) return false;
    const v = cur.Item ? (cur.Item.v || 0) : 0;
    const put = {
      TableName: STATS_TABLE,
      Item: { ...key, R: merged, v: v + 1, _u: new Date().toISOString() },
      ConditionExpression: 'attribute_not_exists(sk)',
    };
    if (cur.Item) {
      put.ConditionExpression = 'v = :v';
      put.ExpressionAttributeValues = { ':v': v };
    }
    try {
      await docClient.send(new PutCommand(put));
      return true;
    } catch (err) {
      if (err.name !== 'ConditionalCheckFailedException') throw err;
      await sleep(20 + Math.random() * 80); // someone else wrote — re-read and merge again
    }
  }
  throw new Error(`mergeHll: too much contention on ${tbl}/${key.sk}`);
}

// ─── backfill / rebuild ──────────────────────────────────────────────────────

/** Absolute write of the backfill counters for one day (idempotent). */
async function putBackfill(docClient, tbl, day, counters, info = {}) {
  await docClient.send(new PutCommand({
    TableName: STATS_TABLE,
    Item: { ...counters, tbl, sk: `${day}#bf`, _bfAt: new Date().toISOString(), ...info },
  }));
}

async function getMeta(docClient, tbl) {
  const r = await docClient.send(new GetCommand({ TableName: STATS_TABLE, Key: { tbl: '_meta', sk: tbl } }));
  return r.Item || null;
}

async function putMeta(docClient, tbl, fields) {
  const names = {}, values = {}, sets = [];
  Object.entries(fields).forEach(([k, v], i) => { names[`#f${i}`] = k; values[`:f${i}`] = v; sets.push(`#f${i} = :f${i}`); });
  await docClient.send(new UpdateCommand({
    TableName: STATS_TABLE, Key: { tbl: '_meta', sk: tbl },
    UpdateExpression: `SET ${sets.join(', ')}`, ExpressionAttributeNames: names, ExpressionAttributeValues: values,
  }));
}

// ─── reads ───────────────────────────────────────────────────────────────────

/** Every stored item for one table between two days (inclusive). */
async function queryRange(docClient, tbl, startDay, endDay) {
  const items = [];
  let lastKey;
  do {
    const r = await docClient.send(new QueryCommand({
      TableName: STATS_TABLE,
      KeyConditionExpression: 'tbl = :t AND sk BETWEEN :a AND :b',
      ExpressionAttributeValues: { ':t': tbl, ':a': startDay, ':b': `${endDay}~` },
      ExclusiveStartKey: lastKey,
    }));
    items.push(...(r.Items || []));
    lastKey = r.LastEvaluatedKey;
  } while (lastKey);
  return items;
}

/** Live + backfill items for many tables on one day (one BatchGet round). */
async function getDayForTables(docClient, tbls, day) {
  const keys = [];
  for (const t of tbls) keys.push({ tbl: t, sk: day }, { tbl: t, sk: `${day}#bf` });
  const out = [];
  for (const part of chunk(keys, 100)) {
    let req = { [STATS_TABLE]: { Keys: part } };
    for (let attempt = 0; req && Object.keys(req).length; attempt++) {
      const r = await docClient.send(new BatchGetCommand({ RequestItems: req }));
      out.push(...((r.Responses && r.Responses[STATS_TABLE]) || []));
      req = r.UnprocessedKeys && Object.keys(r.UnprocessedKeys).length ? r.UnprocessedKeys : null;
      if (req) await sleep(Math.min(1000, 50 * 2 ** attempt));
    }
  }
  return out;
}

module.exports = {
  STATS_TABLE, applyDeltas, mergeHll, putBackfill, getMeta, putMeta, queryRange, getDayForTables,
};

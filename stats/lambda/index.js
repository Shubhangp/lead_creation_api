'use strict';
/**
 * Stream Lambda: DynamoDB Streams (NEW_AND_OLD_IMAGES) of every tracked table
 * → incremental counters in `stats_daily`.
 *
 * For each changed row it computes "what the row contributed before" and
 * "what it contributes now" and ADDs the difference, so inserts, status
 * updates (CSV uploads, RCS retries, lender status uploads…) all keep the
 * daily numbers exact without anyone touching app code. Deletes are ignored
 * on purpose: purging old logs must not erase history from the stats.
 */

const crypto = require('crypto');
const { DynamoDBClient } = require('@aws-sdk/client-dynamodb');
const { DynamoDBDocumentClient } = require('@aws-sdk/lib-dynamodb');
const { BY_TABLE } = require('../specs');
const core = require('../core');
const store = require('../store');

let docClient = DynamoDBDocumentClient.from(new DynamoDBClient({}), {
  marshallOptions: { removeUndefinedValues: true, convertClassInstanceToMap: true },
});

// An INSERT whose createdAt is before the table's cutover and that was written
// within this window of its createdAt belongs to the backfill, not the stream.
const CUTOVER_GRACE_MS = 120 * 1000;
const META_TTL_MS = 5 * 60 * 1000;
const metaCache = new Map();

async function cutoverFor(specKey) {
  const hit = metaCache.get(specKey);
  if (hit && Date.now() - hit.at < META_TTL_MS) return hit.cutoverAt;
  let cutoverAt = null;
  try {
    const meta = await store.getMeta(docClient, specKey);
    cutoverAt = (meta && meta.cutoverAt) || null;
  } catch (e) {
    console.warn(`[stats] meta read failed for ${specKey}: ${e.message}`);
  }
  metaCache.set(specKey, { at: Date.now(), cutoverAt });
  return cutoverAt;
}

function tableFromArn(arn) {
  const m = /:table\/([^/]+)\/stream\//.exec(arn || '');
  return m ? m[1] : null;
}

async function mapLimit(items, limit, fn) {
  const out = [];
  let i = 0;
  const workers = Array.from({ length: Math.min(limit, items.length) }, async () => {
    while (i < items.length) { const idx = i++; out[idx] = await fn(items[idx]); }
  });
  await Promise.all(workers);
  return out;
}

exports.handler = async (event) => {
  const records = (event && event.Records) || [];
  if (!records.length) return { processed: 0 };

  // Same records on a retry → same token → already-applied writes are skipped.
  const token = crypto.createHash('sha1')
    .update(`${records[0].eventID}|${records[records.length - 1].eventID}|${records.length}`)
    .digest('hex').slice(0, 24);

  const deltas = new Map(); // `${specKey}\u0000${day}` → { attr: delta }
  const hll = new Map();    // `${specKey}\u0000${day}\u0000${name}` → registers
  let counted = 0, skipped = 0, errors = 0;

  for (const rec of records) {
    try {
      if (rec.eventName === 'REMOVE') { skipped++; continue; }
      const spec = BY_TABLE[tableFromArn(rec.eventSourceARN)];
      if (!spec) { skipped++; continue; }

      const newItem = core.unmarshall(rec.dynamodb && rec.dynamodb.NewImage);
      const oldItem = rec.eventName === 'MODIFY' ? core.unmarshall(rec.dynamodb && rec.dynamodb.OldImage) : null;
      if (!newItem) { skipped++; continue; }

      if (rec.eventName === 'INSERT') {
        const cutoverAt = await cutoverFor(spec.key);
        const created = Date.parse(newItem.createdAt);
        const writtenAt = (rec.dynamodb.ApproximateCreationDateTime || 0) * 1000;
        if (cutoverAt && newItem.createdAt < cutoverAt && Number.isFinite(created) && writtenAt - created < CUTOVER_GRACE_MS) {
          skipped++; // the backfill counts this row
          continue;
        }
      }

      const diff = core.diffContribution(spec, oldItem, newItem);
      for (const [day, attrs] of diff) {
        const k = `${spec.key}\u0000${day}`;
        deltas.set(k, core.addInto(deltas.get(k) || {}, attrs));
      }

      const c = core.contribution(spec, newItem);
      if (c) {
        for (const [name, value] of Object.entries(c.uniques)) {
          const k = `${spec.key}\u0000${c.day}\u0000${name}`;
          let reg = hll.get(k);
          if (!reg) { reg = core.hllNew(); hll.set(k, reg); }
          core.hllAdd(reg, value);
        }
      }
      counted++;
    } catch (e) {
      errors++;
      console.error(`[stats] record ${rec.eventID} failed: ${e.stack || e.message}`);
    }
  }

  await mapLimit([...deltas.entries()], 8, async ([k, attrs]) => {
    const [specKey, day] = k.split('\u0000');
    for (const a of Object.keys(attrs)) if (Math.abs(attrs[a]) < 1e-9) delete attrs[a];
    if (Object.keys(attrs).length) await store.applyDeltas(docClient, specKey, day, attrs, token);
  });

  await mapLimit([...hll.entries()], 8, async ([k, reg]) => {
    const [specKey, day, name] = k.split('\u0000');
    await store.mergeHll(docClient, specKey, day, name, reg);
  });

  console.log(JSON.stringify({ msg: 'stats batch', records: records.length, counted, skipped, errors, keys: deltas.size, token }));
  return { processed: records.length, counted, skipped, errors };
};

// exported for tests
exports._internal = { tableFromArn, setClient: (c) => { docClient = c; metaCache.clear(); } };

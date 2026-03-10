/**
 * deleteTable.js
 *
 * Deletes all records from the rcs_queue DynamoDB table where
 * createdAt is on or before the specified cutoff date.
 *
 * Usage:
 *   node deleteTable.js                          # defaults to 2026-03-03 23:59:59.999
 *   node deleteTable.js "2026-03-03T23:59:59.999Z"  # custom cutoff (ISO string)
 *   node deleteTable.js --dry-run                # preview without deleting
 */

const { ScanCommand, BatchWriteCommand } = require('@aws-sdk/lib-dynamodb');
const { docClient } = require('./dynamodb'); // reuses your existing credentials

// ─── Config ───────────────────────────────────────────────────────────────────

const TABLE_NAME = 'rcs_queue';
const BATCH_SIZE = 25;          // DynamoDB max per BatchWrite
const CONCURRENCY = 5;          // parallel batch deletes in flight at once
const PARALLEL_SEGMENTS = 4;    // parallel scan segments (speeds up large tables)

// ─── Args ─────────────────────────────────────────────────────────────────────

const args = process.argv.slice(2);
const isDryRun = args.includes('--dry-run');
const cutoffArg = args.find(a => !a.startsWith('--'));

// Default: 03 March 2026 23:59:59.999 UTC
const CUTOFF_ISO = cutoffArg || '2026-03-03T23:59:59.999Z';
const cutoffDate = new Date(CUTOFF_ISO);

if (isNaN(cutoffDate.getTime())) {
  console.error(`❌  Invalid date: "${CUTOFF_ISO}"`);
  console.error(`    Expected ISO format, e.g. "2026-03-03T23:59:59.999Z"`);
  process.exit(1);
}

// ─── Helpers ──────────────────────────────────────────────────────────────────

/**
 * Scan one segment of the table for items where createdAt <= cutoff.
 */
async function scanSegment(cutoffISO, segment, totalSegments) {
  const matched = [];
  let lastKey;

  do {
    const params = {
      TableName: TABLE_NAME,
      ProjectionExpression: 'rcs_queue',        // only the key — cheapest read
      FilterExpression: 'createdAt <= :cutoff',
      ExpressionAttributeValues: { ':cutoff': cutoffISO },
      Segment: segment,
      TotalSegments: totalSegments
    };
    if (lastKey) params.ExclusiveStartKey = lastKey;

    const res = await docClient.send(new ScanCommand(params));
    matched.push(...(res.Items || []));
    lastKey = res.LastEvaluatedKey;
  } while (lastKey);

  return matched;
}

/**
 * Parallel segmented scan — runs PARALLEL_SEGMENTS scans simultaneously.
 */
async function scanItemsBefore(cutoffISO) {
  const segmentPromises = Array.from({ length: PARALLEL_SEGMENTS }, (_, i) =>
    scanSegment(cutoffISO, i, PARALLEL_SEGMENTS)
  );

  process.stdout.write(`  Running ${PARALLEL_SEGMENTS} parallel scan segments …`);
  const results = await Promise.all(segmentPromises);
  const matched = results.flat();
  console.log(` found ${matched.length} matching records.`);
  return matched;
}

/**
 * Batch delete 25 items per request, with CONCURRENCY requests in parallel.
 */
async function deleteItems(items) {
  let deleted = 0;
  let failed = 0;

  // Split into chunks of 25
  const chunks = [];
  for (let i = 0; i < items.length; i += BATCH_SIZE) {
    chunks.push(items.slice(i, i + BATCH_SIZE));
  }

  // Process chunks with limited concurrency
  for (let i = 0; i < chunks.length; i += CONCURRENCY) {
    const window = chunks.slice(i, i + CONCURRENCY);

    await Promise.all(window.map(async (chunk) => {
      const requests = chunk.map(item => ({
        DeleteRequest: { Key: { rcs_queue: item.rcs_queue } }
      }));

      try {
        const res = await docClient.send(new BatchWriteCommand({
          RequestItems: { [TABLE_NAME]: requests }
        }));

        // Handle unprocessed items (DynamoDB may return some on throttle)
        const unprocessed = res.UnprocessedItems?.[TABLE_NAME]?.length || 0;
        deleted += chunk.length - unprocessed;
        failed += unprocessed;
      } catch (err) {
        console.error(`\n  ⚠️  Batch failed: ${err.message}`);
        failed += chunk.length;
      }
    }));

    process.stdout.write(`\r  Deleted ~${Math.min((i + CONCURRENCY) * BATCH_SIZE, items.length)} / ${items.length} …`);
  }

  console.log();
  return { deleted, failed };
}

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

// ─── Main ─────────────────────────────────────────────────────────────────────

async function main() {
  console.log('════════════════════════════════════════════════════');
  console.log('  rcs_queue  –  Delete records by createdAt cutoff');
  console.log('════════════════════════════════════════════════════');
  console.log(`  Table    : ${TABLE_NAME}`);
  console.log(`  Cutoff   : ${CUTOFF_ISO}  (records WITH this timestamp included)`);
  console.log(`  Mode     : ${isDryRun ? '🔍 DRY RUN (no deletes)' : '🗑️  LIVE DELETE'}`);
  console.log('────────────────────────────────────────────────────');

  // Confirm on live run
  if (!isDryRun) {
    console.log('\n  ⚠️  This will PERMANENTLY delete records. You have 5 seconds to cancel (Ctrl+C).\n');
    await sleep(5000);
  }

  console.log('\n  Scanning table …');
  const items = await scanItemsBefore(CUTOFF_ISO);

  if (items.length === 0) {
    console.log('\n  ✅  No records found on or before the cutoff. Nothing to delete.');
    return;
  }

  // Show a sample of what will be deleted
  console.log(`\n  Found ${items.length} record(s) to delete.`);
  console.log('\n  Sample (first 5):');
  items.slice(0, 5).forEach(i =>
    console.log(`    • ${i.rcs_queue}  createdAt: ${i.createdAt}`)
  );
  if (items.length > 5) console.log(`    … and ${items.length - 5} more`);

  if (isDryRun) {
    console.log('\n  🔍 Dry run complete. Rerun without --dry-run to delete.');
    return;
  }

  console.log('\n  Deleting …');
  const { deleted, failed } = await deleteItems(items);

  console.log('\n════════════════════════════════════════════════════');
  console.log(`  ✅  Done.  Deleted: ${deleted}   Failed: ${failed}`);
  console.log('════════════════════════════════════════════════════\n');
}

main().catch(err => {
  console.error('\n❌  Fatal error:', err);
  process.exit(1);
});
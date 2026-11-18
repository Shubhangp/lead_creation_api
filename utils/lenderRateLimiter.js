'use strict';

// Shared per-lender rate limiter with TWO separate budgets per lender:
//
//   live  — the main flow: leads arriving on the API (leadController)
//   bulk  — admin/lead_distribution + admin/lead_upload pushes, all together
//
// The lender's limit is split between them (default 60% live / 40% bulk, set
// on the admin dashboard — see config/lenderRateLimits.js). A big bulk push can
// therefore never eat the live flow's share, and the two together never go
// over the lender's limit.
//
// Every send takes its own slot right before it goes out, and each bucket only
// holds about one second's worth of slots, so sends are spread evenly across
// the minute. (Bulk jobs used to collect 10–50 slots and then fire that many
// requests at the same instant — those bursts are what lenders answered with
// "Too many requests. Please slow down".)

const { getPoolRate, onRateLimitsChanged } = require('../config/lenderRateLimits');

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const POOLS = ['live', 'bulk'];

// Optional override: RATE_LIMIT_BURST=<n> (slots that may go out back-to-back)
function burstFor(ratePerMinute) {
  const fromEnv = parseInt(process.env.RATE_LIMIT_BURST, 10);
  if (!Number.isNaN(fromEnv) && fromEnv > 0) return fromEnv;
  return Math.max(1, Math.ceil(ratePerMinute / 60));
}

class TokenBucket {
  constructor(lender, pool, ratePerMinute) {
    this.lender = lender;
    this.pool = pool;
    this.waiting = 0;          // callers currently queued for a slot
    this.lastWarnAt = 0;
    this.sentThisMinute = 0;
    this.minuteStart = Date.now();
    this.lastMinuteSent = 0;
    this.queue = Promise.resolve(); // hands slots out in arrival order
    this.setRate(ratePerMinute);
    this.tokens = this.capacity;
    this.last = Date.now();
  }

  setRate(ratePerMinute) {
    this._refill && this.last && this._refill();
    this.ratePerMinute = Math.max(1, ratePerMinute);
    this.capacity = burstFor(this.ratePerMinute);
    this.refillPerMs = this.ratePerMinute / 60000;
    if (this.tokens > this.capacity) this.tokens = this.capacity;
  }

  _refill() {
    const now = Date.now();
    this.tokens = Math.min(this.capacity, this.tokens + (now - this.last) * this.refillPerMs);
    this.last = now;
  }

  _count() {
    const now = Date.now();
    if (now - this.minuteStart >= 60000) {
      this.lastMinuteSent = this.sentThisMinute;
      this.sentThisMinute = 0;
      this.minuteStart = now;
    }
    this.sentThisMinute++;
  }

  async _takeOne(shouldAbort) {
    for (;;) {
      if (shouldAbort && shouldAbort()) return false;
      this._refill();
      if (this.tokens >= 1) {
        this.tokens -= 1;
        this._count();
        return true;
      }
      const waitMs = Math.ceil((1 - this.tokens) / this.refillPerMs);
      await sleep(Math.min(Math.max(waitMs, 20), 1000));
    }
  }

  _warnIfBacklogged() {
    // More than one minute of work queued → leads are going out late.
    const now = Date.now();
    if (this.waiting > this.ratePerMinute && now - this.lastWarnAt > 60000) {
      this.lastWarnAt = now;
      const minutes = (this.waiting / this.ratePerMinute).toFixed(1);
      console.warn(`[RateLimiter] ${this.lender} (${this.pool}): ${this.waiting} sends queued (~${minutes} min behind at ${this.ratePerMinute}/min)`);
    }
  }

  // Resolves true when a slot is granted, false if shouldAbort() became true.
  acquire(shouldAbort = null) {
    this.waiting++;
    this._warnIfBacklogged();
    const turn = this.queue.then(() => this._takeOne(shouldAbort));
    this.queue = turn.catch(() => {});
    return turn.finally(() => { this.waiting--; });
  }
}

const buckets = new Map(); // `${lender}|${pool}` -> TokenBucket

function getBucket(lender, pool = 'live') {
  const p = POOLS.includes(pool) ? pool : 'live';
  const key = `${lender}|${p}`;
  let bucket = buckets.get(key);
  if (!bucket) {
    bucket = new TokenBucket(lender, p, getPoolRate(lender, p));
    buckets.set(key, bucket);
    console.log(`[RateLimiter] ${lender} ${p}: ${bucket.ratePerMinute} leads/min, burst ${bucket.capacity}`);
  }
  return bucket;
}

// Dashboard edits apply to running buckets straight away (no restart needed).
onRateLimitsChanged(() => {
  for (const b of buckets.values()) {
    const rpm = getPoolRate(b.lender, b.pool);
    if (rpm !== b.ratePerMinute) {
      b.setRate(rpm);
      console.log(`[RateLimiter] ${b.lender} ${b.pool}: now ${rpm} leads/min`);
    }
  }
});

/**
 * Wait for permission to send ONE lead to `lender`.
 * pool 'live' = main API flow (default), 'bulk' = distribution / lead upload.
 * Resolves false if shouldAbort() becomes true while waiting.
 */
function acquireLenderSlot(lender, shouldAbort = null, pool = 'live') {
  return getBucket(lender, pool).acquire(shouldAbort);
}

/**
 * Wait for `n` slots (kept for older callers). Prefer one acquireLenderSlot()
 * right before each send so requests don't go out in a burst.
 */
async function acquireLenderSlots(lender, n, shouldAbort = null, pool = 'bulk') {
  const bucket = getBucket(lender, pool);
  for (let i = 0; i < n; i++) {
    const ok = await bucket.acquire(shouldAbort);
    if (!ok) return false;
  }
  return true;
}

function getLimiterStats() {
  const out = {};
  for (const b of buckets.values()) {
    out[b.lender] = out[b.lender] || {};
    out[b.lender][b.pool] = {
      ratePerMinute: b.ratePerMinute,
      burst: b.capacity,
      queued: b.waiting,
      sentLastMinute: Date.now() - b.minuteStart >= 60000 ? b.sentThisMinute : b.lastMinuteSent,
      sentThisMinute: b.sentThisMinute,
    };
  }
  return out;
}

module.exports = { acquireLenderSlot, acquireLenderSlots, getLimiterStats };

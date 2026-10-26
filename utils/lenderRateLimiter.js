'use strict';

// Shared per-lender rate limiter.
//
// ONE token bucket per lender for the whole process, used by the live lead API
// (leadController) AND the dashboard bulk jobs (leadDistributionController,
// processLeadController). Before, each of those kept its own bucket, so their
// combined traffic could exceed the lender's limit.
//
// The bucket's burst size is small (about one second's worth of tokens), so
// sends are spread evenly across the minute instead of the whole per-minute
// allowance going out in a single burst after a quiet period - that burst is
// what made OVLY answer "Rate Limit Exceeded".

const { getRateLimit } = require('../config/lenderRateLimits');

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

// Optional override: RATE_LIMIT_BURST=<n> (tokens that may go out back-to-back)
function burstFor(ratePerMinute) {
  const fromEnv = parseInt(process.env.RATE_LIMIT_BURST, 10);
  if (!Number.isNaN(fromEnv) && fromEnv > 0) return fromEnv;
  return Math.max(1, Math.ceil(ratePerMinute / 60));
}

class TokenBucket {
  constructor(lender, ratePerMinute) {
    this.lender = lender;
    this.ratePerMinute = Math.max(1, ratePerMinute);
    this.capacity = burstFor(this.ratePerMinute);
    this.tokens = this.capacity;
    this.refillPerMs = this.ratePerMinute / 60000;
    this.last = Date.now();
    this.waiting = 0;          // callers currently queued for a token
    this.lastWarnAt = 0;
    // Serialises waiters so tokens are handed out in arrival order.
    this.queue = Promise.resolve();
  }

  _refill() {
    const now = Date.now();
    this.tokens = Math.min(this.capacity, this.tokens + (now - this.last) * this.refillPerMs);
    this.last = now;
  }

  async _takeOne(shouldAbort) {
    for (;;) {
      if (shouldAbort && shouldAbort()) return false;
      this._refill();
      if (this.tokens >= 1) {
        this.tokens -= 1;
        return true;
      }
      const waitMs = Math.ceil((1 - this.tokens) / this.refillPerMs);
      await sleep(Math.min(Math.max(waitMs, 20), 1000));
    }
  }

  _warnIfBacklogged() {
    // More than one minute of work queued for this lender -> leads are going
    // out late. Warn at most once a minute so the log stays readable.
    const now = Date.now();
    if (this.waiting > this.ratePerMinute && now - this.lastWarnAt > 60000) {
      this.lastWarnAt = now;
      const minutes = (this.waiting / this.ratePerMinute).toFixed(1);
      console.warn(`[RateLimiter] ${this.lender}: ${this.waiting} sends queued (~${minutes} min behind at ${this.ratePerMinute}/min)`);
    }
  }

  // Resolves true when a token is granted, false if shouldAbort() became true.
  acquire(shouldAbort = null) {
    this.waiting++;
    this._warnIfBacklogged();
    const turn = this.queue.then(() => this._takeOne(shouldAbort));
    this.queue = turn.catch(() => {});
    return turn.finally(() => { this.waiting--; });
  }
}

const buckets = new Map();

function getBucket(lender) {
  let bucket = buckets.get(lender);
  if (!bucket) {
    const rpm = getRateLimit(lender);
    bucket = new TokenBucket(lender, rpm);
    buckets.set(lender, bucket);
    console.log(`[RateLimiter] ${lender}: ${rpm} leads/min, burst ${bucket.capacity}`);
  }
  return bucket;
}

// Wait for permission to send one lead to `lender`.
function acquireLenderSlot(lender, shouldAbort = null) {
  return getBucket(lender).acquire(shouldAbort);
}

// Wait for permission to send `n` leads to `lender` (used by bulk jobs before
// firing a sub-batch). Tokens are taken one at a time through the same queue,
// so live API leads interleave with the batch instead of waiting behind it.
// Resolves false if shouldAbort() becomes true.
async function acquireLenderSlots(lender, n, shouldAbort = null) {
  const bucket = getBucket(lender);
  for (let i = 0; i < n; i++) {
    const ok = await bucket.acquire(shouldAbort);
    if (!ok) return false;
  }
  return true;
}

function getLimiterStats() {
  const out = {};
  for (const [lender, b] of buckets) {
    out[lender] = { ratePerMinute: b.ratePerMinute, burst: b.capacity, queued: b.waiting };
  }
  return out;
}

module.exports = { acquireLenderSlot, acquireLenderSlots, getLimiterStats };

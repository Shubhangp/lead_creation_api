'use strict';

const { getRateLimit } = require('../config/lenderRateLimits');

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

class TokenBucket {
  constructor(ratePerMinute) {
    this.capacity = Math.max(1, ratePerMinute);
    this.tokens = this.capacity;
    this.refillPerMs = ratePerMinute / 60000;
    this.last = Date.now();
    // Serialises waiters so tokens are handed out in arrival order.
    this.queue = Promise.resolve();
  }

  _refill() {
    const now = Date.now();
    this.tokens = Math.min(this.capacity, this.tokens + (now - this.last) * this.refillPerMs);
    this.last = now;
  }

  async _takeOne() {
    for (;;) {
      this._refill();
      if (this.tokens >= 1) {
        this.tokens -= 1;
        return;
      }
      const waitMs = Math.ceil((1 - this.tokens) / this.refillPerMs);
      await sleep(Math.min(Math.max(waitMs, 50), 1000));
    }
  }

  acquire() {
    const turn = this.queue.then(() => this._takeOne());
    this.queue = turn.catch(() => {});
    return turn;
  }
}

const buckets = new Map();

function getBucket(lender) {
  let bucket = buckets.get(lender);
  if (!bucket) {
    const rpm = getRateLimit(lender);
    bucket = new TokenBucket(rpm);
    buckets.set(lender, bucket);
    console.log(`[RateLimiter] ${lender}: ${rpm} leads/min`);
  }
  return bucket;
}

// Wait for permission to send one lead to `lender`.
function acquireLenderSlot(lender) {
  return getBucket(lender).acquire();
}

module.exports = { acquireLenderSlot };

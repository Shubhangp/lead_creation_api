// utils/piiCrypto.js
// ─────────────────────────────────────────────────────────────────────────────
// Deterministic, reversible encryption for PII fields (phone, PAN) stored at
// rest in DynamoDB.
//
// WHY DETERMINISTIC?
//   phone and panNumber are DynamoDB GSI partition keys (phone-index,
//   panNumber-index) used for exact-match duplicate detection and status-upload
//   matching. A random-IV cipher would produce a different ciphertext every time
//   the same phone is encrypted, so `phone = :value` GSI lookups and dedup would
//   break. Deterministic encryption maps a given plaintext to a STABLE
//   ciphertext, so:
//     • storing enc(phone) in the GSI still allows enc(lookupPhone) equality
//       matches, and
//     • dedup (same phone -> same ciphertext) keeps working.
//   The only thing deterministic encryption leaks is equality (which two rows
//   share a phone) — exactly the property we depend on. Values remain fully
//   reversible for outbound lender payloads and exports.
//
// SCHEME
//   iv  = HMAC_SHA256(ivKey, plaintext)[0:16]      (synthetic, SIV-style IV)
//   ct  = AES-256-CBC(encKey, iv, plaintext)
//   out = "enc:1:" + base64(iv || ct)
//   Same plaintext -> same iv -> same out. Fully deterministic and GSI-safe.
//   The 16-byte IV is prepended to the ciphertext so decryption can recover it.
//
// PLAINTEXT-TOLERANT DECRYPT
//   decryptPII() returns any value that is NOT prefixed with "enc:1:" unchanged.
//   This lets encrypted and not-yet-migrated (plaintext) rows coexist during the
//   backfill rollout without breaking reads.
// ─────────────────────────────────────────────────────────────────────────────

const crypto = require('crypto');

const PREFIX = 'enc:1:';
const ALGO = 'aes-256-cbc';

// Master key comes from the environment. A hard fallback is provided ONLY so
// the module never throws on import in dev; production MUST set
// PII_ENCRYPTION_KEY (any length string — it is stretched via SHA-256 below).
const MASTER = process.env.PII_ENCRYPTION_KEY
  || process.env.ENCRYPTION_KEY
  || 'ratecut-dev-pii-key-change-me-please-32b';

if (!process.env.PII_ENCRYPTION_KEY && !process.env.ENCRYPTION_KEY) {
  console.warn('[piiCrypto] PII_ENCRYPTION_KEY not set — using an insecure dev fallback key. Set it in config.env for production.');
}

// Derive independent 32-byte subkeys for the cipher and the IV-HMAC so the same
// master secret is never reused directly for two purposes.
const ENC_KEY = crypto.createHash('sha256').update(`${MASTER}|enc`).digest();     // 32 bytes
const IV_KEY  = crypto.createHash('sha256').update(`${MASTER}|iv`).digest();      // 32 bytes

/** True if a value is one of our encrypted envelopes. */
function isEncrypted(value) {
  return typeof value === 'string' && value.startsWith(PREFIX);
}

/**
 * Deterministically encrypt a scalar PII value.
 * - null/undefined pass through unchanged.
 * - empty string passes through unchanged (nothing to protect).
 * - already-encrypted values pass through unchanged (idempotent).
 * - everything else is coerced to String and encrypted.
 */
function encryptPII(value) {
  if (value === null || value === undefined) return value;
  if (isEncrypted(value)) return value;
  const plain = String(value);
  if (plain === '') return value;

  const iv = crypto.createHmac('sha256', IV_KEY).update(plain, 'utf8').digest().subarray(0, 16);
  const cipher = crypto.createCipheriv(ALGO, ENC_KEY, iv);
  const ct = Buffer.concat([cipher.update(plain, 'utf8'), cipher.final()]);
  return PREFIX + Buffer.concat([iv, ct]).toString('base64');
}

/**
 * Reverse encryptPII. Plaintext-tolerant: any value NOT carrying the encrypted
 * prefix is returned unchanged, so pre-migration rows keep working. If a value
 * looks encrypted but fails to decrypt, the original is returned (never throws).
 */
function decryptPII(value) {
  if (!isEncrypted(value)) return value;
  try {
    const raw = Buffer.from(value.slice(PREFIX.length), 'base64');
    const iv = raw.subarray(0, 16);
    const ct = raw.subarray(16);
    const decipher = crypto.createDecipheriv(ALGO, ENC_KEY, iv);
    const plain = Buffer.concat([decipher.update(ct), decipher.final()]);
    return plain.toString('utf8');
  } catch (err) {
    console.error('[piiCrypto] decrypt failed, returning original:', err.message);
    return value;
  }
}

// ── Masking (for internal portal display) ───────────────────────────────────
// Values are decrypted first, then masked. Keeps enough to be recognizable
// without exposing the full PII in the UI.

/** e.g. "9876543210" -> "98••••3210"; short/blank inputs handled gracefully. */
function maskPhone(value) {
  const p = String(decryptPII(value) ?? '');
  if (!p) return p;
  if (p.length <= 4) return '•'.repeat(p.length);
  const head = p.slice(0, 2);
  const tail = p.slice(-3);
  return `${head}${'•'.repeat(Math.max(3, p.length - 5))}${tail}`;
}

/** e.g. "ABCDE1234F" -> "ABC•••34F". */
function maskPan(value) {
  const p = String(decryptPII(value) ?? '');
  if (!p) return p;
  if (p.length <= 4) return '•'.repeat(p.length);
  const head = p.slice(0, 3);
  const tail = p.slice(-3);
  return `${head}${'•'.repeat(Math.max(3, p.length - 6))}${tail}`;
}

// ── Object field helpers ────────────────────────────────────────────────────
// Convenience for encrypting/decrypting a known set of PII fields on an item.
// Returns a shallow copy; original is untouched. Missing fields are skipped.

// Default field names that hold phone/PAN across our own models AND the various
// per-lender outbound payloads / response bodies (used by the deep helpers so
// nested audit-log PII is encrypted regardless of the lender's key naming).
const PII_FIELDS = [
  // our canonical fields
  'phone', 'panNumber', 'pan', 'mobile', 'mobileNumber', 'phoneNumber', 'userPhoneNumber',
  // lender payload variants — phone
  'mobile_number', 'mobile_no', 'mobilenumber', 'phone_number', 'MobileNumber',
  // lender payload variants — PAN
  'pan_number', 'pancard', 'PanNumber',
];

function encryptFields(obj, fields = PII_FIELDS) {
  if (!obj || typeof obj !== 'object') return obj;
  const out = { ...obj };
  for (const f of fields) {
    if (out[f] !== undefined && out[f] !== null) out[f] = encryptPII(out[f]);
  }
  return out;
}

function decryptFields(obj, fields = PII_FIELDS) {
  if (!obj || typeof obj !== 'object') return obj;
  const out = { ...obj };
  for (const f of fields) {
    if (out[f] !== undefined && out[f] !== null) out[f] = decryptPII(out[f]);
  }
  return out;
}

// ── Deep helpers (for nested payloads: lender requestPayload/responseBody) ────
// Response-log tables store the outbound request and lender reply as nested
// objects. PII (phone/PAN) can appear at any depth under a variety of key names,
// so we walk the structure recursively and transform matching keys in place on a
// copy. Strings that are valid JSON are parsed, transformed, and re-stringified
// so stringified payloads are covered too. Never throws — on any issue the value
// is returned unchanged so high-volume write paths stay safe.

function _deepTransform(value, fieldSet, transform, depth) {
  if (depth > 12) return value; // guard against pathological nesting/cycles
  if (Array.isArray(value)) {
    return value.map((v) => _deepTransform(v, fieldSet, transform, depth + 1));
  }
  if (value && typeof value === 'object') {
    const out = {};
    for (const [k, v] of Object.entries(value)) {
      if (fieldSet.has(k) && (typeof v === 'string' || typeof v === 'number')) {
        out[k] = transform(v);
      } else {
        out[k] = _deepTransform(v, fieldSet, transform, depth + 1);
      }
    }
    return out;
  }
  if (typeof value === 'string') {
    // Attempt to handle stringified JSON payloads.
    const trimmed = value.trim();
    if (trimmed.length > 1 && (trimmed[0] === '{' || trimmed[0] === '[')) {
      try {
        const parsed = JSON.parse(value);
        const transformed = _deepTransform(parsed, fieldSet, transform, depth + 1);
        return JSON.stringify(transformed);
      } catch (_) {
        return value;
      }
    }
    return value;
  }
  return value;
}

/** Recursively encrypt any PII-named field within a nested object/array/JSON-string. */
function deepEncryptFields(payload, fields = PII_FIELDS) {
  if (payload === null || payload === undefined) return payload;
  try {
    return _deepTransform(payload, new Set(fields), encryptPII, 0);
  } catch (err) {
    console.error('[piiCrypto] deepEncryptFields failed, returning original:', err.message);
    return payload;
  }
}

/** Recursively decrypt any PII-named field within a nested object/array/JSON-string. */
function deepDecryptFields(payload, fields = PII_FIELDS) {
  if (payload === null || payload === undefined) return payload;
  try {
    return _deepTransform(payload, new Set(fields), decryptPII, 0);
  } catch (err) {
    console.error('[piiCrypto] deepDecryptFields failed, returning original:', err.message);
    return payload;
  }
}

module.exports = {
  PREFIX,
  PII_FIELDS,
  isEncrypted,
  encryptPII,
  decryptPII,
  maskPhone,
  maskPan,
  encryptFields,
  decryptFields,
  deepEncryptFields,
  deepDecryptFields,
};

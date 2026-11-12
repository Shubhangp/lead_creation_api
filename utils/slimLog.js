'use strict';
/**
 * Keep lender response logs small. DynamoDB charges storage and every write by
 * item size, so only the parts of a lender's response that the app actually
 * reads (status, message, lender ids) are stored.
 */

const MAX_STRING = 500;

const isPlainObject = (v) => v !== null && typeof v === 'object' && !Array.isArray(v) && !(v instanceof Date);
const isScalar = (v) => v === null || ['string', 'number', 'boolean'].includes(typeof v);
const cut = (v) => (typeof v === 'string' && v.length > MAX_STRING ? v.slice(0, MAX_STRING) + '…' : v);

/** Only the scalar fields of an object (one level), long strings shortened. */
function scalarsOf(obj) {
  if (!isPlainObject(obj)) return isScalar(obj) ? cut(obj) : undefined;
  const out = {};
  for (const [k, v] of Object.entries(obj)) if (isScalar(v) && v !== '') out[k] = cut(v);
  return out;
}

/**
 * responseBody → { every top-level scalar } + for each key in `shallow`, that
 * object's own scalars. Keys in `drop` are removed. Nested arrays/objects not
 * listed in `shallow` (offer lists, raw lender payloads, …) are not stored.
 */
function slimBody(body, { drop = [], shallow = [] } = {}) {
  if (body === null || body === undefined) return body;
  if (typeof body === 'string') return body.slice(0, 200); // e.g. an HTML 504 page
  if (!isPlainObject(body)) return body;
  const out = {};
  for (const [k, v] of Object.entries(body)) {
    if (drop.includes(k)) continue;
    if (isScalar(v)) { if (v !== '') out[k] = cut(v); continue; }
    if (shallow.includes(k)) {
      const s = scalarsOf(v);
      if (s !== undefined && !(isPlainObject(s) && Object.keys(s).length === 0)) out[k] = s;
    }
  }
  return out;
}

/** Remove only the listed keys; everything else is kept as it is. */
function dropKeys(body, keys) {
  if (!isPlainObject(body)) return body;
  const out = { ...body };
  keys.forEach((k) => delete out[k]);
  return out;
}

/** Request payloads: drop empty strings / null / empty objects (lossless). */
function compact(value) {
  if (Array.isArray(value)) return value.map(compact);
  if (!isPlainObject(value)) return value;
  const out = {};
  for (const [k, v] of Object.entries(value)) {
    if (v === '' || v === null || v === undefined) continue;
    const c = compact(v);
    if (isPlainObject(c) && Object.keys(c).length === 0) continue;
    out[k] = c;
  }
  return out;
}

/**
 * Mpokket request: the lead payload already carries everything in the dedupe
 * payload (email, mobile), so when both exist only the lead payload is kept,
 * without fields that just repeat another one. Empty fields are dropped.
 */
function slimMpokketRequest(req) {
  if (!isPlainObject(req)) return req;
  let out = req;
  if (isPlainObject(req.leadPayload)) {
    const lead = JSON.parse(JSON.stringify(req.leadPayload));
    const ai = lead.additional_info;
    if (isPlainObject(ai)) {
      if (ai.offical_email_id === lead.email_id) delete ai.offical_email_id;
      if (ai.company_address === ai.current_address) delete ai.company_address;
      if (ai.company_pincode === ai.current_pincode) delete ai.company_pincode;
    }
    out = { leadPayload: lead };
  }
  return compact(out);
}

/** Error details without the stack trace. */
function slimError(details) {
  if (!isPlainObject(details)) return details || null;
  const { stack, ...rest } = details; // eslint-disable-line no-unused-vars
  return scalarsOf(rest);
}

module.exports = { slimBody, dropKeys, compact, slimError, scalarsOf, slimMpokketRequest };

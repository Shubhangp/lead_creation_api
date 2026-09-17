const crypto = require('crypto');

const ALPHABET = '0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ';

const RANDOM_LEN = 8;

// RC + base36 millis (8 chars until year 2059) + RANDOM_LEN.
const PARTNER_REFERENCE_PATTERN = /^RC[0-9A-Z]{12,24}$/;

const BIAS_LIMIT = 256 - (256 % ALPHABET.length);

function randomChars(length) {
  let out = '';
  while (out.length < length) {
    for (const byte of crypto.randomBytes(length * 2)) {
      if (byte >= BIAS_LIMIT) continue;
      out += ALPHABET[byte % ALPHABET.length];
      if (out.length === length) break;
    }
  }
  return out;
}

function generatePartnerReferenceID() {
  return `RC${Date.now().toString(36).toUpperCase()}${randomChars(RANDOM_LEN)}`;
}

function isValidPartnerReferenceID(value) {
  return typeof value === 'string' && PARTNER_REFERENCE_PATTERN.test(value);
}

module.exports = {
  generatePartnerReferenceID,
  isValidPartnerReferenceID,
  PARTNER_REFERENCE_PATTERN,
};

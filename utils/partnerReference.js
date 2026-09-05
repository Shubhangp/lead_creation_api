const crypto = require('crypto');

const ALPHABET = '0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ';
const RANDOM_LEN = 5;

const PARTNER_REFERENCE_PATTERN = /^RC[0-9A-Z]{12,20}$/;

function randomChars(length) {
  const bytes = crypto.randomBytes(length);
  let out = '';
  for (let i = 0; i < length; i += 1) {
    out += ALPHABET[bytes[i] % ALPHABET.length];
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

const crypto = require('crypto');
const { getPartnerNumber } = require('../config/sourceAliases');

const ALPHABET = '0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ';

const RANDOM_LEN = 8;

// RC + {Partner_Number} (2 digits, see PARTNER_NUMBERS in config/sourceAliases.js)
//    + base36 millis (8 chars until year 2059) + RANDOM_LEN.
// e.g. FREO2 -> RC07MH9K2X1AB3CD4EF
const PARTNER_REFERENCE_PATTERN = /^RC[0-9]{2}[0-9A-Z]{12,24}$/;

// Pattern for one specific partner: /^RC{Partner_Number}[0-9A-Z]{12,24}$/
function partnerReferencePatternFor(source) {
  return new RegExp(`^RC${getPartnerNumber(source)}[0-9A-Z]{12,24}$`);
}

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

function generatePartnerReferenceID(source) {
  return `RC${getPartnerNumber(source)}${Date.now().toString(36).toUpperCase()}${randomChars(RANDOM_LEN)}`;
}

// With a source, also checks that the reference carries that partner's number.
function isValidPartnerReferenceID(value, source = null) {
  if (typeof value !== 'string') return false;
  return source ? partnerReferencePatternFor(source).test(value) : PARTNER_REFERENCE_PATTERN.test(value);
}

module.exports = {
  generatePartnerReferenceID,
  isValidPartnerReferenceID,
  PARTNER_REFERENCE_PATTERN,
  partnerReferencePatternFor,
};

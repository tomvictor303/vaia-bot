import extractJson from 'extract-json-from-string';
import crypto from 'crypto';

/**
 * True if value is a plain object (not null, not array) with at least one non-empty string value.
 * @param {*} value
 * @returns {boolean}
 */
export function isValidStringMap(value) {
  if (!value || typeof value !== 'object' || Array.isArray(value)) return false;
  return Object.values(value).some((v) => typeof v === 'string' && v.trim() !== '');
}

/**
 * Parse LLM output into JSON using extract-json-from-string with fallbacks.
 * Returns {} if parsing fails.
 */
export function llmOutputToJson(raw) {
  if (!raw || typeof raw !== 'string') return {};

  const candidates = extractJson(raw);
  if (candidates && candidates.length > 0 && typeof candidates[0] === 'object') {
    return candidates[0];
  }

  try {
    const cleaned = raw.replace(/```json/gi, '').replace(/```/g, '').trim();
    return JSON.parse(cleaned);
  } catch (error) {
    console.error('❌ Error parsing JSON::llmOutputToJson', error.message);
    console.error('❌ Raw JSON Input which caused the error::llmOutputToJson', raw);
    return {};
  }
}

/**
 * Compute SHA256 checksum of content.
 * @param {string} content - Content to hash.
 * @returns {string} Hexadecimal checksum.
 */
export function computeChecksum(content) {
  // Preserve historical logging behavior: skip hashing for falsy inputs.
  if (!content) return '';
  return crypto
    .createHash('sha256')
    .update(String(content).normalize('NFC'), 'utf8')
    .digest('hex');
}



import 'dotenv/config';
import { Tiktoken } from 'js-tiktoken/lite';
import cl100k_base from 'js-tiktoken/ranks/cl100k_base';
import { executeQuery, closePool } from './config/database.js';
import { MD_DATA_FIELDS } from './middleware/constants.js';

const MARKET_DATA_TABLE = process.env.MARKET_DATA_TABLE || 'market_data';

// Encoder used by GPT-3.5/4 and Perplexity sonar (OpenAI-compatible)
const encoder = new Tiktoken(cl100k_base);

/**
 * Actual token count using cl100k_base (same as GPT-3.5/4 and sonar).
 */
function countTokens(text) {
  if (!text || typeof text !== 'string') return 0;
  return encoder.encode(text).length;
}

/**
 * Sum actual token count for all data fields of one market_data record.
 */
function recordTokenCount(record) {
  let total = 0;
  for (const field of MD_DATA_FIELDS) {
    const value = record[field.name];
    if (value != null && typeof value === 'string') {
      total += countTokens(value);
    }
  }
  if (record.other_structured != null && typeof record.other_structured === 'string') {
    total += countTokens(record.other_structured);
  }
  return total;
}

async function main() {
  const query = `
    SELECT * FROM ${MARKET_DATA_TABLE}
    WHERE is_deleted = 0
    ORDER BY hotel_uuid
  `;
  const rows = await executeQuery(query, []);
  const results = rows.map((row) => ({
    hotel_uuid: row.hotel_uuid,
    tokens: recordTokenCount(row),
  }));

  let grandTotal = 0;
  console.log('Token counts per hotel (market_data):\n');
  console.log('hotel_uuid\t\t\t\t\t\ttokens');
  console.log('-'.repeat(70));
  for (const { hotel_uuid, tokens } of results) {
    console.log(`${hotel_uuid}\t${tokens}`);
    grandTotal += tokens;
  }
  console.log('-'.repeat(70));
  console.log(`Total hotels: ${results.length}`);
  console.log(`Total tokens: ${grandTotal}`);
  await closePool();
}

main().catch((err) => {
  console.error('❌', err.message);
  process.exit(1);
});

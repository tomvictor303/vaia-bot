import OpenAI from 'openai';
import { executeQuery } from '../config/database.js';
import { MarketDataService } from '../services/marketDataService.js';
import { MD_CAT_FIELDS, MD_PR_FIELDS } from '../middleware/constants.js';
import { llmOutputToJson } from '../utils/custom.js';
import { AIService } from '../services/aiService.js';

const HOTEL_PAGE_DATA_TABLE = process.env.HOTEL_PAGE_DATA_TABLE || 'hotel_page_data';

const CATEGORY_FIELDS = MD_CAT_FIELDS.map(f => ({ name: f.name, description: f.capture_description }));

const openai = new OpenAI({
  apiKey: process.env['PERPLEXITY_API_KEY'],
  baseURL: 'https://api.perplexity.ai',
});

// BEGIN getActiveMarkdownPages
/**
 * Fetch active markdown pages that have not been processed by the LLM (checksum diff).
 * @param {string} hotelUuid - Hotel UUID.
 * @returns {Promise<Array<{id: number, page_url: string, markdown: string, checksum: string, depth: number}>>}
 */
async function getActiveMarkdownPages(hotelUuid) {
  const query = `
    SELECT id, page_url, markdown, checksum, depth
    FROM ${HOTEL_PAGE_DATA_TABLE}
    WHERE active = 1 AND hotel_uuid = ? AND markdown IS NOT NULL AND markdown != '' AND NOT (checksum <=> llm_input_checksum) 
  `;
  try {
    return await executeQuery(query, [hotelUuid]);
  } catch (error) {
    console.error('❌ Error fetching markdown pages:', error.message);
    return [];
  }
}
// END getActiveMarkdownPages

// BEGIN markLLMInput
/**
 * Update LLM input metadata for a page after successful extraction.
 * @param {number} pageId - Page ID.
 * @param {string} checksum - Checksum used for the LLM input.
 * @returns {Promise<number>} Number of affected rows.
 */
async function markLLMInput(pageId, checksum) {
  const query = `
    UPDATE ${HOTEL_PAGE_DATA_TABLE}
    SET llm_input_checksum = ?, llm_updated = CURRENT_TIMESTAMP
    WHERE id = ?
  `;
  try {
    const result = await executeQuery(query, [checksum, pageId]);
    return result.affectedRows || 0;
  } catch (error) {
    console.error(`❌ Error updating LLM input metadata for page ${pageId}:`, error.message);
    return 0;
  }
}
// END markLLMInput

// BEGIN extractFieldsFromPage
/**
 * Extract category fields from a single markdown page via LLM.
 * @param {string} markdown - Markdown content of the page.
 * @param {string} pageUrl - Source page URL (for context).
 * @param {string} hotelNameLabel - Human-friendly hotel name for prompts.
 * @returns {Promise<Object<string, string>>} Key/value pairs for category fields.
 */
async function extractFieldsFromPage(markdown, pageUrl, hotelNameLabel) {
  hotelNameLabel = hotelNameLabel || 'the hotel';

  const describedFields = CATEGORY_FIELDS.map((f) => {
    const desc = (f.description || '').replace(/\[hotelName\]/g, hotelNameLabel);
    return `- "${f.name}" : ${desc}`;
  }).join('\n');

  const prompt = `You are extracting structured hotel information from Markdown content for ${hotelNameLabel}.
Return a JSON object with EXACTLY these keys (all string values; use "" if not found):
${describedFields}

Rules:
- Base your answers ONLY on the provided Markdown.
- Keep text concise but complete; preserve bullet-like lists as text (comma or semicolon separated).
- Do not invent data.
- If nothing relevant for a key, use "".
- Keep URLs if present.

Markdown source (from ${pageUrl}):
---
${markdown}
---`;

  const completion = await openai.chat.completions.create({
    model: 'sonar-pro',
    messages: [{ role: 'user', content: prompt }],
    max_tokens: 1500,
  });

  const content = completion.choices?.[0]?.message?.content || '';
  const parsed = llmOutputToJson(content);
  if (!parsed || typeof parsed !== 'object') {
    console.error('⚠️  Could not parse extraction response; returning empty object');
    return {};
  }
  return parsed;
}
// END extractFieldsFromPage

// BEGIN refineField
/**
 * Refine and merge snippets for a given field using LLM to remove duplicates and clean formatting.
 * @param {string} fieldName - Field name being refined.
 * @param {string[]} snippets - Snippets collected from pages.
 * @returns {Promise<string>} Refined field value.
 */
async function refineField(fieldName, snippets) {
  const joined = snippets.filter(Boolean).join('\n- ');
  if (!joined) return '';

  const prompt = `You are consolidating hotel information for the field "${fieldName}".
You will receive multiple snippets (bullets). Merge them into one clean, concise paragraph or bullet list.
Remove duplicates, keep URLs, fix formatting. Keep only factual info from the snippets.

Snippets:
- ${joined}

Return ONLY the merged text.`;

  const completion = await openai.chat.completions.create({
    model: 'sonar-pro',
    messages: [{ role: 'user', content: prompt }],
    max_tokens: 800,
  });

  return completion.choices[0]?.message?.content?.trim() || '';
}
// END refineField

// BEGIN extractPrimaryFields
/**
 * Derive primary fields from basic_information and optionally fetch missing via online search.
 * @param {string} basicInfoText - Aggregated basic information text.
 * @param {string} hotelNameLabel - Human-friendly hotel name for prompts.
 * @returns {Promise<Object<string, string>>} Primary field key/value pairs.
 */
async function extractPrimaryFields(basicInfoText, hotelNameLabel) {
  hotelNameLabel = hotelNameLabel || 'the hotel';
  const describedFields = MD_PR_FIELDS.map((f) => {
    const desc = (f.capture_description || '').replace(/\[hotelName\]/g, hotelNameLabel);
    return `- "${f.name}" : ${desc}`;
  }).join('\n');

  // First pass: extract basic information from the provided text
  const prompt = `Extract basic hotel information for ${hotelNameLabel}.
Return a JSON object with EXACTLY these keys (all string values; use "" if not found):
${describedFields}

Rules:
- Base your answers ONLY on the provided text.
- Keep text concise; do not invent data.
- If nothing relevant for a key, use "".
- Keep URLs if present.

Source text:
---
${basicInfoText || ''}
---`;

  const completion = await openai.chat.completions.create({
    model: 'sonar-pro',
    messages: [{ role: 'user', content: prompt }],
    max_tokens: 800,
  });

  const content = completion.choices?.[0]?.message?.content || '';
  let parsed = llmOutputToJson(content);
  if (!parsed || typeof parsed !== 'object') {
    console.error('⚠️  Could not parse primary fields; returning empty object');
    parsed = {};
  }

  // Second pass: if still missing, attempt online fetch for those primary fields
  const missing = MD_PR_FIELDS.filter((f) => {
    const val = parsed[f.name];
    return typeof val !== 'string' || val.trim() === '';
  }).map(f => f.name);

  if (missing.length > 0) {
    try {
      const online = await AIService.fetchHotelData(hotelNameLabel, missing);
      missing.forEach((key) => {
        if (typeof online?.[key] === 'string' && online[key].trim()) {
          parsed[key] = online[key];
        }
      });
    } catch (error) {
      console.log(`⚠️ Online fetch for primary fields failed: ${error.message}`);
    }
  }

  return parsed;
}
// END extractPrimaryFields

// BEGIN aggregateScrapedData
/**
 * **Entry point** of this controller.
 * 
 * Aggregates scraped markdown pages into structured market data via LLM extraction and refinement.
 * @param {string} hotelUuid - Hotel UUID to process.
 * @param {string} hotelName - Hotel name for prompt context and logging.
 * @returns {Promise<Object>} Aggregated market data payload.
 */
export async function aggregateScrapedData(hotelUuid, hotelName) {
  if (!hotelUuid) throw new Error('hotelUuid is required');

  const pages = await getActiveMarkdownPages(hotelUuid);
  if (!pages.length) {
    console.log(`⚠️  No markdown pages to aggregate for hotel ${hotelUuid}`);
    return null;
  }

  const fieldBuckets = Object.fromEntries(CATEGORY_FIELDS.map((f) => [f.name, []]));

  // Per-page extraction (Count(pages) LLM calls)
  console.log(`🔍 Extracting fields' data from pages...`);
  for (const page of pages) {
    try {
      const extracted = await extractFieldsFromPage(page.markdown, page.page_url, hotelName);
      CATEGORY_FIELDS.forEach((field) => {
        const val = extracted[field.name];
        if (typeof val === 'string' && val.trim()) {
          fieldBuckets[field.name].push(val.trim());
        }
      });
      await markLLMInput(page.id, page.checksum);
      console.log(`✅ Extraction: processed page ${page.id} (${page.page_url})`);
    } catch (error) {
      console.log(`❌ Extraction: failed page ${page.id} (${page.page_url}) -> ${error.message}`);
    }
  }

  // Per-field refinement (Count(schema fields) LLM calls)
  console.log(`🔍 Refining extracted fields' data...`);
  const merged = {};
  for (const field of CATEGORY_FIELDS) {
    merged[field.name] = await refineField(field.name, fieldBuckets[field.name]);
    console.log(`✅ Refining done: ${field.name}`);
  }

  // Derive primary fields from basic_information
  const primary = await extractPrimaryFields(merged['basic_information'], hotelName);
  MD_PR_FIELDS.forEach((f) => {
    merged[f.name] = typeof primary[f.name] === 'string' ? primary[f.name] : '';
  });

  // Persist to market_data via upsert
  const result = await MarketDataService.upsertMarketData(merged, hotelUuid);
  console.log(`✅ Finished aggregating data for hotel ${hotelName || hotelUuid}`, result);

  return merged;
}
// END aggregateScrapedData


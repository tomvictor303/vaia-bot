import OpenAI from 'openai';
import { executeQuery } from '../config/database.js';
import { MarketDataService } from '../services/marketDataService.js';
import { AIService } from '../services/aiService.js';
import { MD_CAT_FIELDS } from '../middleware/constants.js';
import { llmOutputToJson, isValidStringMap } from '../utils/custom.js';

const HOTEL_PAGE_DATA_TABLE = process.env.HOTEL_PAGE_DATA_TABLE || 'hotel_page_data';

const CATEGORY_FIELDS = MD_CAT_FIELDS.map(f => ({
  name: f.name,
  description: f.description,
  capture_guide: f.capture_guide,
}));

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
 * @param {string} [llm_output] - JSON string of extracted fields (e.g. JSON.stringify(extracted)); saved to llm_output column.
 * @returns {Promise<number>} Number of affected rows.
 */
async function markLLMInput(pageId, checksum, llm_output) {
  const query = `
    UPDATE ${HOTEL_PAGE_DATA_TABLE}
    SET llm_input_checksum = ?, llm_output = ?, llm_updated = CURRENT_TIMESTAMP
    WHERE id = ?
  `;
  try {
    const result = await executeQuery(query, [checksum, llm_output ?? null, pageId]);
    return result.affectedRows || 0;
  } catch (error) {
    console.error(`❌ Error updating LLM input metadata for page ${pageId}:`, error.message);
    return 0;
  }
}
// END markLLMInput

// BEGIN loadFieldBucketsFromCachedOutputs
/**
 * Load fieldBuckets from cached LLM outputs stored in hotel_page_data.llm_output.
 * Used by UNIT_TEST_ACTION=after_extract to skip per-page extraction LLM calls.
 * @param {string} hotelUuid - Hotel UUID.
 * @param {Object<string, Array<{ page_url: string, value: string }>>} fieldBuckets - Buckets to populate.
 * @returns {Promise<boolean>} True on success, false on fatal error.
 */
async function loadFieldBucketsFromCachedOutputs(hotelUuid, fieldBuckets) {
  console.log('🧪 UNIT_TEST_ACTION=after_extract: loading cached llm_output into field buckets (skipping per-page extraction).');
  const cachedQuery = `
    SELECT page_url, llm_output
    FROM ${HOTEL_PAGE_DATA_TABLE}
    WHERE active = 1 AND hotel_uuid = ? AND llm_output IS NOT NULL AND llm_output != ''
  `;
  let cachedPages = [];
  try {
    cachedPages = await executeQuery(cachedQuery, [hotelUuid]);
  } catch (error) {
    console.error('❌ Error loading cached llm_output for after_extract mode:', error.message);
    return false;
  }

  for (const page of cachedPages) {
    if (!page.llm_output) continue;
    let extracted;
    try {
      extracted = JSON.parse(page.llm_output);
    } catch (err) {
      console.error(`⚠️ Could not parse cached llm_output for page ${page.page_url}:`, err.message);
      continue;
    }
    CATEGORY_FIELDS.forEach((field) => {
      const val = extracted[field.name];
      if (typeof val === 'string' && val.trim()) {
        fieldBuckets[field.name].push({ page_url: page.page_url, value: val.trim() });
      }
    });
  }

  return true;
}
// END loadFieldBucketsFromCachedOutputs

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
    const guide = f.capture_guide
      ? ` (Capture guide: ${f.capture_guide})`
      : '';
    return `- "${f.name}" : ${desc}${guide}`;
  }).join('\n');

  const prompt = `You are extracting structured hotel information from Markdown content for ${hotelNameLabel}.
Return a JSON object with EXACTLY these keys (all string values; use "" if not found):
${describedFields}

Rules:
- Use **only the Markdown below** as the source. Do not fill keys from memory, training, or any source other than this Markdown.
- If nothing in the Markdown is relevant to a key, use "".
- Preserve bullet-like lists as text (comma or semicolon separated).
- Do not summarize the text. Keep the text as is.
- Do not invent data.
- Keep URLs if present.

Markdown source (from ${pageUrl}):
---
${markdown}
---`;

  const completion = await openai.chat.completions.create({
    model: 'sonar-pro',
    messages: [{ role: 'user', content: prompt }],
    max_tokens: 1024 * 16 * 4,
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

// BEGIN mergeAndRefineSnippets
/**
 * Merge and refine snippets for a given field using LLM to remove duplicates and clean formatting.
 * @param {string} fieldName - Field name being refined.
 * @param {Array<{ page_url: string, value: string }>} snippets - Snippets per page (page_url + value).
 * @returns {Promise<string>} Refined field value.
 */
const SNIPPET_DELIM = '\n<<<<<\n\n';

async function mergeAndRefineSnippets(fieldName, snippets) {
  const items = (snippets || []).filter((s) => s && s.value);
  const formatSnippet = (s, i) => {
    const value = String(s.value).trim();
    return `>>>>> Snippet ${i + 1} (page url: ${s.page_url || ''})\n${value}`;
  };
  const joined = items.map(formatSnippet).join(SNIPPET_DELIM);
  if (!joined) return '';

  const fieldDef = MD_CAT_FIELDS.find((f) => f.name === fieldName);
  const isOtherField = fieldName === 'other' || fieldName === 'other_structured';
  const fieldDescription = fieldDef?.description?.trim() || '';
  const descriptionLine = (!isOtherField && fieldDescription) ? `Field description: ${fieldDescription}\n` : '';
  const defaultMergeGuide = `Do not drop, rename, or replace explicitly named places, businesses, properties, room types, brands, services or programs, amenities, events, or routes with generic labels.`;
  const mergeGuide = fieldDef?.merge_guide?.trim() || '';
  const mergeGuideLine = `Merge guide: ${`${mergeGuide} ${defaultMergeGuide}`.trim()}\n`;
  const prioritizeLine = isOtherField ? '' : 'First priority: facts from snippets whose page URL is related to this field. Second priority: facts from the homepage.\n';
  
  const prompt = `You are consolidating hotel information for the field "${fieldName}".
${descriptionLine}${mergeGuideLine}
You will receive multiple snippets. Merge them into **one clean, human-readable, well-structured markdown text**.
${prioritizeLine}**Remove duplicates**, fix formatting. But keep all factual information from the snippets.
Do not include source page URLs in the merged text.
**Return ONLY the merged text.**

**Snippets:**
${joined}`;

  const completion = await openai.chat.completions.create({
    model: 'sonar-pro',
    messages: [{ role: 'user', content: prompt }],
    max_tokens: 1024 * 48,
  });

  return completion.choices[0]?.message?.content?.trim() || '';
}
// END mergeAndRefineSnippets

// BEGIN toOtherStructuredJson
/**
 * Convert free-form "other" text into a simple JSON string for storage.
 * - Keeps existing JSON if already valid.
 * - Otherwise splits lines, extracts key:value pairs, or stores raw notes.
 * @param {string} raw - Raw "other" content.
 * @returns {string} JSON string ('' if nothing to store).
 */
function toOtherStructuredJson(raw) {
  if (!raw) return '';

  if (typeof raw === 'string') {
    try {
      const parsed = JSON.parse(raw);
      if (Array.isArray(parsed) || (parsed && typeof parsed === 'object')) {
        return JSON.stringify(parsed);
      }
    } catch {
      // fall through
    }
  }

  const lines = String(raw)
    .split(/\r?\n+/)
    .map((line) => line.trim())
    .filter(Boolean)
    .map((line) => line.replace(/^[\-\*\u2022]\s*/, '').trim())
    .filter(Boolean);

  const entries = lines.map((line) => {
    const kv = line.match(/^([^:]+):\s*(.+)$/);
    if (kv) {
      return { name: kv[1].trim(), value: kv[2].trim() };
    }
    return { value: line };
  });

  return entries.length ? JSON.stringify(entries) : '';
}
// END toOtherStructuredJson

// BEGIN isFieldUpdated
/**
 * Check if a field is present in the merged payload (indicates an update).
 * @param {string} fieldName - Field to check.
 * @param {Object} mergedData - Merged payload (only includes changed fields).
 * @returns {boolean} True if the field exists in mergedData.
 */
function isFieldUpdated(fieldName, mergedData) {
  return Object.prototype.hasOwnProperty.call(mergedData, fieldName);
}
// END isFieldUpdated

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

  const unitTestAction = String(process.env.UNIT_TEST_ACTION || '').toLowerCase();

  const fieldBuckets = Object.fromEntries(CATEGORY_FIELDS.map((f) => [f.name, []]));

  // BEGIN EXTRACT_DATA_FROM_PAGES
  if (unitTestAction === 'after_extract') {
    // Load field buckets from cached outputs
    // This test action is used to **skip** the extraction step in the unit test.
    console.log(`🧪 UNIT_TEST_ACTION=after_extract: loading cached llm_output into field buckets (skipping per-page extraction).`);
    const ok = await loadFieldBucketsFromCachedOutputs(hotelUuid, fieldBuckets);
    if (!ok) {
      return null;
    }
  } else {
    // BEGIN EXTRACT_DATA_FROM_PAGES_BODY
    const pages = await getActiveMarkdownPages(hotelUuid);
    if (!pages.length) {
      console.log(`⚠️  No markdown pages to aggregate for hotel ${hotelUuid}`);
      return null;
    }
    // Per-page extraction (Count(pages) LLM calls)
    console.log(`🔍 Extracting fields' data from pages...`);
    for (const page of pages) {
      try {
        let extracted = await extractFieldsFromPage(page.markdown, page.page_url, hotelName);
        if (!isValidStringMap(extracted)) {
          console.log(`⚠️ Extraction empty for page ${page.id}, retrying once more...`);
          const retried = await extractFieldsFromPage(page.markdown, page.page_url, hotelName);
          extracted = isValidStringMap(retried) ? retried : {};
        }
        CATEGORY_FIELDS.forEach((field) => {
          const val = extracted[field.name];
          if (typeof val === 'string' && val.trim()) {
            fieldBuckets[field.name].push({ page_url: page.page_url, value: val.trim() });
          }
        });
        await markLLMInput(page.id, page.checksum, JSON.stringify(extracted));
        console.log(`✅ Extraction: processed page ${page.id} (${page.page_url})`);
      } catch (error) {
        console.log(`❌ Extraction: failed page ${page.id} (${page.page_url}) -> ${error.message}`);
      }
    }
    // END EXTRACT_DATA_FROM_PAGES_BODY
  }
  if (unitTestAction === 'extract') {
    console.log(`🧪 UNIT_TEST_ACTION=extract: stopping after extraction (skipping compose, merge, upsert). We are only interested in testing the extraction step.`);
    return null;
  }
  // END EXTRACT_DATA_FROM_PAGES

  // Per-field composition (Count(schema fields) LLM calls)
  // This is where the new data is composed from the extracted snippets (from multiple pages) by iterating each field.
  console.log(`🔍 Composing new data from extracted fields...`);
  const newData = {};
  for (const field of CATEGORY_FIELDS) {
    newData[field.name] = await mergeAndRefineSnippets(field.name, fieldBuckets[field.name]);
    console.log(`✅ Composed new data for field: ${field.name}`);
  }

  // DEBUG LOG: Save newData and source of new data (joined snippets from pages) to database
  // BEGIN DEBUG_LOG_SAVE_NEW_DATA_AND_JOINED_SNIPPETS_FROM_PAGES
  try {
    await MarketDataService.upsertMarketDataDebug1(fieldBuckets, newData, hotelUuid);
    console.log(`✅ Debug1 logs saved: ${hotelUuid}`);
  } catch (err) {
    console.error(`❌ Failed to upsert to market_data_debug1 for ${hotelUuid}:`, err.message);
  }
  // END DEBUG_LOG_SAVE_NEW_DATA_AND_JOINED_SNIPPETS_FROM_PAGES

  // Merge the new data with the existing data
  let mergedData = {};
  let DEBUG2_LOGS = {};
  const existingData = await MarketDataService.getMarketDataByUuid(hotelUuid);
  if (existingData) {
    // BEGIN MERGE_NEW_DATA_WITH_EXISTING_DATA
    for (const fieldName of Object.keys(newData)) {
      if (!newData[fieldName] || newData[fieldName] === 'N/A') {
        continue;
      }
      // If the existing data is empty for **current field**, set the new data
      if (!existingData[fieldName] || existingData[fieldName] === 'N/A') {
        mergedData[fieldName] = newData[fieldName];
        continue;
      }
      // if the values are the same, skip
      if (existingData[fieldName] === newData[fieldName]) {
        continue;
      }
      // Use LLM merge to determine if update is meaningful
      const { isUpdate, mergedText } = await AIService.mergeTextsByLLM(existingData[fieldName], newData[fieldName]);
      if (isUpdate && mergedText) {
        mergedData[fieldName] = mergedText;
      }
      // save for debug log
      DEBUG2_LOGS[fieldName] = {isUpdate, existingData: existingData[fieldName], newData: newData[fieldName], mergedText};
    }
    // END MERGE_NEW_DATA_WITH_EXISTING_DATA
  } else {
    mergedData = newData;
  }
  
  // BEGIN SAVE_DEBUG2_LOG
  if (Object.keys(DEBUG2_LOGS).length > 0) {    
    try {
      await MarketDataService.upsertMarketDataDebug2(DEBUG2_LOGS, hotelUuid);
      console.log(`✅ Debug2 logs saved: ${hotelUuid}`);
    } catch (err) {
      console.error(`❌ Failed to upsert to market_data_debug2 for ${hotelUuid}:`, err.message);
    }
  }
  // END SAVE_DEBUG2_LOG

  // Track "other" changes in a single check
  const otherUpdated = isFieldUpdated('other', mergedData);
  console.log('otherUpdated', otherUpdated);

  // If "other" changed, store structured JSON representation
  if (otherUpdated) {
    const sourceOther = mergedData.other || existingData?.other || '';
    let other_json = await AIService.textToJsonByLLM(sourceOther);
    mergedData.other_structured = JSON.stringify(other_json);
  }

  // Guardrail: no meaningful updates
  const updatedFieldsCount = Object.keys(mergedData).length;
  if (updatedFieldsCount === 0) {
    console.log(`⚠️ [${hotelName || hotelUuid}]: There is no significant new info to update.`);
  } else {
    // If there are significant new info, update the market_data
    console.log(`🔄 [${hotelName || hotelUuid}]: Update market_data via upsert... (fields updated: ${updatedFieldsCount})`);
    await MarketDataService.upsertMarketData(mergedData, hotelUuid);
  }
  console.log(`✅ Finished aggregating data for hotel ${hotelName || hotelUuid} (fields updated: ${updatedFieldsCount})`);

  return newData;
}
// END aggregateScrapedData


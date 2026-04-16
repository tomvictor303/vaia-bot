import { executeQuery } from '../config/database.js';
import { MarketDataService } from '../services/marketDataService.js';
import { AIService } from '../services/aiService.js';
import { MD_CAT_FIELDS, TABLE_NAMES, STAGE_NAMES } from '../middleware/constants.js';
import { llmOutputToJson, isValidStringMap, computeChecksum } from '../utils/custom.js';

const { HOTEL_PAGE_DATA_TABLE } = TABLE_NAMES;

const CATEGORY_FIELDS = MD_CAT_FIELDS.map(f => ({
  name: f.name,
  description: f.description,
  capture_guide: f.capture_guide,
}));

// BEGIN getActiveMarkdownPages
/**
 * Fetch active markdown pages that have not been processed by the LLM (checksum diff).
 * @param {string} hotelUuid - Hotel UUID.
 * @returns {Promise<Array<{id: number, page_url: string, markdown: string, markdown_prev: string|null, checksum: string, depth: number}>>}
 */
async function getActiveMarkdownPages(hotelUuid) {
  const query = `
    SELECT id, page_url, markdown, markdown_prev, checksum, depth
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
 * @returns {Promise<{ ok: boolean, pagesActive: number, pagesAnalyzed: number }>} Operation status and page counts.
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
    return { ok: false, pagesActive: 0, pagesAnalyzed: 0 };
  }

  let pagesAnalyzed = 0;
  for (const page of cachedPages) {
    pagesAnalyzed += 1;
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

  return { ok: true, pagesActive: cachedPages.length, pagesAnalyzed };
}
// END loadFieldBucketsFromCachedOutputs

// BEGIN extractFieldsFromPage
/**
 * Extract category fields from a single markdown page via LLM.
 * @param {string} markdown - Markdown content of the page.
 * @param {string} pageUrl - Source page URL (for context).
 * @param {string} hotelNameLabel - Human-friendly hotel name for prompts.
 * @param {Object} hotelLLMUsage - Per-hotel LLM usage accumulator.
 * @returns {Promise<Object<string, string>>} Key/value pairs for category fields.
 */
async function extractFieldsFromPage(markdown, pageUrl, hotelNameLabel, hotelLLMUsage) {
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
- Do not drop, rename, or replace explicitly named places, businesses, properties, room types, brands, services or programs, amenities, events, or routes with generic labels.
- Do not invent data.
- Keep URLs if present.

Markdown source (from ${pageUrl}):
---
${markdown}
---`;
  const { text } = await AIService.askLLM({
    prompt,
    maxTokens: 1024 * 64,
    jsonMode: true,
  }, hotelLLMUsage);

  const parsed = llmOutputToJson(text);
  if (!parsed || typeof parsed !== 'object') {
    console.error('⚠️  Could not parse extraction response; returning empty object');
    return {};
  }
  return parsed;
}
// END extractFieldsFromPage

// BEGIN aggregateAndRefineSnippets
/**
 * Merge and refine snippets for a given field using LLM to remove duplicates and clean formatting.
 * @param {string} fieldName - Field name being refined.
 * @param {Array<{ page_url: string, value: string }>} snippets - Snippets per page (page_url + value).
 * @param {Object} hotelLLMUsage - Per-hotel LLM usage accumulator.
 * @returns {Promise<string>} Refined field value.
 */
const SNIPPET_DELIM = '\n<<<<<\n\n';

async function aggregateAndRefineSnippets(fieldName, snippets, hotelLLMUsage) {
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
  const { text } = await AIService.askLLM({
    prompt,
    maxTokens: 1024 * 64,
    jsonMode: false,
  }, hotelLLMUsage);

  return text.trim() || '';
}
// END aggregateAndRefineSnippets

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

// BEGIN loadMarketDataFromScrapedPage
/**
 * **Entry point** of this controller.
 * 
 * Loads and processes scraped markdown pages into market data.
 * Runs AI extract/aggregate/merge/finalize stages and upserts when meaningful updates exist.
 * @param {Object} logger - Per-run logger instance
 * @param {string} hotelUuid - Hotel UUID to process.
 * @param {string} hotelName - Hotel name for prompt context and logging.
 * @returns {Promise<Object|null>} Composed newData object, or null when stopped early (unit-test mode/no pages).
 */
export async function loadMarketDataFromScrapedPage(logger, hotelUuid, hotelName) {
  if (!logger?.runId) throw new Error('logger with runId is required');
  if (!hotelUuid) throw new Error('hotelUuid is required');

  const unitTestAction = String(process.env.UNIT_TEST_ACTION || '').toLowerCase();
  let pagesActive = 0;
  let pagesAnalyzed = 0;
  const hotelLLMUsage = { total_tokens: 0, input_tokens: 0, output_tokens: 0, cost: 0 };

  const fieldBuckets = Object.fromEntries(CATEGORY_FIELDS.map((f) => [f.name, []]));

  // BEGIN EXTRACT_DATA_FROM_PAGES
  await logger.markStage(STAGE_NAMES.AI_EXTRACT);
  await logger.event('ai_extract.started');
  if (unitTestAction === 'after_extract') {
    // Load field buckets from cached outputs
    // This test action is used to **skip** the extraction step in the unit test.
    console.log(`🧪 UNIT_TEST_ACTION=after_extract: loading cached llm_output into field buckets (skipping per-page extraction).`);
    const cachedResult = await loadFieldBucketsFromCachedOutputs(hotelUuid, fieldBuckets);
    pagesActive = cachedResult?.pagesActive ?? 0;
    pagesAnalyzed = cachedResult?.pagesAnalyzed ?? 0;
    if (!cachedResult?.ok) {
      return null;
    }
  } else {
    // BEGIN EXTRACT_DATA_FROM_PAGES_BODY
    const pages = await getActiveMarkdownPages(hotelUuid);
    if (!pages.length) {
      console.log(`⚠️  No markdown pages to aggregate for hotel ${hotelUuid}`);
      return null;
    }
    pagesActive = pages.length;
    // Per-page extraction (Count(pages) LLM calls)
    console.log(`🔍 Extracting fields' data from pages...`);
    for (const page of pages) {
      pagesAnalyzed += 1;
      const pageStartedAtMs = Date.now();
      const usedTokensBefore = hotelLLMUsage.total_tokens || 0;
      try {
        let extracted = await extractFieldsFromPage(page.markdown, page.page_url, hotelName, hotelLLMUsage);
        if (!isValidStringMap(extracted)) {
          console.log(`⚠️ Extraction empty for page ${page.id}, retrying once more...`);
          const retried = await extractFieldsFromPage(page.markdown, page.page_url, hotelName, hotelLLMUsage);
          extracted = isValidStringMap(retried) ? retried : {};
        }
        CATEGORY_FIELDS.forEach((field) => {
          const val = extracted[field.name];
          if (typeof val === 'string' && val.trim()) {
            fieldBuckets[field.name].push({ page_url: page.page_url, value: val.trim() });
          }
        });
        const llmOutputJson = JSON.stringify(extracted);
        await markLLMInput(page.id, page.checksum, llmOutputJson);
        await logger.updatePageLog(page.page_url, {
          page_depth: page.depth ?? 0,
          extraction_status: 'success',
          total_tokens: Math.max(0, (hotelLLMUsage.total_tokens || 0) - usedTokensBefore),
          duration_ms: Date.now() - pageStartedAtMs,
          error_message: '',
          markdown: page.markdown,
          markdown_prev: page.markdown_prev ?? '',
          llm_output: llmOutputJson,
        });
        console.log(`✅ Extraction: processed page ${page.id} (${page.page_url})`);
      } catch (error) {
        await logger.updatePageLog(page.page_url, {
          page_depth: page.depth ?? 0,
          extraction_status: 'fail',
          total_tokens: Math.max(0, (hotelLLMUsage.total_tokens || 0) - usedTokensBefore),
          duration_ms: Date.now() - pageStartedAtMs,
          error_message: error?.message || String(error),
        });
        console.log(`❌ Extraction: failed page ${page.id} (${page.page_url}) -> ${error.message}`);
      }
    }
    // END EXTRACT_DATA_FROM_PAGES_BODY
  }
  await logger.updateRun({
    pages_active: pagesActive,
    pages_analyzed: pagesAnalyzed,
    model_version: process.env.LLM_MODEL_VERSION || '',
    prompt_version: process.env.LLM_PROMPT_VERSION || '',
    total_tokens: hotelLLMUsage.total_tokens,
    cost: hotelLLMUsage.cost,
  });
  await logger.event('ai_extract.completed');
  if (unitTestAction === 'extract') {
    console.log(`🧪 UNIT_TEST_ACTION=extract: stopping after extraction (skipping compose, merge, upsert). We are only interested in testing the extraction step.`);
    return null;
  }
  // END EXTRACT_DATA_FROM_PAGES

  // BEGIN NEW_DATA_COMPOSE_BY_AI_AGGREGATE_OF_SNIPPETS
  // Per-field composition (Count(schema fields) LLM calls)
  // This is where the new data is composed from the extracted snippets (from multiple pages) by iterating each field.
  await logger.markStage(STAGE_NAMES.AI_AGGREGATE);
  await logger.event('ai_aggregate.started');
  console.log(`🔍 Composing new data from extracted fields...`);
  const newData = {};
  for (const field of CATEGORY_FIELDS) {
    const categoryStartedAtMs = Date.now();
    const tokensBefore = hotelLLMUsage.total_tokens || 0;
    newData[field.name] = await aggregateAndRefineSnippets(field.name, fieldBuckets[field.name], hotelLLMUsage);
    const newFieldText = newData[field.name] || '';
    const snippetsText = JSON.stringify(fieldBuckets[field.name] || []);
    await logger.categoryLog(field.name, {
      snippets_count: (fieldBuckets[field.name] || []).length,
      snippets: snippetsText,
      new_text: newFieldText,
      old_text: '',
      merged_text: newFieldText,
      output_hash: computeChecksum(newFieldText),
      total_tokens_aggregate: Math.max(0, (hotelLLMUsage.total_tokens || 0) - tokensBefore),
      duration_ms: Date.now() - categoryStartedAtMs,
    });
    console.log(`✅ Composed new data for field: ${field.name}`);
  }
  await logger.event('ai_aggregate.completed');
  // END NEW_DATA_COMPOSE_BY_AI_AGGREGATE_OF_SNIPPETS

  // BEGIN DEBUG_LOG_SAVE_NEW_DATA_AND_JOINED_SNIPPETS_FROM_PAGES
  // DEBUG LOG: Save newData and source of new data (joined snippets from pages) to database
  try {
    await MarketDataService.upsertMarketDataDebug1(fieldBuckets, newData, hotelUuid);
    console.log(`✅ Debug1 logs saved: ${hotelUuid}`);
  } catch (err) {
    console.error(`❌ Failed to upsert to market_data_debug1 for ${hotelUuid}:`, err.message);
  }
  // END DEBUG_LOG_SAVE_NEW_DATA_AND_JOINED_SNIPPETS_FROM_PAGES

  // BEGIN AI_MERGE_FOR_NEW_AND_EXISTING_DATA
  // Merge the new data with the existing data
  await logger.markStage(STAGE_NAMES.AI_MERGE);
  await logger.event('ai_merge.started');
  // BEGIN AI_MERGE_FOR_NEW_AND_EXISTING_DATA_BODY
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
      const tokensBefore = hotelLLMUsage.total_tokens || 0;
      const { isUpdate, mergedText } = await AIService.mergeTextsByLLM(existingData[fieldName], newData[fieldName], hotelLLMUsage);
      if (isUpdate && mergedText) {
        mergedData[fieldName] = mergedText;
        await logger.updateCategoryLog(fieldName, {
          snippets_count: (fieldBuckets[fieldName] || []).length,
          snippets: JSON.stringify(fieldBuckets[fieldName] || []),
          old_text: existingData[fieldName],
          new_text: newData[fieldName],
          is_updated: 1,
          merged_text: mergedText,
          output_hash: computeChecksum(mergedText),
          total_tokens_merge: Math.max(0, (hotelLLMUsage.total_tokens || 0) - tokensBefore),
        });
      }
      // save for debug log
      DEBUG2_LOGS[fieldName] = {isUpdate, existingData: existingData[fieldName], newData: newData[fieldName], mergedText};
    }
    // END MERGE_NEW_DATA_WITH_EXISTING_DATA
  } else {
    mergedData = newData; // If there is no existing data, use the new data entirely.
  }
  // END AI_MERGE_FOR_NEW_AND_EXISTING_DATA_BODY

  await logger.event('ai_merge.completed');
  // END AI_MERGE_FOR_NEW_AND_EXISTING_DATA
  
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
  await logger.markStage(STAGE_NAMES.AI_FINALIZE);
  await logger.event('ai_finalize.started');
  const otherUpdated = isFieldUpdated('other', mergedData);
  console.log('otherUpdated', otherUpdated);

  // If "other" changed, store structured JSON representation
  if (otherUpdated) {
    const sourceOther = mergedData.other || existingData?.other || '';
    let other_json = await AIService.textToJsonByLLM(sourceOther, hotelLLMUsage);
    mergedData.other_structured = JSON.stringify(other_json);
  }
  await logger.event('ai_finalize.completed');

  // Guardrail: no meaningful updates
  await logger.markStage(STAGE_NAMES.SAVE);
  const updatedFieldsCount = Object.keys(mergedData).length;
  if (updatedFieldsCount === 0) {
    console.log(`⚠️ [${hotelName || hotelUuid}]: There is no significant new info to update.`);
  } else {
    // If there are significant new info, update the market_data
    console.log(`🔄 [${hotelName || hotelUuid}]: Update market_data via upsert... (fields updated: ${updatedFieldsCount})`);
    await MarketDataService.upsertMarketData(mergedData, hotelUuid);    
    await logger.event('save.completed');
    await logger.updateRun({
      categories_updated: updatedFieldsCount,
      model_version: process.env.LLM_MODEL_VERSION || '',
      prompt_version: process.env.LLM_PROMPT_VERSION || '',
      total_tokens: hotelLLMUsage.total_tokens,
      cost: hotelLLMUsage.cost,
    });
  }
  console.log(`✅ Finished aggregating data for hotel ${hotelName || hotelUuid} (fields updated: ${updatedFieldsCount})`);

  return newData;
}
// END loadMarketDataFromScrapedPage


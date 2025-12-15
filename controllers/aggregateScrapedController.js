import OpenAI from 'openai';
import { executeQuery } from '../config/database.js';
import { MarketDataService } from '../services/marketDataService.js';

const HOTEL_PAGE_DATA_TABLE = process.env.HOTEL_PAGE_DATA_TABLE || 'hotel_page_data';

// Category fields (16) plus "other"
const CATEGORY_FIELDS = [
  'hotel_information',
  'accessibility',
  'amenities',
  'cleanliness_enhancements',
  'food_beverage',
  'guest_rooms',
  'guest_services_front_desk',
  'housekeeping_laundry',
  'local_area_information',
  'meeting_events',
  'on_property_convenience',
  'parking_transportation',
  'policies',
  'recreation_fitness',
  'safety_security',
  'technology_business_services',
  'other',
];

const openai = new OpenAI({
  apiKey: process.env['PERPLEXITY_API_KEY'],
  baseURL: 'https://api.perplexity.ai',
});

async function getActiveMarkdownPages(hotelUuid) {
  const query = `
    SELECT id, page_url, markdown
    FROM ${HOTEL_PAGE_DATA_TABLE}
    WHERE active = 1 AND hotel_uuid = ? AND markdown IS NOT NULL AND markdown != ''
  `;
  try {
    return await executeQuery(query, [hotelUuid]);
  } catch (error) {
    console.error('❌ Error fetching markdown pages:', error.message);
    return [];
  }
}

async function extractFieldsFromPage(markdown, pageUrl) {
  const prompt = `You are extracting structured hotel information from Markdown content.
Return a JSON object with EXACTLY these keys (all string values; use "" if not found):
${CATEGORY_FIELDS.map((f) => `- "${f}"`).join('\n')}

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

  // best-effort JSON parse
  const content = completion.choices[0]?.message?.content || '{}';
  try {
    return JSON.parse(content);
  } catch {
    try {
      const wrapped = content
        .replace(/```json/gi, '')
        .replace(/```/g, '')
        .trim();
      return JSON.parse(wrapped);
    } catch {
      console.error('⚠️  Could not parse extraction response; returning empty object');
      return {};
    }
  }
}

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

export async function aggregateScrapedData(hotelUuid, hotelName = '') {
  if (!hotelUuid) throw new Error('hotelUuid is required');

  const pages = await getActiveMarkdownPages(hotelUuid);
  if (!pages.length) {
    console.log(`⚠️  No markdown pages to aggregate for hotel ${hotelUuid}`);
    return null;
  }

  const fieldBuckets = Object.fromEntries(CATEGORY_FIELDS.map((f) => [f, []]));

  // Per-page extraction (Count(pages) LLM calls)
  console.log(`🔍 Extracting fields' data from pages...`);
  for (const page of pages) {
    try {
      const extracted = await extractFieldsFromPage(page.markdown, page.page_url);
      CATEGORY_FIELDS.forEach((field) => {
        const val = extracted[field];
        if (typeof val === 'string' && val.trim()) {
          fieldBuckets[field].push(val.trim());
        }
      });
      console.log(`✅ Extraction: processed page ${page.id} (${page.page_url})`);
    } catch (error) {
      console.log(`❌ Extraction: failed page ${page.id} (${page.page_url}) -> ${error.message}`);
    }
  }

  // Per-field refinement (Count(schema fields) LLM calls)
  console.log(`🔍 Refining extracted fields' data...`);
  const merged = {};
  for (const field of CATEGORY_FIELDS) {
    merged[field] = await refineField(field, fieldBuckets[field]);
    console.log(`✅ Refining done: ${field}`);
  }

  // Persist to market_data via upsert
  const result = await MarketDataService.upsertMarketData(merged, hotelUuid);
  console.log(`✅ Finished aggregating data for hotel ${hotelName || hotelUuid}`, result);

  return merged;
}


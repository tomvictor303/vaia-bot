import { executeQuery } from '../config/database.js';
import TurndownService from 'turndown';

const HOTEL_PAGE_DATA_TABLE = process.env.HOTEL_PAGE_DATA_TABLE || 'hotel_page_data';
const turndown = new TurndownService({ headingStyle: 'atx' });

async function getActivePages(hotelUuid) {
  const query = `
    SELECT id, page_url, content
    FROM ${HOTEL_PAGE_DATA_TABLE}
    WHERE active = 1 AND hotel_uuid = ?
  `;
  try {
    return await executeQuery(query, [hotelUuid]);
  } catch (error) {
    console.error('❌ Error fetching active pages:', error.message);
    return [];
  }
}

async function saveMarkdown(id, markdown) {
  const query = `
    UPDATE ${HOTEL_PAGE_DATA_TABLE}
    SET lll_output = ?, updated_at = CURRENT_TIMESTAMP
    WHERE id = ?
  `;
  try {
    const result = await executeQuery(query, [markdown, id]);
    return result.affectedRows || 0;
  } catch (error) {
    console.error(`❌ Error saving markdown for page ${id}:`, error.message);
    return 0;
  }
}

export async function refineScrapedData(hotelUuid, hotelName = '') {
  if (!hotelUuid) {
    throw new Error('hotelUuid is required to refine page data');
  }

  const label = hotelName ? `${hotelName} (${hotelUuid})` : hotelUuid;
  console.log(`📝 Refining page data to Markdown for hotel ${label}...`);
  const pages = await getActivePages(hotelUuid);
  console.log(`📄 Active pages to process: ${pages.length}`);

  let updated = 0;
  for (const page of pages) {
    try {
      const markdown = turndown.turndown(page.content || '');
      const affected = await saveMarkdown(page.id, markdown);
      if (affected > 0) updated += 1;
      console.log(`✅ Converted page ${page.id} (${page.page_url})`);
    } catch (error) {
      console.error(`❌ Failed converting page ${page.id}:`, error.message);
    }
  }

  console.log(`🏁 Refinement complete. Updated ${updated}/${pages.length} pages.`);
}


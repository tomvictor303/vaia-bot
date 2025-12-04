import { PlaywrightCrawler } from 'crawlee';
import crypto from 'crypto';
import { executeQuery } from '../config/database.js';

// Get table name from environment variable, default to 'hotel_data'
const HOTEL_DATA_TABLE = process.env.HOTEL_DATA_TABLE || 'hotel_data';

/**
 * Save scraped page to database
 * @param {string} hotelUuid - Hotel UUID
 * @param {string} url - Page URL
 * @param {string} html - Full HTML content
 * @param {string} checksum - SHA256 checksum of the HTML
 * @returns {Promise<number>} Insert ID or affected rows
 */
async function saveScrapedPage(hotelUuid, url, html, checksum) {
  // Validate inputs
  if (!hotelUuid || typeof hotelUuid !== 'string') {
    throw new Error(`Invalid hotelUuid: ${typeof hotelUuid}`);
  }
  if (!url || typeof url !== 'string') {
    throw new Error(`Invalid url: ${typeof url}`);
  }
  if (!html || typeof html !== 'string') {
    throw new Error(`Invalid html: ${typeof html}`);
  }
  if (!checksum || typeof checksum !== 'string') {
    throw new Error(`Invalid checksum: ${typeof checksum}`);
  }

  // Check if record already exists for this hotel_uuid and page_url
  const checkQuery = `
    SELECT id FROM ${HOTEL_DATA_TABLE}
    WHERE hotel_uuid = ? AND page_url = ? AND active = 1
    LIMIT 1
  `;

  try {
    const existing = await executeQuery(checkQuery, [hotelUuid, url]);
    
    if (existing && existing.length > 0) {
      // Update existing record
      const updateQuery = `
        UPDATE ${HOTEL_DATA_TABLE}
        SET content = ?,
            checksum = ?,
            updated_at = CURRENT_TIMESTAMP,
            active = 1
        WHERE id = ?
      `;
      const result = await executeQuery(updateQuery, [html, checksum, existing[0].id]);
      return result.affectedRows;
    } else {
      // Insert new record
      const insertQuery = `
        INSERT INTO ${HOTEL_DATA_TABLE} (hotel_uuid, page_url, checksum, content, active)
        VALUES (?, ?, ?, ?, 1)
      `;
      const result = await executeQuery(insertQuery, [hotelUuid, url, checksum, html]);
      return result.insertId || result.affectedRows;
    }
  } catch (error) {
    // Use safe error message extraction to avoid serialization issues
    const errorMessage = error?.message || String(error) || 'Unknown database error';
    const errorDetails = {
      message: errorMessage,
      hotelUuid: hotelUuid?.substring(0, 50),
      url: url?.substring(0, 100),
      htmlLength: html?.length || 0,
      checksum: checksum?.substring(0, 20),
    };
    console.error(`❌ Error saving scraped page to database:`, errorDetails);
    throw new Error(`Database save failed: ${errorMessage}`);
  }
}

/**
 * Compute SHA256 checksum of content
 * @param {string} content - Content to hash
 * @returns {string} Hexadecimal checksum
 */
function computeChecksum(content) {
  return crypto
    .createHash('sha256')
    .update(content)
    .digest('hex');
}

/**
 * Scrape a hotel website using PlaywrightCrawler
 * Crawls all pages from the hotel's main URL (crawl all mode)
 * 
 * @param {string} hotelUrl - Starting URL for the hotel
 * @param {string} hotelUuid - Hotel UUID
 * @param {string} hotelName - Hotel name (for logging)
 * @returns {Promise<Object>} Scraping statistics
 */
export async function scrapeHotel(hotelUrl, hotelUuid, hotelName) {
  if (!hotelUrl || !hotelUrl.startsWith('http')) {
    throw new Error(`Invalid hotel URL: ${hotelUrl}`);
  }

  const maxDepth = Number.isNaN(parseInt(process.env.CRAWLER_MAX_DEPTH ?? '', 10))
    ? Infinity
    : parseInt(process.env.CRAWLER_MAX_DEPTH ?? '', 10);
  const maxConcurrency = parseInt(process.env.CRAWLER_MAX_CONCURRENCY || '3', 10);
  const maxRetries = parseInt(process.env.CRAWLER_MAX_RETRIES || '2', 10);
  const timeoutSecs = parseInt(process.env.CRAWLER_TIMEOUT_SECS || '60', 10);
  const blockedExtensions = new Set(['.jpg', '.jpeg', '.png', '.gif', '.webp', '.svg', '.ico', '.bmp', '.mp4', '.avi', '.mov', '.wmv', '.flv', '.webm', '.mkv', '.m4v', '.pdf', '.mp3', '.wav', '.ogg', '.aac', '.flac']);
  const visited = new Set();
  const stats = { scraped: 0, skipped: 0, errors: 0 };

  console.log(`\n🕷️  Crawl-all mode: ${hotelName}`);
  console.log(`📍 Start URL: ${hotelUrl}`);
  console.log(`📏 Max depth: ${maxDepth === Infinity ? 'unlimited' : maxDepth}`);

  const crawler = new PlaywrightCrawler({
    maxConcurrency,
    maxRequestRetries: maxRetries,
    requestHandlerTimeoutSecs: timeoutSecs,
    launchContext: { launchOptions: { headless: true } },

    async requestHandler({ page, request, response, enqueueLinks, log }) {
      const currentDepth = request.userData?.depth ?? 0;
      const pageUrl = response?.url() || request.url;
      if (pageUrl !== request.url) {
        log.info(`🔁 Redirected: ${request.url} → ${pageUrl}`);
      }

      if (visited.has(pageUrl)) {
        stats.skipped += 1;
        return;
      }
      if (maxDepth !== Infinity && currentDepth > maxDepth) {
        stats.skipped += 1;
        return;
      }

      try {
        await page.waitForLoadState('domcontentloaded', { timeout: 30000 }).catch(() => {});
        await page.waitForSelector('body', { timeout: 5000 }).catch(() => {});

        const status = response?.status();
        const title = (await page.title().catch(() => '') || '').toLowerCase();
        if (status && status >= 400) {
          log.warning(`⚠️  Skipping error page (status ${status}): ${pageUrl}`);
          stats.errors += 1;
          return;
        }
        if (title.includes('404') || title.includes('500')) {
          log.warning(`⚠️  Skipping page due to error code intitle (${title}): ${pageUrl}`);
          stats.errors += 1;
          return;
        }

        const html = await page.content();
        if (!html || html.trim().length === 0) {
          log.warning(`⚠️  Empty HTML: ${pageUrl}`);
          stats.errors += 1;
          return;
        }

        const checksum = computeChecksum(html);
        await saveScrapedPage(hotelUuid, pageUrl, html, checksum);
        visited.add(pageUrl);
        stats.scraped += 1;
        log.info(`✅ Saved: ${pageUrl}`);

        const rawLinks = await page.$$eval('a[href]', anchors =>
          anchors.map(a => a.getAttribute('href') || '').filter(Boolean)
        ).catch(() => []);

        const urlsToEnqueue = rawLinks
          .map((href) => {
            try {
              return new URL(href, pageUrl).toString();
            } catch {
              return '';
            }
          })
          .filter(Boolean);

        if (urlsToEnqueue.length > 0) {
          await enqueueLinks({
            urls: urlsToEnqueue,
            transformRequestFunction: (newReq) => {
              if (!newReq?.url) return false;

              const lower = newReq.url.toLowerCase();
              if (lower.startsWith('javascript:') || lower.startsWith('tel:')) return false;
              if (Array.from(blockedExtensions).some(ext => lower.endsWith(ext))) return false;
              if (visited.has(newReq.url)) return false;
              if (maxDepth !== Infinity && currentDepth + 1 > maxDepth) return false;

              newReq.userData = { depth: currentDepth + 1 };
              return newReq;
            },
          });
        }
      } catch (error) {
        stats.errors += 1;
        log.error(`❌ Failed: ${request.url} -> ${error?.message || error}`);
      }
    },

    errorHandler({ request, log, error }) {
      stats.errors += 1;
      log.error(`❌ Handler error: ${request.url} -> ${error?.message || error}`);
    },
  });

  try {
    await crawler.run([{ url: hotelUrl, userData: { depth: 0 } }]);
  } catch (error) {
    console.error(`❌ Fatal crawl error for ${hotelName}: ${error?.message || error}`);
    throw error;
  }

  console.log(`\n📊 Crawl summary for ${hotelName}`);
  console.log(`   ✅ Scraped: ${stats.scraped}`);
  console.log(`   ⏭️  Skipped: ${stats.skipped}`);
  console.log(`   ❌ Errors: ${stats.errors}`);

  return {
    hotelUuid,
    hotelName,
    hotelUrl,
    pagesScraped: stats.scraped,
    pagesSkipped: stats.skipped,
    errors: stats.errors,
    totalPages: stats.scraped + stats.skipped,
  };
}


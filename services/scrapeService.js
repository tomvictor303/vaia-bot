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
  // Validate input
  if (!hotelUrl || !hotelUrl.startsWith('http')) {
    throw new Error(`Invalid hotel URL: ${hotelUrl}`);
  }

  // Configuration
  const maxDepth = parseInt(process.env.CRAWLER_MAX_DEPTH || '3', 10);
  const maxConcurrency = parseInt(process.env.CRAWLER_MAX_CONCURRENCY || '3', 10);
  const maxRetries = parseInt(process.env.CRAWLER_MAX_RETRIES || '2', 10);
  const timeout = parseInt(process.env.CRAWLER_TIMEOUT_SECS || '60', 10);
  const blockedExtensions = ['.jpg', '.jpeg', '.png', '.gif', '.webp', '.svg', '.ico', '.bmp', '.mp4', '.avi', '.mov', '.wmv', '.flv', '.webm', '.mkv', '.m4v', '.pdf', '.mp3', '.wav', '.ogg', '.aac', '.flac'];

  // Statistics tracking
  const stats = {
    pagesScraped: 0,
    pagesSkipped: 0,
    errors: 0,
    scrapedUrls: new Set(),
  };

  // Logging
  console.log(`\n🕷️  Starting crawl-all mode for: ${hotelName}`);
  console.log(`📍 URL: ${hotelUrl}`);
  console.log(`🆔 UUID: ${hotelUuid}`);
  console.log(`📏 Max depth: ${maxDepth}`);
  console.log(`⚙️  Concurrency: ${maxConcurrency}, Retries: ${maxRetries}, Timeout: ${timeout}s`);

  // Create crawler
  const crawler = new PlaywrightCrawler({
    maxConcurrency,
    maxRequestRetries: maxRetries,
    requestHandlerTimeoutSecs: timeout,
    
    launchContext: {
      launchOptions: { headless: true },
    },

    preNavigationHooks: [
      async ({ page, log }) => {
        // Block images, videos, and PDFs
        await page.route('**/*', (route) => {
          const request = route.request();
          const url = request.url();
          const resourceType = request.resourceType();
          const urlLower = url.toLowerCase();

          // Block by resource type
          if (resourceType === 'image' || resourceType === 'media') {
            route.abort();
            return;
          }

          // Block by file extension
          if (blockedExtensions.some(ext => urlLower.endsWith(ext))) {
            route.abort();
            return;
          }

          // Block by MIME type patterns
          if (['image/', 'video/', 'application/pdf', 'audio/'].some(mime => urlLower.includes(mime))) {
            route.abort();
            return;
          }

          route.continue();
        });
        log.debug('🚫 Resource blocking enabled');
      },
    ],

    async requestHandler({ page, request, enqueueLinks, log }) {
      const url = request.url;

      // Skip duplicates
      if (stats.scrapedUrls.has(url)) {
        stats.pagesSkipped++;
        log.debug(`⏭️  Skipping duplicate: ${url}`);
        return;
      }

      try {
        log.info(`📄 Scraping: ${url}`);

        // Wait for page to load
        await page.waitForLoadState('networkidle', { timeout: 30000 }).catch(() => {
          log.warning(`⚠️  Network idle timeout for ${url}, continuing...`);
        });
        await page.waitForLoadState('domcontentloaded').catch(() => {});
        await page.waitForSelector('a[href]', { timeout: 5000 }).catch(() => {
          log.debug(`⚠️  No links found on ${url}, continuing...`);
        });

        // Get HTML content
        const html = await page.content();
        if (!html || html.length === 0) {
          log.warning(`⚠️  Empty HTML for ${url}`);
          stats.errors++;
          return;
        }

        // Compute checksum and save
        const checksum = computeChecksum(html);
        await saveScrapedPage(hotelUuid, url, html, checksum);
        stats.scrapedUrls.add(url);
        stats.pagesScraped++;
        log.info(`✅ Saved page ${stats.pagesScraped}: ${url.substring(0, 80)}...`);

        // Discover and enqueue links
        const currentDepth = request.userData?.depth ?? 0;
        const links = await page.$$eval('a[href]', (anchors) => {
          return anchors
            .map(a => a.href)
            .filter(href => href && href.startsWith('http'))
            .filter((href, index, self) => self.indexOf(href) === index);
        });

        const baseUrl = new URL(url);
        const validLinks = links
          .filter(link => {
            try {
              return new URL(link).hostname === baseUrl.hostname;
            } catch {
              return false;
            }
          })
          .filter(link => {
            const linkLower = link.toLowerCase();
            if (blockedExtensions.some(ext => linkLower.endsWith(ext))) return false;
            if (currentDepth >= maxDepth) return false;
            return true;
          });

        if (validLinks.length > 0) {
          await enqueueLinks({
            urls: validLinks.map(linkUrl => ({
              url: linkUrl,
              userData: { depth: currentDepth + 1 },
            })),
          });
          log.debug(`🔗 Enqueued ${validLinks.length} links (depth: ${currentDepth + 1})`);
        }

      } catch (error) {
        stats.errors++;
        const errorMessage = error?.message || String(error) || 'Unknown error';
        log.error(`❌ Error processing ${url}: ${errorMessage}`);
      }
    },

    errorHandler({ request, log, error }) {
      stats.errors++;
      log.error(`❌ Failed to process ${request.url}:`, error?.message || String(error));
    },
  });

  // Start crawling
  try {
    await crawler.run([{
      url: hotelUrl,
      userData: { depth: 0 },
    }]);

    // Final statistics
    const finalStats = {
      hotelUuid,
      hotelName,
      hotelUrl,
      pagesScraped: stats.pagesScraped,
      pagesSkipped: stats.pagesSkipped,
      errors: stats.errors,
      totalPages: stats.pagesScraped + stats.pagesSkipped,
    };

    console.log(`\n📊 Crawl completed for ${hotelName}:`);
    console.log(`   ✅ Pages scraped: ${finalStats.pagesScraped}`);
    console.log(`   ⏭️  Pages skipped: ${finalStats.pagesSkipped}`);
    console.log(`   ❌ Errors: ${finalStats.errors}`);
    console.log(`   📦 Total: ${finalStats.totalPages} pages`);

    return finalStats;

  } catch (error) {
    console.error(`❌ Fatal error scraping ${hotelName}:`, error?.message || String(error));
    throw error;
  }
}


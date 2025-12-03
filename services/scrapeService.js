import { PlaywrightCrawler } from 'crawlee';
import crypto from 'crypto';
import { executeQuery } from '../config/database.js';

// Get table name from environment variable, default to 'hotel_pages'
const HOTEL_PAGES_TABLE = process.env.HOTEL_PAGES_TABLE || 'hotel_pages';

/**
 * Save scraped page to database
 * @param {string} hotelUuid - Hotel UUID
 * @param {string} url - Page URL
 * @param {string} html - Full HTML content
 * @param {string} checksum - SHA256 checksum of the HTML
 * @returns {Promise<number>} Insert ID
 */
async function saveScrapedPage(hotelUuid, url, html, checksum) {
  const query = `
    INSERT INTO ${HOTEL_PAGES_TABLE} (hotel_uuid, url, html_content, checksum, created_at)
    VALUES (?, ?, ?, ?, NOW())
    ON DUPLICATE KEY UPDATE
      html_content = VALUES(html_content),
      checksum = VALUES(checksum),
      updated_at = NOW()
  `;

  try {
    const result = await executeQuery(query, [hotelUuid, url, html, checksum]);
    return result.insertId || result.affectedRows;
  } catch (error) {
    // If table doesn't exist or ON DUPLICATE KEY UPDATE fails, try simple INSERT
    if (error.message.includes("doesn't exist") || error.message.includes("Duplicate")) {
      const simpleQuery = `
        INSERT INTO ${HOTEL_PAGES_TABLE} (hotel_uuid, url, html_content, checksum, created_at)
        VALUES (?, ?, ?, ?, NOW())
      `;
      const result = await executeQuery(simpleQuery, [hotelUuid, url, html, checksum]);
      return result.insertId || result.affectedRows;
    }
    throw error;
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
 * Recursively crawls all pages from the hotel's main URL
 * @param {string} hotelUrl - Starting URL for the hotel
 * @param {string} hotelUuid - Hotel UUID
 * @param {string} hotelName - Hotel name (for logging)
 * @returns {Promise<Object>} Scraping statistics
 */
export async function scrapeHotel(hotelUrl, hotelUuid, hotelName) {
  if (!hotelUrl || !hotelUrl.startsWith('http')) {
    throw new Error(`Invalid hotel URL: ${hotelUrl}`);
  }

  console.log(`\n🕷️  Starting scrape for: ${hotelName}`);
  console.log(`📍 URL: ${hotelUrl}`);
  console.log(`🆔 UUID: ${hotelUuid}`);

  let pagesScraped = 0;
  let pagesSkipped = 0;
  let errors = 0;
  const scrapedUrls = new Set();

  // Create PlaywrightCrawler instance
  const crawler = new PlaywrightCrawler({
    maxConcurrency: parseInt(process.env.CRAWLER_MAX_CONCURRENCY || '3', 10),
    maxRequestRetries: parseInt(process.env.CRAWLER_MAX_RETRIES || '2', 10),
    requestHandlerTimeoutSecs: parseInt(process.env.CRAWLER_TIMEOUT_SECS || '60', 10),
    
    // Launch options for Playwright
    launchContext: {
      launchOptions: {
        headless: true,
      },
    },

    async requestHandler({ page, request, enqueueLinks, log }) {
      const url = request.url;
      
      // Skip if already scraped (avoid duplicates)
      if (scrapedUrls.has(url)) {
        pagesSkipped++;
        log.debug(`⏭️  Skipping duplicate: ${url}`);
        return;
      }

      try {
        log.info(`📄 Scraping: ${url}`);

        // Wait for page to load (adjust selector if needed)
        await page.waitForLoadState('networkidle', { timeout: 30000 }).catch(() => {
          log.warning(`⚠️  Network idle timeout for ${url}, continuing anyway...`);
        });

        // Get full HTML content
        const html = await page.content();

        // Compute checksum
        const checksum = computeChecksum(html);

        // Save to database
        await saveScrapedPage(hotelUuid, url, html, checksum);

        scrapedUrls.add(url);
        pagesScraped++;
        
        log.info(`✅ Saved page ${pagesScraped}: ${url.substring(0, 80)}...`);

        // Auto-enqueue links from same domain
        await enqueueLinks({
          strategy: 'same-domain',
          selector: 'a[href]',
          label: 'hotel-page',
        });

      } catch (error) {
        errors++;
        log.error(`❌ Error scraping ${url}:`, error.message);
        // Continue with other pages even if one fails
      }
    },

    // Error handler
    errorHandler({ request, log, error }) {
      errors++;
      log.error(`❌ Failed to process ${request.url}:`, error.message);
    },
  });

  try {
    // Start crawling from the hotel's main URL
    await crawler.run([hotelUrl]);

    const stats = {
      hotelUuid,
      hotelName,
      hotelUrl,
      pagesScraped,
      pagesSkipped,
      errors,
      totalPages: pagesScraped + pagesSkipped,
    };

    console.log(`\n📊 Scraping completed for ${hotelName}:`);
    console.log(`   ✅ Pages scraped: ${pagesScraped}`);
    console.log(`   ⏭️  Pages skipped: ${pagesSkipped}`);
    console.log(`   ❌ Errors: ${errors}`);
    console.log(`   📦 Total: ${stats.totalPages} pages`);

    return stats;

  } catch (error) {
    console.error(`❌ Fatal error scraping ${hotelName}:`, error.message);
    throw error;
  }
}


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

  // Get max depth from environment variable (default: unlimited)
  // Depth 0 = starting page only, Depth 1 = starting page + direct links, etc.
  const maxDepth = process.env.CRAWLER_MAX_DEPTH 
    ? parseInt(process.env.CRAWLER_MAX_DEPTH, 10) 
    : 3; // Default max depth to 3.

  console.log(`\n🕷️  Starting scrape for: ${hotelName}`);
  console.log(`📍 URL: ${hotelUrl}`);
  console.log(`🆔 UUID: ${hotelUuid}`);
  if (maxDepth !== null) {
    console.log(`📏 Max depth: ${maxDepth}`);
  } else {
    console.log(`📏 Max depth: unlimited`);
  }

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

    // Pre-navigation hooks to block images, videos, and PDFs
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
          const blockedExtensions = [
            '.jpg', '.jpeg', '.png', '.gif', '.webp', '.svg', '.ico', '.bmp', // Images
            '.mp4', '.avi', '.mov', '.wmv', '.flv', '.webm', '.mkv', '.m4v', // Videos
            '.pdf', // PDFs
            '.mp3', '.wav', '.ogg', '.aac', '.flac', // Audio (optional)
          ];

          const hasBlockedExtension = blockedExtensions.some(ext => urlLower.endsWith(ext));
          if (hasBlockedExtension) {
            route.abort();
            return;
          }

          // Block by MIME type patterns in URL
          const blockedMimePatterns = [
            'image/', 'video/', 'application/pdf', 'audio/',
          ];

          const hasBlockedMime = blockedMimePatterns.some(mime => urlLower.includes(mime));
          if (hasBlockedMime) {
            route.abort();
            return;
          }

          // Allow all other requests
          route.continue();
        });

        log.debug('🚫 Resource blocking enabled: images, videos, and PDFs will be blocked');
      },
    ],

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

        // Step 1: Get full HTML content safely
        let html = '';
        try {
          html = await page.content();
          if (!html || html.length === 0) {
            log.warning(`⚠️  Empty HTML content for ${url}`);
            errors++;
            return;
          }
          log.debug(`📄 HTML length: ${html.length} bytes`);
        } catch (err) {
          errors++;
          log.error(`🔥 Error getting page content for ${url}:`, err?.message || String(err));
          return; // Can't continue without HTML
        }

        // Step 2: Compute checksum safely
        let checksum = 'ERROR';
        try {
          checksum = computeChecksum(html);
          if (!checksum || checksum === 'ERROR') {
            log.warning(`⚠️  Failed to compute checksum for ${url}`);
          }
        } catch (err) {
          log.error(`🔥 Error computing checksum for ${url}:`, err?.message || String(err));
          // Continue anyway with ERROR checksum
        }

        // Step 3: Save to database safely
        try {
          await saveScrapedPage(hotelUuid, url, html, checksum);
          scrapedUrls.add(url);
          pagesScraped++;
          log.info(`✅ Saved page ${pagesScraped}: ${url.substring(0, 80)}...`);
        } catch (err) {
          errors++;
          log.error(`🔥 Error saving to database for ${url}:`, err?.message || String(err));
          // Don't return here - still try to enqueue links even if save failed
        }

        // Step 4: Get current depth from request userData
        const currentDepth = request.userData?.depth ?? 0;
        
        // Log depth for debugging
        if (maxDepth !== null) {
          log.debug(`📍 Current depth: ${currentDepth}/${maxDepth}`);
        }

        // Step 5: Auto-enqueue links from same domain (excluding images, videos, PDFs)
        try {
          await enqueueLinks({
            strategy: 'same-domain',
            selector: 'a[href]',
            label: 'hotel-page',
            transformRequestFunction: ({ request: newRequest }) => {
              const linkUrl = newRequest.url.toLowerCase();
              
              // Skip images, videos, and PDFs
              const blockedExtensions = [
                '.jpg', '.jpeg', '.png', '.gif', '.webp', '.svg', '.ico', '.bmp',
                '.mp4', '.avi', '.mov', '.wmv', '.flv', '.webm', '.mkv', '.m4v',
                '.pdf',
                '.mp3', '.wav', '.ogg', '.aac', '.flac',
              ];

              const hasBlockedExtension = blockedExtensions.some(ext => linkUrl.endsWith(ext));
              if (hasBlockedExtension) {
                return false; // Don't enqueue this link
              }

              // Check depth limit
              if (maxDepth !== null && currentDepth >= maxDepth) {
                return false; // Don't enqueue if max depth reached
              }

              // Set depth for the new request
              newRequest.userData = { 
                ...newRequest.userData, 
                depth: currentDepth + 1 
              };
              return newRequest; // Enqueue this link
            },
          });
        } catch (err) {
          log.error(`🔥 Error enqueueing links for ${url}:`, err?.message || String(err));
          // Non-fatal - continue
        }

      } catch (error) {
        errors++;
        // Use safe error logging to avoid serialization issues
        const errorMessage = error?.message || String(error) || 'Unknown error';
        log.error(`❌ Handler failure for ${url}: ${errorMessage}`);
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
    // Start crawling from the hotel's main URL with depth 0
    await crawler.run([{
      url: hotelUrl,
      userData: { depth: 0 },
    }]);

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


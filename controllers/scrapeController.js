import { PlaywrightCrawler } from 'crawlee';
import crypto from 'crypto';
import TurndownService from 'turndown';
import { executeQuery } from '../config/database.js';

// Get table name from environment variable, default to 'hotel_page_data'
const HOTEL_PAGE_DATA_TABLE = process.env.HOTEL_PAGE_DATA_TABLE || 'hotel_page_data';

/* ⭐ Fully pinned Turndown configuration (NO defaults) */
const turndown = new TurndownService({
  headingStyle: 'atx',
  hr: '---',
  bulletListMarker: '-',
  codeBlockStyle: 'fenced',
  emDelimiter: '*',
  strongDelimiter: '**',
  linkStyle: 'inlined',
  linkReferenceStyle: 'full',
});
// Strip links (keep text, drop URLs)
// Keep link/button intent without including URLs (avoid checksum noise)
turndown.addRule('stripLinks', {
  filter: 'a',
  replacement: (content, node) => {
    const text = typeof content === 'string' ? content : '';
    if (!text.trim()) {
      return '';
    }
    const cls = (node?.className || '').toLowerCase();
    const role = (node?.getAttribute ? node.getAttribute('role') : '')?.toLowerCase() || '';
    const isButton = role === 'button' || cls.includes('button') || cls.includes('btn');
    const tag = isButton ? 'button' : 'link';
    return `${content} [${tag}]`;
  },
});

// Drop images entirely
// Image source URLs might confuse checksum calculation - different checksums for the same content
turndown.addRule('dropImages', {
  filter: 'img',
  replacement: () => '',
});

// Convert button tags to markdown format: content [button]
turndown.addRule('stripButtons', {
  filter: 'button',
  replacement: (content) => {
    const text = typeof content === 'string' ? content : '';
    if (!text.trim()) {
      return '';
    }
    return `${content} [button]`;
  },
});

/**
 * Save scraped page to database
 * @param {string} hotelUuid - Hotel UUID
 * @param {string} url - Page URL
 * @param {string} html - Cleaned HTML content
 * @param {string|null} htmlRaw - raw HTML content (before cleaning)
 * @param {string} markdown - Markdown converted from cleaned HTML
 * @param {string} checksum - SHA256 checksum of the HTML
 * @param {number} depth - Crawl depth of the page
 * @param {number|null} pageId - Existing page id (if known)
 * @returns {Promise<number>} Insert ID or affected rows
 */
async function saveScrapedPage(hotelUuid, url, html, htmlRaw, markdown, checksum, depth, pageId = null) {
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
  if (htmlRaw !== null && typeof htmlRaw !== 'string') {
    throw new Error(`Invalid htmlRaw: ${typeof htmlRaw}`);
  }
  if (!checksum || typeof checksum !== 'string') {
    throw new Error(`Invalid checksum: ${typeof checksum}`);
  }
  if (!markdown || typeof markdown !== 'string') {
    throw new Error(`Invalid markdown: ${typeof markdown}`);
  }
  if (!Number.isInteger(depth) || depth < 0) {
    depth = 9999; // Fallback for debugging invalid depth
  }

  // Resolve target id if not provided
  let targetId = pageId;
  let existingChecksum = null;
  let isChecksumUpdated = 0; // This is only for update (of page) use case. We **do not** treat checksum-updated in **new (insert) page** use case.
  if (!targetId) {
    const checkQuery = `
      SELECT id, checksum FROM ${HOTEL_PAGE_DATA_TABLE}
      WHERE hotel_uuid = ? AND page_url = ?
      LIMIT 1
    `;
    const found = await executeQuery(checkQuery, [hotelUuid, url]);
    if (found && found.length > 0) {
      targetId = found[0].id;
      existingChecksum = found[0].checksum;
    }
  }

  if (targetId && existingChecksum !== null && existingChecksum !== checksum) {
    isChecksumUpdated = 1;
    if (process.env.NODE_ENV === 'development') {
      console.log(`⚠️  checksum updated for ${url}: ${existingChecksum} !== ${checksum}`);
    }
  }

  try {
    if (targetId) {
      // Update existing record
      const updateQuery = `
        UPDATE ${HOTEL_PAGE_DATA_TABLE}
        SET html_prev = html,
            markdown_prev = markdown,
            html = ?,
            html_raw = ?,
            markdown = ?,
            checksum = ?,
            is_checksum_updated = ?,
            depth = ?,
            updated_at = CURRENT_TIMESTAMP,
            active = 1
        WHERE id = ?
      `;
      const result = await executeQuery(updateQuery, [html, htmlRaw, markdown, checksum, isChecksumUpdated, depth, targetId]);
      return result.affectedRows;
    } else {
      // Insert new record
      const insertQuery = `
        INSERT INTO ${HOTEL_PAGE_DATA_TABLE} (hotel_uuid, page_url, checksum, html, html_raw, markdown, depth, active)
        VALUES (?, ?, ?, ?, ?, ?, ?, 1)
      `;
      const result = await executeQuery(insertQuery, [hotelUuid, url, checksum, html, htmlRaw, markdown, depth]);
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
 * Fetch existing pages for a hotel (id, url, checksum).
 * Note: active = 0 indicates the page was not scraped in the last run; it is not a deletion flag.
 */
async function getExistingPages(hotelUuid) {
  const query = `
    SELECT id, page_url, checksum
    FROM ${HOTEL_PAGE_DATA_TABLE}
    WHERE hotel_uuid = ?
  `;
  try {
    return await executeQuery(query, [hotelUuid]);
  } catch (error) {
    console.error('❌ Error fetching existing pages:', error.message);
    return [];
  }
}

/**
 * Deactivate pages that were not scraped in the current scrapeHotel execution.
 * Note: Sets active = 0 to indicate the page was not scraped in the last run. This does not mean the page is deleted.
 *
 * @param {Array<number>} pageIds - Array of page ids to deactivate
 * @returns {Promise<number>} Number of affected rows
 */
async function deactivatePagesByIds(pageIds = []) {
  if (pageIds.length === 0) {
    return 0;
  }

  const placeholders = pageIds.map(() => '?').join(', ');
  const query = `
    UPDATE ${HOTEL_PAGE_DATA_TABLE}
    SET active = 0, updated_at = CURRENT_TIMESTAMP
    WHERE id IN (${placeholders})
  `;

  try {
    const result = await executeQuery(query, pageIds);
    return result.affectedRows || 0;
  } catch (error) {
    console.error('❌ Error deactivating old pages:', error.message);
    return 0;
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
    .update(content.normalize('NFC'), 'utf8')
    .digest('hex');
}

/**
 * Normalize markdown to a deterministic form for hashing/storage.
 * @param {string} markdown
 * @returns {string}
 */
function normalizeMarkdown(markdown) {
  return (markdown || '')
    .replace(/\r\n/g, '\n')
    .trim();
}

/**
 * Wait until the DOM has stopped changing for `quietMs`,
 * but never wait longer than `timeoutMs`.
 *
 * This is NOT a fixed sleep.
 * It continuously checks DOM convergence and exits early if stable.
 * Uses a local checksum (djb2 over DOM snapshot) for stability only;
 * does not use or affect computeChecksum.
 */
async function waitForDomToSettle(page, {
  quietMs = 800,               // how long DOM must remain unchanged
  timeoutMs = 15000,           // hard upper bound
  minSigIntervalMs = 250,      // minimum interval between signature calculations
} = {}) {
  await page.waitForFunction(
    ({ quietMs, minSigIntervalMs }) => {
      const now = Date.now();
      const root = document.body;
      if (!root) return false;

      // Ensure state exists
      if (!window.__domStability) {
        window.__domStability = {
          signature: '',
          lastChange: now,
          lastSigAt: 0,
        };
      }

      // ❌ Too soon to recompute signature → only check quiet window
      if ((now - window.__domStability.lastSigAt) < minSigIntervalMs) {
        return (now - window.__domStability.lastChange) >= quietMs;
      }

      // ✅ Allowed to recompute signature
      window.__domStability.lastSigAt = now;

      // Own checksum for DOM stability only (not computeChecksum).
      // djb2 over text only; deterministic, browser-only.
      function domStabilityHash(str) {
        let h = 5381;
        for (let i = 0; i < str.length; i++) {
          h = ((h << 5) + h) + str.charCodeAt(i);
        }
        return h >>> 0;
      }

      const text = (root.innerText || '').replace(/\s+/g, ' ').trim();
      const elCount = root.getElementsByTagName('*').length;
      const checksum = domStabilityHash(text);
      const signature = `${elCount}|${text.length}|${checksum}`;

      // Signature changed → reset quiet window
      if (signature !== window.__domStability.signature) {
        window.__domStability.signature = signature;
        window.__domStability.lastChange = now;
        return false;
      }

      // Signature unchanged → check quiet window
      return (now - window.__domStability.lastChange) >= quietMs;
    },
    { quietMs, minSigIntervalMs }, // args
    { timeout: timeoutMs } // options
  )
  .catch(() => {});
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
  const existingPages = await getExistingPages(hotelUuid);
  const nonScrapedPageMap = new Map((existingPages || []).map((page) => [page.page_url, page.id]));
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

        // Bridge page console logs to Node for debug visibility
        page.on('console', (msg) => log.info(`[page:${msg.type()}] ${msg.text()}`));

        // BEGIN LAZY_SCROLL_CONTENT_LOADING
        // Attempt to load lazy/scroll-triggered content
        try {
          await page.evaluate(async () => {
            const delay = (ms) => new Promise(res => setTimeout(res, ms));
            let lastHeight = document.body.scrollHeight;
            for (let i = 0; i < 25; i++) {
              window.scrollTo(0, document.body.scrollHeight);
              await delay(1500);
              const newHeight = document.body.scrollHeight;
              if (newHeight === lastHeight) break;
              lastHeight = newHeight;
              // console.log(`Scrolled to ${newHeight} at ${i+1} of 5`);
            }
            window.scrollTo(0, 0);
          });
        } catch (error) {
          // Ignore scroll failures; continue scraping
        }
        // END LAZY_SCROLL_CONTENT_LOADING

        // BEGIN WAIT_FOR_ASYNC_CONTENT_TO_SETTLE
        // Allow hero/above-the-fold async content to settle on root page
        // await page.waitForFunction(
        //   () => false,
        //   { timeout: currentDepth === 0 ? 10000 : 6000 } // Home page might have more async content to settle
        // ).catch(() => {});        
        const settleStartMs = Date.now();
        await waitForDomToSettle(page, {
          quietMs: currentDepth === 0 ? 6000 : 4000,
          timeoutMs: currentDepth === 0 ? 12000 : 8000,
          minSigIntervalMs: 400,
        });
        if (process.env.NODE_ENV === 'development') {
          const waitedSecs = (Date.now() - settleStartMs) / 1000;
          log.info(`⏳ DOM settle waited ${waitedSecs.toFixed(2)}s: ${pageUrl}`);
        }
        // END WAIT_FOR_ASYNC_CONTENT_TO_SETTLE

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

        // BEGIN CLEAN_PAGE_DOM_FOR_MARKDWON_CONVERSION_FRIENDLY
        // Capture raw HTML before cleaning (for debugging)
        const htmlRaw = await page.content();
        
        // BEGIN GET_RAW_LINKS_IN_PAGE
        // Capture raw links before DOM mutations (for enqueue after save), skipping obvious ads
        const rawLinks = await page.evaluate(() => {
          const isAd = (el) => {
            const id = (el.id || '').toLowerCase();
            const cls = (el.className || '').toLowerCase();
            if (id.includes('ad') || id.includes('ads') || id.includes('advertisement')) return true;
            if (cls.includes(' ad') || cls.includes('ads') || cls.includes('advertisement')) return true;
            if (el.closest("[id*='ad'], .ad, .ads, .advertisement")) return true;
            return false;
          };
          const blockedHosts = ['google.com', 'bing.com', 'yahoo.com'];
          return Array.from(document.querySelectorAll('a[href]'))
            .filter((a) => !isAd(a))
            .map((a) => a.getAttribute('href') || '')
            .filter(Boolean)
            .filter((href) => {
              try {
                const url = new URL(href, location.href);
                return !blockedHosts.some((host) => url.hostname.includes(host));
              } catch {
                return true;
              }
            });
        }).catch(() => []);
        // END GET_RAW_LINKS_IN_PAGE

        /* ⭐ Deterministic DOM cleanup */
        const bodyHtml = await page.evaluate((currentDepth) => {
          const root = document.body || document.documentElement;
          if (!root) return '';

          // Remove unstable elements
          document.querySelectorAll('script, style, noscript, iframe, frame').forEach(e => e.remove());
          document.querySelectorAll("[id*='ad'], .ad, .ads, .advertisement").forEach(e => e.remove());
          document.querySelectorAll('svg, figure').forEach(e => e.remove());

          // Remove Here Map tags (https://www.here.com/)
          document.querySelectorAll('.H_imprint [class^="H_"], .H_ui [class^="H_"]').forEach(e => e.remove());

          // Strip all inline styles for consistency
          document.querySelectorAll('[style]').forEach(el => el.removeAttribute('style'));

          // Remove navigational chrome for markdown friendliness (skip on depth 0)
          if (currentDepth > 0) {
            document.querySelectorAll(
              'nav, header, footer, breadcrumb, [class*="nav"], [id*="nav"], [role*="nav"], [class*="header"], [id*="header"], [role*="header"], [class*="footer"], [id*="footer"], [role*="footer"], [class*="breadcrumb"], [id*="breadcrumb"], [role*="breadcrumb"]'
            ).forEach(el => el.remove());
          }

          // Remove common reCAPTCHA containers
          document.querySelectorAll(
            '.g-recaptcha, .recaptcha, .grecaptcha, [class*="recaptcha"], [id*="recaptcha"], [data-sitekey]'
          ).forEach(el => el.remove());

          // Resolve relative URLs deterministically
          const toAbsolute = (url) => {
            try {
              return new URL(url, location.href).href;
            } catch {
              return url;
            }
          };

          document.querySelectorAll('a[href]').forEach(a => {
            const href = a.getAttribute('href');
            if (href) a.setAttribute('href', toAbsolute(href));
          });

          document.querySelectorAll('img[src]').forEach(img => {
            const src = img.getAttribute('src');
            if (src) img.setAttribute('src', toAbsolute(src));
          });

          // Remove only structurally empty containers
          document.querySelectorAll('p, div, span').forEach(el => {
            if (
              el.children.length === 0 &&
              el.textContent.replace(/\s+/g, '').length === 0
            ) {
              el.remove();
            }
          });

          // Normalize adjacent text nodes for deterministic Turndown output
          (function normalizeTextNodes(node) {
            // Safety guards
            if (!node || node.nodeType !== Node.ELEMENT_NODE) return;
          
            // Do not normalize inside code blocks (preserve formatting)
            const tag = node.tagName && node.tagName.toLowerCase();
            if (tag === 'pre' || tag === 'code') return;
          
            let prevTextNode = null;
          
            node.childNodes.forEach(child => {
              if (child.nodeType === Node.TEXT_NODE) {
                if (prevTextNode) {
                  // Merge adjacent text nodes
                  prevTextNode.textContent += child.textContent;
                  child.remove();
                } else {
                  prevTextNode = child;
                }
              } else {
                // Reset when encountering a non-text node
                prevTextNode = null;
                normalizeTextNodes(child);
              }
            });
          })(root);

          return root.innerHTML || '';
        }, currentDepth); // Pass currentDepth to the evaluate function
        // END CLEAN_PAGE_DOM_FOR_MARKDWON_CONVERSION_FRIENDLY

        // remove whitespace between tags
        let html = bodyHtml ? bodyHtml.replace(/>\s+</g, '><').trim() : '';
        
        if (!html || html.length === 0 || html.trim().length === 0) {
          log.warning(`⚠️  Empty HTML: ${pageUrl}`);
          stats.errors += 1;
          return;
        }

        // ⭐ Deterministic Markdown + checksum
        const markdownRaw = turndown.turndown(html);
        const markdown = normalizeMarkdown(markdownRaw);
        const checksum = computeChecksum(markdown);
        await saveScrapedPage(hotelUuid, pageUrl, html, htmlRaw, markdown, checksum, currentDepth);
        visited.add(pageUrl);
        if (nonScrapedPageMap.has(pageUrl)) {
          nonScrapedPageMap.delete(pageUrl);
        }
        stats.scraped += 1;
        log.info(`✅ Saved: ${pageUrl}`);

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

  // Deactivate pages that were present before but not scraped in this run
  const stalePageIds = Array.from(nonScrapedPageMap.values());
  if (stalePageIds.length > 0) {
    const deactivated = await deactivatePagesByIds(stalePageIds);
    console.log(`🗂️  Deactivated ${deactivated} outdated page(s) for ${hotelName}`);
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




function parseHttpUrl(value) {
  const url = new URL(value);
  if (!['http:', 'https:'].includes(url.protocol) || url.username || url.password) {
    throw new Error('Crawl URLs must be HTTP/HTTPS URLs without credentials');
  }
  url.hash = '';
  return url;
}

function pageKey(url) {
  // Treat HTTP/HTTPS and a trailing slash as equivalent, retaining query parameters.
  return `${url.host}${url.pathname.replace(/\/$/, '')}${url.search}`;
}

/**
 * Build a fixed crawl boundary around the hotel's URL.
 * In all modes, additional URLs allow discovered pages outside the normal
 * boundary. They are not starting URLs and do not allow their host or descendants.
 */
export function createCrawlScope(hotelUrl, { crawlScope = 'site', additionalUrls = [] } = {}) {
  if (!['site', 'subtree', 'page'].includes(crawlScope)) {
    throw new Error(`Invalid crawl scope: ${crawlScope}`);
  }
  if (!Array.isArray(additionalUrls)) throw new Error('additionalUrls must be an array');

  const root = parseHttpUrl(hotelUrl);
  const rootKey = pageKey(root);
  const exceptions = new Set(additionalUrls.map((value) => {
    const url = parseHttpUrl(value);
    if (url.host !== root.host) {
      throw new Error(`Additional URL must use the hotel URL host: ${value}`);
    }
    return pageKey(url);
  }));
  const rootPath = root.pathname.replace(/\/$/, '');

  return {
    startUrls: [root.href],
    isAllowed(value) {
      try {
        const url = parseHttpUrl(value);
        const key = pageKey(url);
        if (key === rootKey || exceptions.has(key)) return true;
        if (url.host !== root.host) return false;
        if (crawlScope === 'site') return true;
        if (crawlScope === 'subtree') {
          return url.pathname === rootPath || url.pathname.startsWith(`${rootPath}/`);
        }
        return false;
      } catch {
        return false;
      }
    },
  };
}

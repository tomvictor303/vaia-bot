import assert from 'node:assert/strict';
import test from 'node:test';
import { createCrawlScope } from '../utils/crawlScope.js';

const hotelUrl = 'https://hotels.example/ambassador';

test('site remains the default and stays on the configured host', () => {
  const scope = createCrawlScope(hotelUrl);
  assert.equal(scope.isAllowed('https://hotels.example/rooms'), true);
  assert.equal(scope.isAllowed('https://booking.hotels.example/rooms'), false);
  assert.equal(scope.isAllowed('https://hotels.example.evil.test/rooms'), false);
  assert.equal(scope.isAllowed('https://hotels.example:8443/rooms'), false);
});

test('subtree accepts descendants but excludes sibling hotels and parent paths', () => {
  const scope = createCrawlScope(`${hotelUrl}/`, { crawlScope: 'subtree' });
  for (const suffix of ['', '/', '/rooms', '/rooms?view=all']) {
    assert.equal(scope.isAllowed(`${hotelUrl}${suffix}`), true, suffix);
  }
  for (const suffix of ['-other', '/../another-hotel', '/%2e%2e/another-hotel']) {
    assert.equal(scope.isAllowed(`${hotelUrl}${suffix}`), false, suffix);
  }
  assert.equal(scope.isAllowed('https://hotels.example/'), false);
});

test('subtree rooted at / allows paths on that host', () => {
  const scope = createCrawlScope('https://hotels.example/', { crawlScope: 'subtree' });
  assert.equal(scope.isAllowed('https://hotels.example/rooms'), true);
});

test('page permits trailing slashes, fragments and HTTPS upgrades, preserving queries', () => {
  const scope = createCrawlScope('http://hotels.example/hotel?id=12', { crawlScope: 'page' });
  assert.equal(scope.isAllowed('https://hotels.example/hotel/?id=12#rooms'), true);
  assert.equal(scope.isAllowed('https://hotels.example/hotel?id=13'), false);
  assert.equal(scope.isAllowed('https://hotels.example/hotel'), false);
  assert.equal(scope.isAllowed('https://hotels.example/hotel/rooms?id=12'), false);
});

test('additional pages do not expand the scope to their siblings or descendants', () => {
  for (const crawlScope of ['site', 'subtree', 'page']) {
    const scope = createCrawlScope(hotelUrl, {
      crawlScope,
      additionalUrls: ['https://hotels.example/ambassador-history?view=full'],
    });
    assert.equal(scope.isAllowed('https://hotels.example/ambassador-history/?view=full'), true);
    assert.equal(scope.isAllowed('https://hotels.example/ambassador-history/rooms?view=full'), crawlScope === 'site');
    assert.equal(scope.isAllowed('https://hotels.example/ambassador-history?view=short'), crawlScope === 'site');
  }
});

test('only the main URL seeds the crawl, even with additional URLs configured', () => {
  for (const crawlScope of ['site', 'subtree', 'page']) {
    const scope = createCrawlScope(`${hotelUrl}#rooms`, {
      crawlScope,
      additionalUrls: [`${hotelUrl}/#rooms`, `${hotelUrl}?id=1`, `${hotelUrl}?id=2`],
    });
    assert.deepEqual(scope.startUrls, [hotelUrl]);
  }
});

test('site mode accepts additional URLs without seeding them', () => {
  const scope = createCrawlScope(hotelUrl, {
    crawlScope: 'site',
    additionalUrls: ['https://hotels.example/history'],
  });
  assert.deepEqual(scope.startUrls, [hotelUrl]);
  assert.equal(scope.isAllowed('https://hotels.example/history'), true);
});

test('out-of-scope redirects cannot broaden a page scope', () => {
  const scope = createCrawlScope(hotelUrl, { crawlScope: 'page' });
  assert.equal(scope.isAllowed('https://hotels.example/'), false);
  assert.equal(scope.isAllowed('https://www.hotels.example/ambassador'), false);
  assert.equal(scope.isAllowed('https://hotels.example/other-hotel'), false);
});

test('invalid configuration fails and non-web links are excluded', () => {
  assert.throws(() => createCrawlScope(hotelUrl, { crawlScope: 'unknown' }));
  assert.throws(() => createCrawlScope(hotelUrl, { additionalUrls: 'not an array' }));
  assert.throws(() => createCrawlScope(hotelUrl, { crawlScope: 'page', additionalUrls: ['invalid'] }));
  assert.throws(() => createCrawlScope(hotelUrl, {
    additionalUrls: ['https://another.example/history'],
  }));
  const scope = createCrawlScope(hotelUrl);
  for (const url of ['mailto:hotel@example.com', 'javascript:void(0)', 'ftp://hotels.example/',
    'https://user:password@hotels.example/', 'invalid']) {
    assert.equal(scope.isAllowed(url), false, url);
  }
});

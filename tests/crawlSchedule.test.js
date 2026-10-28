import test from 'node:test';
import assert from 'node:assert/strict';
import { isCrawlDue, parseRunOptions } from '../utils/crawlSchedule.js';

function localDate(year, month, day) {
  return new Date(year, month - 1, day, 12, 0, 0);
}

test('daily is due every day', () => {
  assert.equal(isCrawlDue('daily', localDate(2026, 10, 28)), true);
});

test('weekly is due on Sunday', () => {
  assert.equal(isCrawlDue('weekly', localDate(2026, 11, 1)), true);
  assert.equal(isCrawlDue('weekly', localDate(2026, 11, 2)), false);
});

test('every_2_weeks uses January 4, 1970 as its Sunday anchor', () => {
  assert.equal(isCrawlDue('every_2_weeks', localDate(1970, 1, 4)), true);
  assert.equal(isCrawlDue('every_2_weeks', localDate(1970, 1, 11)), false);
  assert.equal(isCrawlDue('every_2_weeks', localDate(1970, 1, 18)), true);
});

test('monthly is due on the first day of the month', () => {
  assert.equal(isCrawlDue('monthly', localDate(2026, 10, 1)), true);
  assert.equal(isCrawlDue('monthly', localDate(2026, 10, 2)), false);
  assert.equal(isCrawlDue(null, localDate(2026, 10, 1)), true);
});

test('every_2_months is due on the first day of odd-numbered months', () => {
  assert.equal(isCrawlDue('every_2_months', localDate(2026, 1, 1)), true);
  assert.equal(isCrawlDue('every_2_months', localDate(2026, 2, 1)), false);
  assert.equal(isCrawlDue('every_2_months', localDate(2026, 3, 1)), true);
});

test('parseRunOptions supports scheduled, all, and selected-hotel modes', () => {
  assert.deepEqual(parseRunOptions([]), { mode: 'scheduled', hotelUuid: null });
  assert.deepEqual(parseRunOptions(['--all']), { mode: 'all', hotelUuid: null });
  assert.deepEqual(parseRunOptions(['--hotel', 'hotel-123']), { mode: 'hotel', hotelUuid: 'hotel-123' });
  assert.deepEqual(parseRunOptions(['--hotel=hotel-123']), { mode: 'hotel', hotelUuid: 'hotel-123' });
});

test('parseRunOptions rejects conflicting or incomplete options', () => {
  assert.throws(() => parseRunOptions(['--all', '--hotel', 'hotel-123']), /cannot be used together/);
  assert.throws(() => parseRunOptions(['--hotel']), /requires a hotel UUID/);
  assert.throws(() => parseRunOptions(['--unknown']), /Unknown command-line option/);
});

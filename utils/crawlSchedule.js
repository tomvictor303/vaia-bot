const DAY_MS = 24 * 60 * 60 * 1000;
const EVERY_TWO_WEEKS_ANCHOR_UTC = Date.UTC(1970, 0, 4);

export const CRAWL_CYCLES = Object.freeze([
  'daily',
  'weekly',
  'every_2_weeks',
  'monthly',
  'every_2_months',
]);

/**
 * Check whether a crawl cycle is due on a calendar date.
 * Calendar fields use the server's local timezone. A missing cycle falls back
 * to the database default, "monthly".
 *
 * @param {string} crawlCycle
 * @param {Date} [date=new Date()]
 * @param {number} [crawlOffsetDays=0] Signed days from the cycle anchor.
 * @returns {boolean}
 */
export function isCrawlDue(crawlCycle, date = new Date(), crawlOffsetDays = 0) {
  if (!(date instanceof Date) || Number.isNaN(date.getTime())) {
    throw new Error('A valid date is required to evaluate the crawl cycle');
  }

  const offsetDays = Number(crawlOffsetDays ?? 0);
  if (!Number.isInteger(offsetDays)) {
    throw new Error(`Invalid crawl offset days: ${crawlOffsetDays}`);
  }

  const cycle = crawlCycle || 'monthly';
  if (!CRAWL_CYCLES.includes(cycle)) {
    throw new Error(`Unsupported crawl cycle: ${cycle}`);
  }

  // Invert the offset to find the cycle anchor that would produce this run date.
  // UTC arithmetic is used only for calendar math, avoiding DST-length days.
  const anchorCandidate = new Date(Date.UTC(
    date.getFullYear(),
    date.getMonth(),
    date.getDate() - offsetDays
  ));
  const year = anchorCandidate.getUTCFullYear();
  const month = anchorCandidate.getUTCMonth();
  const dayOfMonth = anchorCandidate.getUTCDate();
  const dayOfWeek = anchorCandidate.getUTCDay();

  switch (cycle) {
    case 'daily':
      return true;
    case 'weekly':
      return dayOfWeek === 0;
    case 'every_2_weeks': {
      if (dayOfWeek !== 0) return false;
      const currentDateUtc = Date.UTC(year, month, dayOfMonth);
      const daysSinceAnchor = Math.floor((currentDateUtc - EVERY_TWO_WEEKS_ANCHOR_UTC) / DAY_MS);
      return daysSinceAnchor % 14 === 0;
    }
    case 'monthly':
      return dayOfMonth === 1;
    case 'every_2_months':
      return dayOfMonth === 1 && month % 2 === 0;
    default:
      return false;
  }
}

/**
 * Parse supported command-line run modes.
 *
 * @param {string[]} args Arguments after the script name.
 * @returns {{ mode: 'scheduled'|'all'|'hotel', hotelUuid: string|null }}
 */
export function parseRunOptions(args = []) {
  let runAll = false;
  let hotelUuid = null;

  for (let i = 0; i < args.length; i += 1) {
    const arg = args[i];

    if (arg === '--all') {
      runAll = true;
      continue;
    }

    if (arg === '--hotel') {
      const value = args[i + 1];
      if (!value || value.startsWith('--')) {
        throw new Error('--hotel requires a hotel UUID');
      }
      hotelUuid = value;
      i += 1;
      continue;
    }

    if (arg.startsWith('--hotel=')) {
      hotelUuid = arg.slice('--hotel='.length);
      if (!hotelUuid) {
        throw new Error('--hotel requires a hotel UUID');
      }
      continue;
    }

    throw new Error(`Unknown command-line option: ${arg}`);
  }

  if (runAll && hotelUuid) {
    throw new Error('--all and --hotel cannot be used together');
  }

  if (hotelUuid) return { mode: 'hotel', hotelUuid };
  if (runAll) return { mode: 'all', hotelUuid: null };
  return { mode: 'scheduled', hotelUuid: null };
}

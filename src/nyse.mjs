/**
 * @file NYSE trading-day calendar — Phase A-4 pre-flight gate.
 *
 * The Gemini Enterprise Deep Research morning brief is scheduled for
 * trading days only: there is no point paying the Deep Research Agent to
 * re-read an idle market on Saturdays, Sundays, or US federal holidays
 * that NYSE observes, and the downstream `magi_core.market_research`
 * rows are consumed by the PLM (8 LLM Jobs) which itself only runs
 * during NYSE sessions.
 *
 * This module is the single pre-flight check the Cloud Run Job will
 * call before invoking `generateDeepBrief()` (Phase B) or the Deep
 * Research Agent (Phase C). If it returns `false`, the job exits
 * cleanly — see design §3.1, §4.2, and §5.4.
 *
 * All public functions accept a JavaScript `Date` and evaluate it in
 * the **America/New_York** timezone regardless of the host machine's
 * local TZ. The Cloud Run runtime is UTC, Devin's dev machine is UTC,
 * and Jun's laptop is JST; we never want any of those to silently shift
 * a trading-day decision.
 *
 * Holiday data is delegated to the MIT-licensed `nyse-holidays` package
 * (a thin, dependency-light wrapper around dayjs that implements NYSE's
 * official holiday schedule including the Saturday → Friday / Sunday →
 * Monday observation rules). We deliberately do not maintain our own
 * holiday table — NYSE occasionally adds ad-hoc closures (e.g. a
 * presidential funeral) and the upstream package is easier to refresh
 * via `npm update` than a hand-rolled date list.
 *
 * Half-day sessions (the day after Thanksgiving, Christmas Eve on
 * certain years) are intentionally treated as full trading days: NYSE
 * is open, just with a 1pm ET close, and the Deep Research brief is
 * written *before* market open regardless.
 */

import { getHolidays } from 'nyse-holidays';

/**
 * Formatter that renders a `Date` as `YYYY-MM-DD` in America/New_York.
 * Reused across calls to avoid the per-call `Intl` object construction
 * cost (~1ms each) on the hot path — the Cloud Run Job will call
 * `isTradingDay()` hundreds of times when walking forward to find the
 * next open session after a long weekend.
 */
const ET_DATE_FMT = new Intl.DateTimeFormat('en-CA', {
  timeZone: 'America/New_York',
  year: 'numeric',
  month: '2-digit',
  day: '2-digit',
});

/**
 * Formatter that returns the ET weekday short name (`Sun`..`Sat`).
 * We deliberately avoid `Date.prototype.getUTCDay()` / `getDay()` which
 * would give the UTC or host-local weekday — wrong on either side of
 * the ET day boundary.
 */
const ET_WEEKDAY_FMT = new Intl.DateTimeFormat('en-US', {
  timeZone: 'America/New_York',
  weekday: 'short',
});

const WEEKDAY_INDEX = Object.freeze({
  Sun: 0,
  Mon: 1,
  Tue: 2,
  Wed: 3,
  Thu: 4,
  Fri: 5,
  Sat: 6,
});

/**
 * Assert that `date` is a finite `Date`. Surfaces a clear error early
 * rather than letting `Invalid Date` propagate through the formatters.
 *
 * @param {unknown} date
 * @param {string}  fn - Function name, used in the error message.
 * @returns {Date}
 */
function assertDate(date, fn) {
  if (!(date instanceof Date) || Number.isNaN(date.getTime())) {
    throw new TypeError(`${fn}: date must be a valid Date`);
  }
  return date;
}

/**
 * ET calendar date of `date` in `YYYY-MM-DD` form.
 *
 * @param {Date} date
 * @returns {string}
 */
export function etDateString(date) {
  assertDate(date, 'etDateString');
  return ET_DATE_FMT.format(date);
}

/**
 * ET weekday of `date` as `0..6` (Sunday through Saturday), matching
 * the convention of `Date.prototype.getDay()`.
 *
 * @param {Date} date
 * @returns {number}
 */
export function etWeekday(date) {
  assertDate(date, 'etWeekday');
  const name = ET_WEEKDAY_FMT.format(date);
  const idx = WEEKDAY_INDEX[name];
  // Defense-in-depth: future Node versions or locale data drift could
  // change the output. We'd rather fail loudly than silently mis-route
  // a holiday check.
  if (idx === undefined) {
    throw new Error(`etWeekday: unexpected Intl weekday output ${JSON.stringify(name)}`);
  }
  return idx;
}

/**
 * `true` iff the ET calendar date of `date` is a Saturday or Sunday.
 *
 * @param {Date} date
 * @returns {boolean}
 */
export function isWeekend(date) {
  const wd = etWeekday(date);
  return wd === 0 || wd === 6;
}

/**
 * Return the NYSE holiday name observed on the ET calendar date of
 * `date`, or `null` if no holiday is observed.
 *
 * This respects observation rules (e.g. July 4 2026 falls on a
 * Saturday, so NYSE observes it on Friday July 3; `getHolidayName(new
 * Date(Date.UTC(2026, 6, 3, 17, 0)))` returns `"Independence Day"`).
 *
 * @param {Date} date
 * @returns {string | null}
 */
export function getHolidayName(date) {
  assertDate(date, 'getHolidayName');
  const ds = etDateString(date);
  const year = Number(ds.slice(0, 4));
  // `nyse-holidays` constructs holiday dates at local midnight and
  // formats them via dayjs's `YYYY-MM-DD`, which renders the calendar
  // date regardless of host TZ (see upstream `getDate` util). So
  // `holiday.dateString` is always the NYSE calendar date and can be
  // compared directly to `etDateString(date)`.
  const match = getHolidays(year).find((h) => h.dateString === ds);
  return match ? match.name : null;
}

/**
 * `true` iff `date` falls on an NYSE-observed holiday (weekends not
 * counted — use `isTradingDay()` for the full check).
 *
 * @param {Date} date
 * @returns {boolean}
 */
export function isHoliday(date) {
  return getHolidayName(date) !== null;
}

/**
 * `true` iff the NYSE is scheduled to be open on the ET calendar date
 * of `date`. This is the canonical pre-flight gate for the Deep
 * Research morning brief.
 *
 * Does not model mid-day early closes (half-day sessions): those are
 * still trading days and the morning brief runs regardless.
 *
 * @param {Date} date
 * @returns {boolean}
 */
export function isTradingDay(date) {
  if (isWeekend(date)) return false;
  if (isHoliday(date)) return false;
  return true;
}

/**
 * Find the next NYSE trading day strictly *after* `date`. Useful for
 * the "market closed, next session YYYY-MM-DD" log line the Cloud Run
 * Job emits when it exits early.
 *
 * Walks forward one ET calendar day at a time by adding 24h and
 * re-evaluating in ET. 24h steps tolerate DST transitions because the
 * output only depends on the ET *date*, not the ET clock time — any
 * 24h step lands on a date at least one ET day later (usually exactly
 * one, at worst one-plus-one-hour during spring-forward or one-minus-
 * one-hour during fall-back). The loop caps at `MAX_LOOKAHEAD_DAYS` so
 * a pathological holiday table cannot produce an infinite loop.
 *
 * @param {Date} date
 * @returns {Date} A new `Date` pointing at 12:00 UTC on the next
 *   trading day. We normalise the time component to noon UTC to keep
 *   the returned value well-defined regardless of the input's clock
 *   time — callers that want the ET date should pass through
 *   `etDateString()`.
 */
export function nextTradingDay(date) {
  assertDate(date, 'nextTradingDay');
  const MAX_LOOKAHEAD_DAYS = 14; // longer than any realistic NYSE gap
  const startDs = etDateString(date);
  let cursor = new Date(date.getTime());
  for (let step = 0; step < MAX_LOOKAHEAD_DAYS; step += 1) {
    cursor = new Date(cursor.getTime() + 24 * 60 * 60 * 1000);
    const cursorDs = etDateString(cursor);
    if (cursorDs !== startDs && isTradingDay(cursor)) {
      // Normalise to 12:00 UTC on the ET date so the returned Date's
      // ET calendar date is stable regardless of DST shifts.
      const [y, m, d] = cursorDs.split('-').map(Number);
      return new Date(Date.UTC(y, m - 1, d, 12, 0, 0, 0));
    }
  }
  throw new Error(
    `nextTradingDay: no trading day found within ${MAX_LOOKAHEAD_DAYS} days after ${startDs}`,
  );
}

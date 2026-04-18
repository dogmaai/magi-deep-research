import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import {
  etDateString,
  etWeekday,
  getHolidayName,
  isHoliday,
  isTradingDay,
  isWeekend,
  nextTradingDay,
} from '../src/nyse.mjs';

/**
 * Construct a UTC Date at `hour:00` on `YYYY-MM-DD`. We pin the hour
 * so the tests are explicit about where the ET conversion lands
 * relative to the UTC date boundary.
 *
 * @param {string} ymd - `YYYY-MM-DD`
 * @param {number} [hour=17] - UTC hour (default 17:00 UTC = 12/13 ET)
 */
function at(ymd, hour = 17) {
  const [y, m, d] = ymd.split('-').map(Number);
  return new Date(Date.UTC(y, m - 1, d, hour, 0, 0, 0));
}

describe('etDateString', () => {
  it('renders YYYY-MM-DD in America/New_York', () => {
    // 2026-07-04 17:00 UTC = 13:00 EDT on July 4
    assert.equal(etDateString(at('2026-07-04', 17)), '2026-07-04');
  });

  it('crosses the ET date boundary correctly at UTC midnight', () => {
    // 2026-07-04 03:30 UTC = 23:30 EDT on July 3
    assert.equal(
      etDateString(new Date(Date.UTC(2026, 6, 4, 3, 30))),
      '2026-07-03',
    );
  });

  it('throws TypeError for invalid Date', () => {
    assert.throws(() => etDateString(new Date('not a date')), TypeError);
    assert.throws(() => etDateString(null), TypeError);
    assert.throws(() => etDateString('2026-07-04'), TypeError);
  });
});

describe('etWeekday / isWeekend', () => {
  it('returns 5 (Friday) for 2026-04-17 at noon ET', () => {
    assert.equal(etWeekday(at('2026-04-17', 16)), 5);
    assert.equal(isWeekend(at('2026-04-17', 16)), false);
  });

  it('returns 6 (Saturday) for 2026-04-18 at noon ET', () => {
    assert.equal(etWeekday(at('2026-04-18', 16)), 6);
    assert.equal(isWeekend(at('2026-04-18', 16)), true);
  });

  it('returns 0 (Sunday) for 2026-04-19 at noon ET', () => {
    assert.equal(etWeekday(at('2026-04-19', 16)), 0);
    assert.equal(isWeekend(at('2026-04-19', 16)), true);
  });

  it('respects the ET date boundary (UTC Monday 03:30 is still Sunday ET)', () => {
    // 2026-04-20 03:30 UTC = 23:30 EDT on 2026-04-19 (Sun)
    const d = new Date(Date.UTC(2026, 3, 20, 3, 30));
    assert.equal(etWeekday(d), 0);
    assert.equal(isWeekend(d), true);
  });
});

describe('getHolidayName / isHoliday', () => {
  it('identifies New Year\'s Day (2026-01-01, Thu)', () => {
    assert.match(getHolidayName(at('2026-01-01')), /New Year/i);
    assert.equal(isHoliday(at('2026-01-01')), true);
  });

  it('identifies Good Friday (2026-04-03)', () => {
    assert.match(getHolidayName(at('2026-04-03')), /Good Friday/i);
    assert.equal(isHoliday(at('2026-04-03')), true);
  });

  it('identifies Memorial Day (last Mon of May 2026 = 2026-05-25)', () => {
    assert.match(getHolidayName(at('2026-05-25')), /Memorial/i);
  });

  it('identifies Thanksgiving Day (4th Thu of Nov 2026 = 2026-11-26)', () => {
    assert.match(getHolidayName(at('2026-11-26')), /Thanksgiving/i);
  });

  it('identifies Christmas Day (2026-12-25, Fri)', () => {
    assert.match(getHolidayName(at('2026-12-25')), /Christmas/i);
  });

  it('returns null for ordinary trading Fridays', () => {
    assert.equal(getHolidayName(at('2026-04-17')), null);
    assert.equal(isHoliday(at('2026-04-17')), false);
  });

  it('returns null for weekends (isHoliday is NYSE-holiday-only)', () => {
    // Saturdays / Sundays are NOT holidays per this function — they
    // are filtered out separately by isWeekend(). isTradingDay is the
    // composite check.
    assert.equal(getHolidayName(at('2026-04-18', 16)), null);
    assert.equal(isHoliday(at('2026-04-18', 16)), false);
  });

  it('applies the Saturday → Friday observation rule (July 4 2026 → Fri July 3)', () => {
    // July 4 2026 is Saturday; NYSE observes Independence Day on
    // Friday July 3.
    assert.match(getHolidayName(at('2026-07-03')), /Independence/i);
    assert.equal(getHolidayName(at('2026-07-04', 16)), null);
  });
});

describe('isTradingDay', () => {
  it('returns true for an ordinary Friday (2026-04-17)', () => {
    assert.equal(isTradingDay(at('2026-04-17', 16)), true);
  });

  it('returns false for the following Saturday (2026-04-18)', () => {
    assert.equal(isTradingDay(at('2026-04-18', 16)), false);
  });

  it('returns false for the following Sunday (2026-04-19)', () => {
    assert.equal(isTradingDay(at('2026-04-19', 16)), false);
  });

  it('returns false for New Year\'s Day on a Thursday (2026-01-01)', () => {
    assert.equal(isTradingDay(at('2026-01-01', 16)), false);
  });

  it('returns false for Good Friday (2026-04-03)', () => {
    assert.equal(isTradingDay(at('2026-04-03', 16)), false);
  });

  it('returns false for observed Independence Day (Fri 2026-07-03)', () => {
    assert.equal(isTradingDay(at('2026-07-03', 16)), false);
  });

  it('returns true for the day before / after a long weekend (Tue after MLK Day 2026-01-20)', () => {
    // 2026-01-19 is MLK Day (Mon); 2026-01-20 is Tue (trading day)
    assert.equal(isTradingDay(at('2026-01-19', 16)), false);
    assert.equal(isTradingDay(at('2026-01-20', 16)), true);
  });

  it('treats half-day sessions as trading days (day after Thanksgiving 2026-11-27)', () => {
    // Day after Thanksgiving is a 1pm-ET early close, not a holiday.
    assert.equal(isTradingDay(at('2026-11-27', 16)), true);
  });

  it('throws TypeError for non-Date input', () => {
    assert.throws(() => isTradingDay('2026-04-17'), TypeError);
    assert.throws(() => isTradingDay(null), TypeError);
    assert.throws(() => isTradingDay(new Date(Number.NaN)), TypeError);
  });
});

describe('nextTradingDay', () => {
  it('skips the weekend after a Friday close (Fri 2026-04-17 → Mon 2026-04-20)', () => {
    const nxt = nextTradingDay(at('2026-04-17', 20));
    assert.equal(etDateString(nxt), '2026-04-20');
  });

  it('skips both weekend and Monday holiday (Fri 2026-01-16 → Tue 2026-01-20)', () => {
    // 2026-01-19 Mon = MLK Day (holiday); expect Tue 01-20.
    const nxt = nextTradingDay(at('2026-01-16', 20));
    assert.equal(etDateString(nxt), '2026-01-20');
  });

  it('skips a mid-week holiday (Wed before Thanksgiving 2026-11-25 → Fri 2026-11-27)', () => {
    // 2026-11-26 Thu = Thanksgiving; 11-27 Fri is a trading day.
    const nxt = nextTradingDay(at('2026-11-25', 20));
    assert.equal(etDateString(nxt), '2026-11-27');
  });

  it('starts from a weekend and returns Monday (Sat 2026-04-18 → Mon 2026-04-20)', () => {
    const nxt = nextTradingDay(at('2026-04-18', 16));
    assert.equal(etDateString(nxt), '2026-04-20');
  });

  it('returns a Date whose ET day is indeed a trading day', () => {
    const nxt = nextTradingDay(at('2026-04-03', 16)); // Good Friday
    assert.equal(isTradingDay(nxt), true);
    // First trading day after Good Friday is Mon 2026-04-06.
    assert.equal(etDateString(nxt), '2026-04-06');
  });

  it('throws TypeError for invalid Date', () => {
    assert.throws(() => nextTradingDay(new Date(Number.NaN)), TypeError);
    assert.throws(() => nextTradingDay(undefined), TypeError);
  });
});

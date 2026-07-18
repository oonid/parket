//! Gregorian calendar math shared across modules (epoch-day → Y/M/D).

pub(crate) fn epoch_days_to_ymd(days: i64) -> (i64, i64, i64) {
    let mut year = 1970i64;
    let mut remaining = days;

    // L1: skip whole 400-year cycles in one O(1) step before falling into the year-at-a-time
    // loop below. The Gregorian leap-year pattern (`is_leap` only ever looks at year % 4,
    // % 100, % 400) repeats exactly every 400 years, so every 400-year span — starting from
    // ANY year, not just a multiple of 400 — covers exactly 146097 days (100 leap years every
    // 4, minus 4 for every 100, plus 1 back for every 400 = 97 leap years + 303 common years).
    // Skipping whole cycles this way changes nothing about the resulting (year, month, day);
    // it only bounds the loop below to at most a few hundred iterations. Without it, a
    // pathological `days` near i64::MAX/i64::MIN would force that loop through up to ~2.5e16
    // iterations — not a correctness bug by itself, but an effectively unbounded hang.
    //
    // The div_euclid/rem_euclid run in i128 so nothing can overflow regardless of `days`'s
    // magnitude. `rem_euclid` yields the Euclidean remainder — always in [0, 146097) — which
    // becomes the post-shift `remaining`; casting THAT to i64 is exact. We deliberately do NOT
    // reconstruct `remaining` as `days - cycles*146097` in i64: at `days == i64::MIN` that
    // product is a hair BELOW i64::MIN (the largest 146097-multiple ≤ i64::MIN undershoots it
    // by the remainder), so the i64 subtraction would wrap and leave the loop unbounded again.
    // `cycles * 400` cannot overflow i64 here: |cycles| ≤ i64::MAX/146097 ≈ 6.3e13, so
    // |cycles*400| ≈ 2.5e16, far inside i64; `saturating_add` is a belt-and-braces guard.
    const DAYS_PER_400_YEARS: i64 = 146_097;
    let cycles = (remaining as i128).div_euclid(DAYS_PER_400_YEARS as i128);
    if cycles != 0 {
        let year_shift = (cycles * 400) as i64;
        year = year.saturating_add(year_shift);
        remaining = (remaining as i128).rem_euclid(DAYS_PER_400_YEARS as i128) as i64;
    }

    loop {
        let year_len = if is_leap(year) { 366 } else { 365 };
        if remaining >= 0 && remaining < year_len {
            break;
        }
        if remaining >= 0 {
            remaining -= year_len;
            year += 1;
        } else {
            year -= 1;
            remaining += if is_leap(year) { 366 } else { 365 };
        }
    }

    let leap = is_leap(year);
    let month_days = [
        31,
        if leap { 29 } else { 28 },
        31,
        30,
        31,
        30,
        31,
        31,
        30,
        31,
        30,
        31,
    ];

    let mut month = 1i64;
    for &md in &month_days {
        if remaining < md {
            break;
        }
        remaining -= md;
        month += 1;
    }

    (year, month, remaining + 1)
}

pub(crate) fn is_leap(year: i64) -> bool {
    (year % 4 == 0 && year % 100 != 0) || year % 400 == 0
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn epoch_days_to_ymd_epoch() {
        let (y, m, d) = epoch_days_to_ymd(0);
        assert_eq!((y, m, d), (1970, 1, 1));
    }

    #[test]
    fn epoch_days_to_ymd_known_date() {
        let (y, m, d) = epoch_days_to_ymd(365);
        assert_eq!((y, m, d), (1971, 1, 1));
    }

    #[test]
    fn epoch_days_to_ymd_negative_day() {
        let (y, m, d) = epoch_days_to_ymd(-1);
        assert_eq!((y, m, d), (1969, 12, 31));
    }

    #[test]
    fn epoch_days_to_ymd_negative_large() {
        let (y, m, d) = epoch_days_to_ymd(-365);
        assert_eq!((y, m, d), (1969, 1, 1));
    }

    #[test]
    fn epoch_days_to_ymd_month_boundary() {
        let (y, m, d) = epoch_days_to_ymd(31);
        assert_eq!((y, m, d), (1970, 2, 1));
    }

    #[test]
    fn epoch_days_to_ymd_leap_year_1972() {
        let days_to_1972_0203 = 365 + 365 + 33;
        let (y, m, d) = epoch_days_to_ymd(days_to_1972_0203);
        assert_eq!((y, m, d), (1972, 2, 3));
    }

    #[test]
    fn epoch_days_to_ymd_year_boundary() {
        let (y, m, d) = epoch_days_to_ymd(730);
        assert_eq!((y, m, d), (1972, 1, 1));
    }

    /// L1: pins a normal-range result (2024-03-01, well past the 400-year-cycle threshold of
    /// 146097 days from a magnitude standpoint but not from the fast path's no-op-for-small-
    /// inputs standpoint) to prove the 400-year-cycle fast path changes nothing for an
    /// everyday date.
    #[test]
    fn epoch_days_to_ymd_normal_date_unchanged() {
        let days_to_2024_03_01 = 19_783;
        let (y, m, d) = epoch_days_to_ymd(days_to_2024_03_01);
        assert_eq!((y, m, d), (2024, 3, 1));
    }

    /// L1: `days` near i64::MAX used to force the year-at-a-time loop through on the order of
    /// 2.5e16 iterations (effectively an unbounded hang, not a real panic/wrap in practice,
    /// but unusable). The 400-year-cycle fast path bounds this to a handful of iterations —
    /// this test times out (rather than merely "eventually" passing) if the fast path
    /// regresses back to the unbounded loop.
    #[test]
    fn epoch_days_to_ymd_extreme_positive_no_panic_or_hang() {
        let (y, m, d) = epoch_days_to_ymd(i64::MAX);
        assert!(y > 1970, "expected a far-future year, got {y}");
        assert!((1..=12).contains(&m), "month out of range: {m}");
        assert!((1..=31).contains(&d), "day out of range: {d}");
    }

    /// L1: same as above for the negative extreme (`days` near i64::MIN).
    #[test]
    fn epoch_days_to_ymd_extreme_negative_no_panic_or_hang() {
        let (y, m, d) = epoch_days_to_ymd(i64::MIN);
        assert!(y < 1970, "expected a far-past year, got {y}");
        assert!((1..=12).contains(&m), "month out of range: {m}");
        assert!((1..=31).contains(&d), "day out of range: {d}");
    }

    #[test]
    fn is_leap_true_div4() {
        assert!(is_leap(2024));
    }

    #[test]
    fn is_leap_true_div400() {
        assert!(is_leap(2000));
    }

    #[test]
    fn is_leap_false_div100() {
        assert!(!is_leap(1900));
    }

    #[test]
    fn is_leap_false_normal() {
        assert!(!is_leap(2023));
    }
}

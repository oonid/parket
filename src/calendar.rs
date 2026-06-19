//! Gregorian calendar math shared across modules (epoch-day → Y/M/D).

pub(crate) fn epoch_days_to_ymd(days: i64) -> (i64, i64, i64) {
    let mut year = 1970i64;
    let mut remaining = days;

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

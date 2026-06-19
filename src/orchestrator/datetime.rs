pub(crate) fn format_timestamp_now() -> String {
    let dur = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default();
    let secs = dur.as_secs();
    let days = secs / 86400;
    let time_secs = secs % 86400;
    let hours = time_secs / 3600;
    let minutes = (time_secs % 3600) / 60;
    let seconds = time_secs % 60;
    let (year, month, day) = epoch_days_to_ymd(days as i64);
    format!(
        "{year:04}-{month:02}-{day:02}T{hours:02}:{minutes:02}:{seconds:02}Z"
    )
}

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

fn is_leap(year: i64) -> bool {
    (year % 4 == 0 && year % 100 != 0) || year % 400 == 0
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn format_timestamp_now_produces_valid_string() {
        let ts = format_timestamp_now();
        assert!(ts.contains('T'));
        assert!(ts.ends_with('Z'));
        assert!(ts.contains("20"));
    }

    #[test]
    fn epoch_days_to_ymd_orch_test() {
        let (y, m, d) = epoch_days_to_ymd(0);
        assert_eq!((y, m, d), (1970, 1, 1));
    }

    #[test]
    fn is_leap_orch_test() {
        assert!(is_leap(2024));
        assert!(!is_leap(2023));
    }
}

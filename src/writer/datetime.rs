use deltalake::arrow::array::{Array, StringArray, TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray, TimestampSecondArray};
use std::sync::Arc;

use crate::calendar::epoch_days_to_ymd;

pub(crate) fn extract_timestamp_as_strings(col: &Arc<dyn Array>) -> Option<Vec<String>> {
    if let Some(ts) = col.as_any().downcast_ref::<TimestampMicrosecondArray>() {
        Some(
            (0..ts.len())
                .map(|i| {
                    if ts.is_null(i) {
                        String::new()
                    } else {
                        micros_to_string(ts.value(i))
                    }
                })
                .collect(),
        )
    } else if let Some(ts) = col.as_any().downcast_ref::<TimestampMillisecondArray>() {
        Some(
            (0..ts.len())
                .map(|i| {
                    if ts.is_null(i) {
                        String::new()
                    } else {
                        millis_to_string(ts.value(i))
                    }
                })
                .collect(),
        )
    } else if let Some(ts) = col.as_any().downcast_ref::<TimestampSecondArray>() {
        Some(
            (0..ts.len())
                .map(|i| {
                    if ts.is_null(i) {
                        String::new()
                    } else {
                        secs_to_string(ts.value(i))
                    }
                })
                .collect(),
        )
    } else if let Some(ts) = col.as_any().downcast_ref::<TimestampNanosecondArray>() {
        Some(
            (0..ts.len())
                .map(|i| {
                    if ts.is_null(i) {
                        String::new()
                    } else {
                        nanos_to_string(ts.value(i))
                    }
                })
                .collect(),
        )
    } else {
        col.as_any()
            .downcast_ref::<StringArray>()
            .map(|s| (0..s.len()).map(|i| s.value(i).to_string()).collect())
    }
}

pub(crate) fn micros_to_string(micros: i64) -> String {
    let secs = micros / 1_000_000;
    let subsec_nanos = (micros % 1_000_000).unsigned_abs() as u32 * 1000;
    format_naive_datetime(secs, subsec_nanos)
}

pub(crate) fn millis_to_string(millis: i64) -> String {
    let secs = millis / 1000;
    let subsec_nanos = ((millis % 1000).unsigned_abs() as u32) * 1_000_000;
    format_naive_datetime(secs, subsec_nanos)
}

pub(crate) fn secs_to_string(secs: i64) -> String {
    format_naive_datetime(secs, 0)
}

pub(crate) fn nanos_to_string(nanos: i64) -> String {
    let secs = nanos / 1_000_000_000;
    let subsec_nanos = (nanos % 1_000_000_000).unsigned_abs() as u32;
    format_naive_datetime(secs, subsec_nanos)
}

pub(crate) fn format_naive_datetime(secs: i64, subsec_nanos: u32) -> String {
    let time_secs = secs.rem_euclid(86400);
    let days = secs.div_euclid(86400);
    let hours = time_secs / 3600;
    let minutes = (time_secs % 3600) / 60;
    let seconds = time_secs % 60;

    let (year, month, day) = epoch_days_to_ymd(days);

    if subsec_nanos > 0 {
        let frac = format!("{subsec_nanos:09}").trim_end_matches('0').to_string();
        format!(
            "{year:04}-{month:02}-{day:02} {:02}:{:02}:{:02}.{frac}",
            hours, minutes, seconds
        )
    } else {
        format!(
            "{year:04}-{month:02}-{day:02} {:02}:{:02}:{:02}",
            hours, minutes, seconds
        )
    }
}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn format_naive_datetime_basic() {
        let result = format_naive_datetime(0, 0);
        assert_eq!(result, "1970-01-01 00:00:00");
    }

    #[test]
    fn format_naive_datetime_with_subsec() {
        let result = format_naive_datetime(0, 500_000_000);
        assert_eq!(result, "1970-01-01 00:00:00.5");
    }

    #[test]
    fn format_naive_datetime_known_date() {
        let result = format_naive_datetime(1743158400, 0);
        assert!(result.starts_with("2025-"));
    }

    #[test]
    fn format_naive_datetime_negative_secs() {
        let result = format_naive_datetime(-86400, 0);
        assert_eq!(result, "1969-12-31 00:00:00");
    }

    #[test]
    fn format_naive_datetime_negative_secs_with_subsec() {
        let result = format_naive_datetime(-1, 500_000_000);
        assert_eq!(result, "1969-12-31 23:59:59.5");
    }

    #[test]
    fn format_naive_datetime_trailing_zeros() {
        let result = format_naive_datetime(0, 123_456_000);
        assert_eq!(result, "1970-01-01 00:00:00.123456");
    }

    #[test]
    fn format_naive_datetime_zero_subsec_nanos() {
        let result = format_naive_datetime(0, 0);
        assert_eq!(result, "1970-01-01 00:00:00");
    }

    #[test]
    fn micros_to_string_conversion() {
        let result = micros_to_string(1743158400000000i64);
        assert!(result.contains("2025"));
    }

    #[test]
    fn micros_to_string_negative() {
        let result = micros_to_string(-1_000_000);
        assert!(result.contains("1969"));
    }

    #[test]
    fn millis_to_string_conversion() {
        let result = millis_to_string(1743158400000i64);
        assert!(result.contains("2025"));
    }

    #[test]
    fn millis_to_string_negative() {
        let result = millis_to_string(-1000);
        assert!(result.contains("1969"));
    }

    #[test]
    fn secs_to_string_conversion() {
        let result = secs_to_string(1743158400i64);
        assert!(result.contains("2025"));
    }

    #[test]
    fn secs_to_string_negative() {
        let result = secs_to_string(-1);
        assert!(result.contains("1969"));
    }

}

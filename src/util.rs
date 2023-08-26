use crate::base::TimeStamp;

/// Utility functions

/// Returns the current time in UTC as a timestamp in milliseconds.
pub fn utc_now() -> TimeStamp {
    chrono::DateTime::timestamp_millis(&chrono::Utc::now()).into()
}

/// Returns the given i64 timestamp as a UTC datetime.
pub fn ts_to_utc(ts: impl Into<TimeStamp>) -> chrono::DateTime<chrono::Utc> {
    chrono::DateTime::from_utc(
        chrono::NaiveDateTime::from_timestamp_millis(ts.into().into())
            .unwrap_or(chrono::NaiveDateTime::from_timestamp_millis(0).unwrap()),
        chrono::Utc,
    )
}

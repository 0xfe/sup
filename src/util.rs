pub fn utc_now() -> i64 {
    chrono::DateTime::timestamp_millis(&chrono::Utc::now())
}

pub fn ts_to_utc(ts: i64) -> chrono::DateTime<chrono::Utc> {
    chrono::DateTime::from_utc(
        chrono::NaiveDateTime::from_timestamp_millis(ts)
            .unwrap_or(chrono::NaiveDateTime::from_timestamp_millis(0).unwrap()),
        chrono::Utc,
    )
}

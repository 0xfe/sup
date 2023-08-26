extern crate derive_more;
use core::fmt;

use derive_more::{Add, Div, From, Into, Mul, Sub};

#[repr(transparent)]
#[derive(From, Into, Debug, PartialEq, Eq, Clone, Ord, PartialOrd, Add, Sub, Mul, Div, Copy)]
pub struct TimeStamp(pub i64);

impl TimeStamp {
    pub fn now() -> Self {
        Self(chrono::DateTime::timestamp_millis(&chrono::Utc::now()))
    }

    pub fn to_utc(&self) -> chrono::DateTime<chrono::Utc> {
        chrono::DateTime::from_utc(
            chrono::NaiveDateTime::from_timestamp_millis(self.0)
                .unwrap_or(chrono::NaiveDateTime::from_timestamp_millis(0).unwrap()),
            chrono::Utc,
        )
    }

    pub fn from_utc(dt: chrono::DateTime<chrono::Utc>) -> Self {
        Self(dt.timestamp_millis())
    }

    pub fn millis(&self) -> i64 {
        self.0
    }
}

impl fmt::Display for TimeStamp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.to_utc())
    }
}

#[repr(transparent)]
#[derive(From, Into, Debug, PartialEq, Eq, Clone, Ord, PartialOrd, Add, Sub, Mul, Div, Copy)]
pub struct Duration(pub i64);

impl Duration {
    pub fn millis(&self) -> i64 {
        self.0
    }

    pub fn from_minutes(mins: i64) -> Self {
        Self(mins * 60 * 1000)
    }

    pub fn from_secs(secs: i64) -> Self {
        Self(secs * 1000)
    }

    pub fn from_millis(millis: i64) -> Self {
        Self(millis)
    }
}

impl fmt::Display for Duration {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let secs = self.0 / 1000;
        let millis = self.0 % 1000;
        write!(f, "{}.{:03}s", secs, millis)
    }
}

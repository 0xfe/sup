use std::fmt;

use crate::{
    base::TimeStamp,
    sample::{Sample, SampleValue},
};

/// Element represents a single timestamped sample.
#[derive(Debug, Clone)]
pub struct Element<T: SampleValue>(pub TimeStamp, pub Sample<T>);

impl<T: SampleValue, U: Into<TimeStamp>> From<(U, Sample<T>)> for Element<T> {
    fn from((ts, sample): (U, Sample<T>)) -> Self {
        Self(ts.into(), sample)
    }
}

impl<T: SampleValue> fmt::Display for Element<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} {}", self.0, self.1)
    }
}

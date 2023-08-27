use std::fmt;

use crate::{
    base::*,
    element::Element,
    sample::{Sample, SampleValue},
    window::WindowIter,
};

/// `RawSeries` represents a series of raw timestamped
/// data samples.
#[derive(Debug, Clone)]
pub struct RawSeries<T: SampleValue> {
    pub values: Vec<Element<T>>,
}

impl<T: SampleValue> RawSeries<T> {
    /// Create a new empty series.
    pub fn new() -> Self {
        Self { values: vec![] }
    }

    /// Returns the last value in the series.
    pub fn last_val(&self) -> T {
        self.values
            .last()
            .unwrap_or(&(0, Sample::zero()).into())
            .1
            .val()
    }

    /// Add a new sample to the series. The timestamp must be greater than the
    /// last sample's timestamp.
    pub fn push(&mut self, ts: TimeStamp, value: T) {
        self.push_sample(ts, Sample::point(value))
    }

    /// Add a new sample to the series. The timestamp must be greater than the
    /// last sample's timestamp.
    pub fn push_sample(&mut self, ts: TimeStamp, sample: Sample<T>) {
        self.values.push((ts, sample).into());
    }

    /// Returns the number of samples in the series.
    pub fn len(&self) -> usize {
        self.values.len()
    }

    /// Returns true if the series is empty.
    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }

    /// Get the sample at the given index.
    pub fn get(&self, index: usize) -> Option<&Element<T>> {
        self.values.get(index)
    }

    /// Return an iterator over windows of the series.
    pub fn windows(&self, window_size: Interval, start_ts: TimeStamp) -> WindowIter<T> {
        WindowIter::new(self, window_size, start_ts)
    }

    /// Returns the nearest sample after or equal to the given timestamp.
    pub fn at_or_after(&self, ts: TimeStamp) -> Option<&Element<T>> {
        // Binary search for the first sample with a timestamp greater than or
        // equal to the given timestamp.
        let mut left = 0;
        let mut right = self.values.len();

        while left < right {
            let mid = left + (right - left) / 2;
            if self.values[mid].0 < ts {
                left = mid + 1;
            } else {
                right = mid;
            }
        }

        if left < self.values.len() {
            self.get(left)
        } else {
            None
        }
    }
}

impl<T: SampleValue> Default for RawSeries<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T: SampleValue> fmt::Display for RawSeries<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for sample in self.values.iter() {
            write!(f, "\n {}", sample)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sample::SampleEquals;

    #[test]
    fn nearest_after() {
        let mut series = RawSeries::new();
        series.push(0.into(), 0);
        series.push(1.into(), 1);
        series.push(2.into(), 2);
        series.push(3.into(), 3);
        series.push(4.into(), 4);
        series.push(5.into(), 5);
        series.push(6.into(), 6);
        series.push(7.into(), 7);
        series.push(8.into(), 8);
        series.push(9.into(), 9);

        assert_eq!(series.at_or_after(TimeStamp(0)).unwrap().0, TimeStamp(0));
        assert!(series
            .at_or_after(0.into())
            .unwrap()
            .1
            .equals(&Sample::point(0)));

        assert_eq!(series.at_or_after(1.into()).unwrap().0, 1.into());
        assert!(series
            .at_or_after(TimeStamp(1))
            .unwrap()
            .1
            .equals(&Sample::point(1)));

        assert_eq!(series.at_or_after(TimeStamp(9)).unwrap().0, 9.into());
        assert!(series
            .at_or_after(TimeStamp(9))
            .unwrap()
            .1
            .equals(&Sample::point(9)));

        assert!(series.at_or_after(TimeStamp(10)).is_none())
    }

    #[test]
    fn nearest_after_random_intervals() {
        let mut series = RawSeries::new();
        series.push(0.into(), 0);
        series.push(200.into(), 1);
        series.push(350.into(), 2);
        series.push(500.into(), 3);
        series.push(1023.into(), 4);
        series.push(3044.into(), 5);
        series.push(4033.into(), 6);
        series.push(9000.into(), 7);

        assert_eq!(series.at_or_after(TimeStamp(0)).unwrap().0, 0.into());
        assert!(series
            .at_or_after(TimeStamp(0))
            .unwrap()
            .1
            .equals(&Sample::point(0)),);

        assert_eq!(series.at_or_after(TimeStamp(1)).unwrap().0, 200.into());
        assert!(series
            .at_or_after(TimeStamp(1))
            .unwrap()
            .1
            .equals(&Sample::point(1)),);

        assert_eq!(series.at_or_after(TimeStamp(2)).unwrap().0, 200.into());
        assert!(series
            .at_or_after(TimeStamp(2))
            .unwrap()
            .1
            .equals(&Sample::point(1)),);

        assert_eq!(series.at_or_after(TimeStamp(201)).unwrap().0, 350.into());
        assert!(series
            .at_or_after(TimeStamp(201))
            .unwrap()
            .1
            .equals(&Sample::point(2)));

        assert_eq!(series.at_or_after(TimeStamp(350)).unwrap().0, 350.into());
        assert!(series
            .at_or_after(TimeStamp(350))
            .unwrap()
            .1
            .equals(&Sample::point(2)));

        assert_eq!(series.at_or_after(TimeStamp(351)).unwrap().0, 500.into());
        assert!(series
            .at_or_after(TimeStamp(351))
            .unwrap()
            .1
            .equals(&Sample::point(3)));

        assert_eq!(series.at_or_after(TimeStamp(500)).unwrap().0, 500.into());
        assert!(series
            .at_or_after(TimeStamp(500))
            .unwrap()
            .1
            .equals(&Sample::point(3)));

        assert!(series.at_or_after(TimeStamp(9001)).is_none());
    }
}

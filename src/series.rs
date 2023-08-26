use std::{fmt, time::Duration};

use num_traits::Zero;

use crate::{sample::Sample, util::ts_to_utc, window::WindowIter};

/// `Series` represents an unaligned Time Series.
pub struct Series<T> {
    pub values: Vec<(i64, Sample<T>)>,
}

impl<T: Zero + Copy> Series<T> {
    /// Create a new empty series.
    pub fn new() -> Self {
        Self { values: vec![] }
    }

    /// Returns the last value in the series.
    pub fn last_val(&self) -> T {
        self.values.last().unwrap_or(&(0, Sample::zero())).1.val()
    }

    /// Add a new sample to the series. The timestamp must be greater than the
    /// last sample's timestamp.
    pub fn push(&mut self, ts: i64, value: T) {
        self.push_sample(ts, Sample::point(value))
    }

    /// Add a new sample to the series. The timestamp must be greater than the
    /// last sample's timestamp.
    pub fn push_sample(&mut self, ts: i64, sample: Sample<T>) {
        self.values.push((ts, sample));
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
    pub fn get(&self, index: usize) -> Option<(i64, T)> {
        self.values.get(index).map(|s| (s.0, s.1.val()))
    }

    /// Return an iterator over windows of the series.
    pub fn windows_iter(&self, window_size: Duration, start_ts: i64) -> WindowIter<T> {
        WindowIter::new(self, window_size, start_ts)
    }

    /// Returns the nearest sample after the given timestamp.
    pub fn nearest_after(&self, ts: i64) -> Option<(i64, T)> {
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

impl<T: Zero + Copy> Default for Series<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T: fmt::Display + Zero + Copy> fmt::Display for Series<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for sample in self.values.iter() {
            write!(f, "\n {} {}", ts_to_utc(sample.0), sample.1)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn nearest_after() {
        let mut series = Series::new();
        series.push(0, 0);
        series.push(1, 1);
        series.push(2, 2);
        series.push(3, 3);
        series.push(4, 4);
        series.push(5, 5);
        series.push(6, 6);
        series.push(7, 7);
        series.push(8, 8);
        series.push(9, 9);

        assert_eq!(series.nearest_after(0), Some((0, 0)));
        assert_eq!(series.nearest_after(1), Some((1, 1)));
        assert_eq!(series.nearest_after(2), Some((2, 2)));
        assert_eq!(series.nearest_after(3), Some((3, 3)));
        assert_eq!(series.nearest_after(4), Some((4, 4)));
        assert_eq!(series.nearest_after(5), Some((5, 5)));
        assert_eq!(series.nearest_after(6), Some((6, 6)));
        assert_eq!(series.nearest_after(7), Some((7, 7)));
        assert_eq!(series.nearest_after(8), Some((8, 8)));
        assert_eq!(series.nearest_after(9), Some((9, 9)));
        assert_eq!(series.nearest_after(10), None);
    }

    #[test]
    fn nearest_after_random_intervals() {
        let mut series = Series::new();
        series.push(0, 0);
        series.push(200, 1);
        series.push(350, 2);
        series.push(500, 3);
        series.push(1023, 4);
        series.push(3044, 5);
        series.push(4033, 6);
        series.push(9000, 7);

        assert_eq!(series.nearest_after(0), Some((0, 0)));
        assert_eq!(series.nearest_after(1), Some((200, 1)));
        assert_eq!(series.nearest_after(2), Some((200, 1)));
        assert_eq!(series.nearest_after(201), Some((350, 2)));
        assert_eq!(series.nearest_after(350), Some((350, 2)));
        assert_eq!(series.nearest_after(351), Some((500, 3)));
        assert_eq!(series.nearest_after(500), Some((500, 3)));
        assert_eq!(series.nearest_after(9001), None);
    }
}

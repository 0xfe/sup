use std::fmt;

use crate::{
    sample::{Sample, SampleValue},
    util::ts_to_utc,
    window::WindowIter,
    window_ops::Op,
};

#[derive(Debug, Clone)]
pub struct Element<T: SampleValue>(pub i64, pub Sample<T>);
impl<T: SampleValue> fmt::Display for Element<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} {}", ts_to_utc(self.0), self.1)
    }
}

impl<T: SampleValue> From<(i64, Sample<T>)> for Element<T> {
    fn from((ts, sample): (i64, Sample<T>)) -> Self {
        Self(ts, sample)
    }
}

/// `Series` represents an unaligned Time Series.
pub struct Series<T: SampleValue> {
    pub values: Vec<Element<T>>,
}

impl<T: SampleValue> Series<T> {
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
    pub fn push(&mut self, ts: i64, value: T) {
        self.push_sample(ts, Sample::point(value))
    }

    /// Add a new sample to the series. The timestamp must be greater than the
    /// last sample's timestamp.
    pub fn push_sample(&mut self, ts: i64, sample: Sample<T>) {
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
    pub fn get(&self, index: usize) -> Option<(i64, Sample<T>)> {
        self.values.get(index).map(|s| (s.0, s.1))
    }

    /// Return an iterator over windows of the series.
    pub fn windows_iter(&self, window_size: i64, start_ts: i64) -> WindowIter<T> {
        WindowIter::new(self, window_size, start_ts)
    }

    /// Returns the nearest sample after or equal to the given timestamp.
    pub fn at_or_after(&self, ts: i64) -> Option<(i64, Sample<T>)> {
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

impl<T: SampleValue> Default for Series<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T: SampleValue> fmt::Display for Series<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for sample in self.values.iter() {
            write!(f, "\n {} {}", ts_to_utc(sample.0), sample.1)?;
        }
        Ok(())
    }
}

/// `AlignedSeries` represents Time Series with a fixed interval between
/// samples.
#[derive(Debug)]
pub struct AlignedSeries<T: SampleValue> {
    pub start_ts: i64,
    pub interval: i64,
    pub values: Vec<Sample<T>>,
}

impl<T: SampleValue> AlignedSeries<T> {
    /// Create a new empty series.
    pub fn new(start_ts: i64, interval: i64) -> Self {
        Self {
            start_ts,
            interval,
            values: vec![],
        }
    }

    pub fn push_series(&mut self, series: &Series<T>, op: Op<T>) {
        for v in series
            .windows_iter(self.interval, self.start_ts)
            .aggregate(op)
        {
            self.push(v);
        }
    }

    /// Add a new value to the series.
    pub fn push(&mut self, value: T) {
        self.push_sample(Sample::point(value));
    }

    /// Add a new sample to the series.
    pub fn push_sample(&mut self, sample: Sample<T>) {
        self.values.push(sample);
    }

    /// Returns the number of samples in the series.
    pub fn len(&self) -> usize {
        self.values.len()
    }

    /// Returns true if the series is empty.
    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }

    /// Get the nearest sample after or equal to the given timestamp.
    pub fn at_or_after(&self, ts: i64) -> Option<(i64, Sample<T>)> {
        if ts <= self.start_ts {
            if self.is_empty() {
                return None;
            } else {
                return Some((self.start_ts, self.values[0]));
            }
        }

        if (ts - self.start_ts) % self.interval == 0 {
            let index = ((ts - self.start_ts) / self.interval) as usize;
            if index < self.values.len() {
                return Some((ts, self.values[index]));
            }
        } else {
            let index = ((ts - self.start_ts) / self.interval) as usize + 1;
            if index < self.values.len() {
                return Some((
                    self.start_ts + (index as i64 * self.interval),
                    self.values[index],
                ));
            }
        }

        None
    }
}

impl<T> fmt::Display for AlignedSeries<T>
where
    T: SampleValue + fmt::Display,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for (i, sample) in self.values.iter().enumerate() {
            write!(
                f,
                "\n {} {}",
                ts_to_utc(self.start_ts + (i as i64 * self.interval)),
                sample
            )?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {

    use crate::{sample::SampleEquals, window_ops::sum};

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

        assert_eq!(series.at_or_after(0).unwrap().0, 0);
        assert!(series.at_or_after(0).unwrap().1.equals(&Sample::point(0)));

        assert_eq!(series.at_or_after(1).unwrap().0, 1);
        assert!(series.at_or_after(1).unwrap().1.equals(&Sample::point(1)));

        assert_eq!(series.at_or_after(9).unwrap().0, 9);
        assert!(series.at_or_after(9).unwrap().1.equals(&Sample::point(9)));

        assert!(series.at_or_after(10).is_none())
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

        assert_eq!(series.at_or_after(0).unwrap().0, 0);
        assert!(series.at_or_after(0).unwrap().1.equals(&Sample::point(0)),);

        assert_eq!(series.at_or_after(1).unwrap().0, 200);
        assert!(series.at_or_after(1).unwrap().1.equals(&Sample::point(1)),);

        assert_eq!(series.at_or_after(2).unwrap().0, 200);
        assert!(series.at_or_after(2).unwrap().1.equals(&Sample::point(1)),);

        assert_eq!(series.at_or_after(201).unwrap().0, 350);
        assert!(series.at_or_after(201).unwrap().1.equals(&Sample::point(2)));

        assert_eq!(series.at_or_after(350).unwrap().0, 350);
        assert!(series.at_or_after(350).unwrap().1.equals(&Sample::point(2)));

        assert_eq!(series.at_or_after(351).unwrap().0, 500);
        assert!(series.at_or_after(351).unwrap().1.equals(&Sample::point(3)));

        assert_eq!(series.at_or_after(500).unwrap().0, 500);
        assert!(series.at_or_after(500).unwrap().1.equals(&Sample::point(3)));

        assert!(series.at_or_after(9001).is_none());
    }

    #[test]
    fn aligned_series() {
        let mut series = AlignedSeries::new(1000, 100);
        series.push(0);
        series.push(1);
        series.push(2);
        series.push(3);
        series.push(4);
        series.push(5);
        series.push(6);
        series.push(7);
        series.push(8);
        series.push(9);

        assert_eq!(series.at_or_after(0).unwrap().0, 1000);
        assert!(series.at_or_after(0).unwrap().1.equals(&Sample::point(0)));

        assert_eq!(series.at_or_after(999).unwrap().0, 1000);
        assert!(series.at_or_after(999).unwrap().1.equals(&Sample::point(0)));

        assert_eq!(series.at_or_after(1000).unwrap().0, 1000);
        assert!(series
            .at_or_after(1000)
            .unwrap()
            .1
            .equals(&Sample::point(0)));

        assert_eq!(series.at_or_after(1010).unwrap().0, 1100);
        assert!(series
            .at_or_after(1010)
            .unwrap()
            .1
            .equals(&Sample::point(1)));

        assert_eq!(series.at_or_after(1100).unwrap().0, 1100);
        assert!(series
            .at_or_after(1100)
            .unwrap()
            .1
            .equals(&Sample::point(1)));

        assert_eq!(series.at_or_after(1900).unwrap().0, 1900);
        assert!(series.at_or_after(1901).is_none());
    }

    #[test]
    fn to_aligned_series() {
        let mut series = Series::new();
        series.push(0, 1);
        series.push(2, 1);
        series.push(3, 1);
        series.push(4, 1);
        series.push(6, 1);
        series.push(7, 1);
        series.push(9, 1);
        series.push(15, 1);
        series.push(22, 1);
        series.push(28, 1);
        series.push(30, 1);
        series.push(31, 1);
        series.push(32, 1);
        series.push(35, 1);
        series.push(40, 1);

        println!("series: {}\n\n", series);

        for e in series.windows_iter(5, 0) {
            println!("w: {:?}", e);
        }

        for e in series.windows_iter(5, 0).samples() {
            println!("e: {:?}", e);
        }

        let mut aligned_series = AlignedSeries::new(0, 5);
        aligned_series.push_series(&series, sum);
        println!("aligned_series: {}\n\n", aligned_series);
    }
}

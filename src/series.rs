use std::fmt;

use crate::{
    base::*,
    element::Element,
    ops::WindowOp,
    sample::{Sample, SampleValue},
    util::ts_to_utc,
    window::WindowIter,
};

/// `RawSeries` represents a series of raw timestamped
/// data samples.
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
    pub fn get(&self, index: usize) -> Option<&Element<T>> {
        self.values.get(index)
    }

    /// Return an iterator over windows of the series.
    pub fn windows(&self, window_size: Duration, start_ts: TimeStamp) -> WindowIter<T> {
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
            write!(f, "\n {} {}", ts_to_utc(sample.0), sample.1)?;
        }
        Ok(())
    }
}

/// `AlignedSeries` represents Time Series with a fixed interval between
/// samples.
#[derive(Debug)]
pub struct AlignedSeries<T: SampleValue> {
    pub start_ts: TimeStamp,
    pub interval: Duration,
    pub values: Vec<Sample<T>>,
}

impl<T: SampleValue> AlignedSeries<T> {
    /// Create a new empty series.
    pub fn new(interval: Duration, start_ts: TimeStamp) -> Self {
        Self {
            interval,
            start_ts,
            values: vec![],
        }
    }

    /// Create a new aligned series from a raw series. The raw series is
    /// aggregated into windows of the given interval.
    pub fn from_raw_series(
        series: &RawSeries<T>,
        interval: Duration,
        start_ts: TimeStamp,
        end_ts: Option<TimeStamp>,
        op: WindowOp<T>,
    ) -> anyhow::Result<Self> {
        if let Some(end_ts) = end_ts {
            if end_ts < start_ts {
                anyhow::bail!("end_ts must be greater than or equal to start_ts");
            }
        }

        let mut aligned_series = Self::new(interval, start_ts);
        let mut window_iter = series.windows(interval, start_ts);
        let mut current_ts = start_ts.millis();

        for w in window_iter.samples().aggregate(op) {
            if let Some(end_ts) = end_ts {
                if current_ts > end_ts.millis() {
                    break;
                }
            }

            aligned_series.push_sample(w);
            current_ts += interval.millis();
        }

        Ok(aligned_series)
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
    pub fn at_or_after(&self, ts: TimeStamp) -> Option<Element<T>> {
        if ts <= self.start_ts {
            if self.is_empty() {
                return None;
            } else {
                return Some((self.start_ts, self.values[0]).into());
            }
        }

        if (ts - self.start_ts).millis() % self.interval.millis() == 0 {
            let index = ((ts - self.start_ts).millis() / self.interval.millis()) as usize;
            if index < self.values.len() {
                return Some((ts, self.values[index]).into());
            }
        } else {
            let index = ((ts - self.start_ts).millis() / self.interval.millis()) as usize + 1;
            if index < self.values.len() {
                return Some(
                    (
                        self.start_ts.millis() + (index as i64 * self.interval.millis()),
                        self.values[index],
                    )
                        .into(),
                );
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
                ts_to_utc(self.start_ts.millis() + (i as i64 * self.interval.millis())),
                sample
            )?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {

    use crate::{ops::sum, sample::SampleEquals};

    use super::*;

    #[test]
    fn nearest_after() {
        let mut series = RawSeries::new();
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
        series.push(0, 0);
        series.push(200, 1);
        series.push(350, 2);
        series.push(500, 3);
        series.push(1023, 4);
        series.push(3044, 5);
        series.push(4033, 6);
        series.push(9000, 7);

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

    #[test]
    fn aligned_series() {
        let mut series = AlignedSeries::new(Duration(100), TimeStamp(1000));
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

        assert_eq!(series.at_or_after(TimeStamp(0)).unwrap().0, 1000.into());
        assert!(series
            .at_or_after(TimeStamp(0))
            .unwrap()
            .1
            .equals(&Sample::point(0)));

        assert_eq!(series.at_or_after(TimeStamp(999)).unwrap().0, 1000.into());
        assert!(series
            .at_or_after(TimeStamp(999))
            .unwrap()
            .1
            .equals(&Sample::point(0)));

        assert_eq!(series.at_or_after(TimeStamp(1000)).unwrap().0, 1000.into());
        assert!(series
            .at_or_after(TimeStamp(1000))
            .unwrap()
            .1
            .equals(&Sample::point(0)));

        assert_eq!(series.at_or_after(TimeStamp(1010)).unwrap().0, 1100.into());
        assert!(series
            .at_or_after(TimeStamp(1010))
            .unwrap()
            .1
            .equals(&Sample::point(1)));

        assert_eq!(series.at_or_after(TimeStamp(1100)).unwrap().0, 1100.into());
        assert!(series
            .at_or_after(TimeStamp(1100))
            .unwrap()
            .1
            .equals(&Sample::point(1)));

        assert_eq!(series.at_or_after(TimeStamp(1900)).unwrap().0, 1900.into());
        assert!(series.at_or_after(TimeStamp(1910)).is_none());
    }

    #[test]
    fn to_aligned_series() {
        let mut series = RawSeries::new();
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

        for e in series.windows(Duration(5), TimeStamp(0)) {
            println!("w: {:?}", e);
        }

        for e in series.windows(Duration(5), TimeStamp(0)).samples() {
            println!("e: {:?}", e);
        }

        let aligned_series =
            AlignedSeries::from_raw_series(&series, Duration(5), TimeStamp(0), None, sum);

        println!("aligned_series: {}\n\n", aligned_series.unwrap());
    }
}

use core::fmt;
use std::time::Duration;

use num_traits::Zero;

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

#[derive(Debug)]
pub enum Sample<T> {
    Err,
    Zero,
    Point(T),
}

impl<T: Zero + Copy> Sample<T> {
    /// Create a new sample with the given millisecond timestamp.
    pub fn point(value: T) -> Self {
        Self::Point(value)
    }

    pub fn zero() -> Self {
        Self::Zero
    }

    pub fn is_err(&self) -> bool {
        matches!(self, Self::Err)
    }

    pub fn val(&self) -> T {
        match self {
            Self::Err => T::zero(),
            Self::Zero => T::zero(),
            Self::Point(v) => *v,
        }
    }
}

impl<T: fmt::Display + Zero> fmt::Display for Sample<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Err => write!(f, "Err"),
            Self::Zero => write!(f, "Zero({})", T::zero()),
            Self::Point(v) => write!(f, "Point({})", v),
        }
    }
}

pub struct RawConfig {
    pub tz: String,
}

pub enum SeriesValues<T> {
    Aligned(i64, i64, Vec<Sample<T>>), // start_ts, interval, samples
    Unaligned(Vec<(i64, Sample<T>)>),  // vec<(ts, sample)>
}

pub struct Series<T> {
    pub values: SeriesValues<T>,
}

type WindowVec = Vec<Option<(usize, usize)>>;

impl<T: Zero + Copy> Series<T> {
    pub fn new() -> Self {
        Self {
            values: SeriesValues::Unaligned(Vec::new()),
        }
    }

    pub fn new_aligned(start_ts: i64, interval: i64) -> Self {
        Self {
            values: SeriesValues::Aligned(start_ts, interval, Vec::new()),
        }
    }

    pub fn last_val(&self) -> T {
        match &self.values {
            SeriesValues::Aligned(_, _, samples) => samples.last().unwrap_or(&Sample::zero()).val(),
            SeriesValues::Unaligned(samples) => {
                samples.last().unwrap_or(&(0, Sample::zero())).1.val()
            }
        }
    }

    /// Add a new sample to the series. The timestamp must be greater than the
    /// last sample's timestamp.
    pub fn push(&mut self, value: T) {
        self.push_sample(Sample::point(value), None)
    }

    pub fn push_sample(&mut self, sample: Sample<T>, ts: Option<i64>) {
        match &mut self.values {
            SeriesValues::Aligned(_, _, samples) => {
                assert_eq!(ts, None);
                samples.push(sample);
            }
            SeriesValues::Unaligned(samples) => {
                assert!(ts.is_some());
                samples.push((ts.unwrap(), sample));
            }
        }
    }

    pub fn len(&self) -> usize {
        match &self.values {
            SeriesValues::Aligned(_, _, samples) => samples.len(),
            SeriesValues::Unaligned(samples) => samples.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        match &self.values {
            SeriesValues::Aligned(_, _, samples) => samples.is_empty(),
            SeriesValues::Unaligned(samples) => samples.is_empty(),
        }
    }

    pub fn values(&self) -> Vec<(i64, T)> {
        match &self.values {
            SeriesValues::Aligned(start_ts, interval, samples) => {
                let mut values = Vec::new();
                let mut ts = *start_ts;
                for sample in samples {
                    values.push((ts, sample.val()));
                    ts += *interval;
                }
                values
            }
            SeriesValues::Unaligned(samples) => {
                let mut values = Vec::new();
                for (ts, sample) in samples {
                    values.push((*ts, sample.val()));
                }
                values
            }
        }
    }

    pub fn get(&self, index: usize) -> Option<(i64, T)> {
        match &self.values {
            SeriesValues::Aligned(start_ts, interval, samples) => {
                let ts = *start_ts + (*interval * index as i64);
                samples.get(index).map(|s| (ts, s.val()))
            }
            SeriesValues::Unaligned(samples) => samples.get(index).map(|s| (s.0, s.1.val())),
        }
    }

    pub fn windows(&self, window_size: Duration, start_ts: i64) -> WindowVec {
        if self.is_empty() {
            return Vec::new();
        }

        let last_sample_ts = self.values().last().unwrap().0;
        if last_sample_ts < start_ts {
            return Vec::new();
        }

        let num_windows = ((last_sample_ts - start_ts) / window_size.as_millis() as i64) + 1;
        let mut windows = Vec::with_capacity(num_windows as usize);

        let values = self.values();
        let mut last_index = 0;

        for i in 0..num_windows {
            let window_start_ts = start_ts + (i * window_size.as_millis() as i64);
            let window_end_ts = window_start_ts + window_size.as_millis() as i64;

            let mut start_index = Some(last_index);
            let mut end_index = None;

            for (j, sample) in values.iter().enumerate().skip(last_index) {
                if sample.0 >= window_start_ts && sample.0 < (window_end_ts - 1) {
                    start_index = Some(j);
                    break;
                }
            }

            if let Some(start_index) = start_index {
                for (j, sample) in values.iter().enumerate().skip(start_index) {
                    if sample.0 >= (window_end_ts - 1) {
                        end_index = Some(j - 1);
                        break;
                    }
                }
            }

            if let Some(start_index) = start_index {
                if let Some(end_index) = end_index {
                    if end_index < start_index {
                        // No samples in this window
                        windows.push(None);
                    } else {
                        windows.push(Some((start_index, end_index)));
                    }
                    last_index = end_index + 1;
                    continue;
                } else {
                    // Last window
                    windows.push(Some((start_index, values.len() - 1)));
                    break;
                }
            }

            unreachable!()
        }

        windows
    }
}

impl<T: Zero + Copy> Default for Series<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T: fmt::Display + Zero + Copy> fmt::Display for Series<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for sample in self.values().iter() {
            write!(f, "\n {} {}", ts_to_utc(sample.0), sample.1)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use chrono::{TimeZone, Utc};

    use super::*;

    fn assert_window_sizes(w: &WindowVec, len: usize, window_size: usize) {
        assert_eq!(w.len(), len, "incorrect number of windows");
        for (i, r) in w.iter().enumerate() {
            if let Some((start, end)) = r {
                assert_eq!(
                    end - start,
                    window_size - 1,
                    "incorrect window size for {}",
                    i
                );
            } else {
                println!("Window {}: None", i);
            }
        }
    }

    #[test]
    fn windowing() {
        let mut s = Series::new();

        // Make a 10 minute series with 10 second intervals
        let mut c = 0;
        for i in 0..10 {
            for j in 0..6 {
                s.push_sample(
                    Sample::point(c),
                    Some(
                        Utc.with_ymd_and_hms(2023, 1, 1, 1, i, j * 10)
                            .unwrap()
                            .timestamp_millis(),
                    ),
                );
                c += 1;
            }
        }

        // Break it into 1 minute windows
        let windows = s.windows(
            Duration::from_secs(60),
            Utc.with_ymd_and_hms(2023, 1, 1, 1, 0, 0)
                .unwrap()
                .timestamp_millis(),
        );

        // Expect 10 windows with 6 samples each
        assert_window_sizes(&windows, 10, 6);

        // Break it into 2 minute windows
        let windows = s.windows(
            Duration::from_secs(120),
            Utc.with_ymd_and_hms(2023, 1, 1, 1, 0, 0)
                .unwrap()
                .timestamp_millis(),
        );

        assert_window_sizes(&windows, 5, 12);

        // Break it into 30 second windows
        let windows = s.windows(
            Duration::from_secs(30),
            Utc.with_ymd_and_hms(2023, 1, 1, 1, 0, 0)
                .unwrap()
                .timestamp_millis(),
        );

        assert_window_sizes(&windows, 20, 3);

        // Break it into 2 second windows
        let windows = s.windows(
            Duration::from_secs(2),
            Utc.with_ymd_and_hms(2023, 1, 1, 1, 0, 0)
                .unwrap()
                .timestamp_millis(),
        );

        println!("{} - {:?}", windows.len(), windows);
    }
}

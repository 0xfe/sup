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
    Zero, // Reset
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

pub enum Window {
    Empty,
    Range(usize, usize),
}

impl Window {
    pub fn is_empty(&self) -> bool {
        matches!(self, Self::Empty)
    }

    pub fn is_range(&self) -> bool {
        matches!(self, Self::Range(_, _))
    }
}

pub struct RawConfig {
    pub tz: String,
}

pub struct Series<T> {
    pub values: Vec<(i64, Sample<T>)>,
}

impl<T: Zero + Copy> Series<T> {
    pub fn new() -> Self {
        Self { values: vec![] }
    }

    pub fn last_val(&self) -> T {
        self.values.last().unwrap_or(&(0, Sample::zero())).1.val()
    }

    /// Add a new sample to the series. The timestamp must be greater than the
    /// last sample's timestamp.
    pub fn push(&mut self, ts: i64, value: T) {
        self.push_sample(ts, Sample::point(value))
    }

    pub fn push_sample(&mut self, ts: i64, sample: Sample<T>) {
        self.values.push((ts, sample));
    }

    pub fn len(&self) -> usize {
        self.values.len()
    }

    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }

    pub fn get(&self, index: usize) -> Option<(i64, T)> {
        self.values.get(index).map(|s| (s.0, s.1.val()))
    }

    pub fn windows(&self, window_size: Duration, start_ts: i64) -> Vec<Window> {
        if self.is_empty() {
            return Vec::new();
        }

        let last_sample_ts = self.values.last().unwrap().0;
        if last_sample_ts < start_ts {
            return Vec::new();
        }

        let num_windows = ((last_sample_ts - start_ts) / window_size.as_millis() as i64) + 1;
        let mut windows = Vec::with_capacity(num_windows as usize);

        let values = &self.values;
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
                        windows.push(Window::Empty);
                    } else {
                        windows.push(Window::Range(start_index, end_index));
                    }
                    last_index = end_index + 1;
                    continue;
                } else {
                    // Last window
                    windows.push(Window::Range(start_index, values.len() - 1));
                    break;
                }
            }

            unreachable!()
        }

        windows
    }

    pub fn windows_iter(&self, window_size: Duration, start_ts: i64) -> WindowIter<T> {
        WindowIter::new(self, window_size, start_ts)
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

pub struct WindowIter<'a, T> {
    series: &'a Series<T>,
    window_size: Duration,
    start_ts: i64,
    num_windows: usize,
    current_window: usize,
    last_index: usize,
}

impl<'a, T> WindowIter<'a, T> {
    pub fn new(series: &'a Series<T>, window_size: Duration, start_ts: i64) -> Self {
        let last_sample_ts = series.values.last().unwrap().0;
        let mut num_windows = ((last_sample_ts - start_ts) / window_size.as_millis() as i64) + 1;

        if last_sample_ts < start_ts {
            num_windows = 0;
        }

        Self {
            series,
            window_size,
            start_ts,
            num_windows: num_windows as usize,
            current_window: 0,
            last_index: 0,
        }
    }
}

impl<'a, T> Iterator for WindowIter<'a, T> {
    type Item = Window;

    fn next(&mut self) -> Option<Self::Item> {
        if self.current_window >= self.num_windows {
            return None;
        }

        let window_start_ts =
            self.start_ts + (self.current_window as i64 * self.window_size.as_millis() as i64);
        let window_end_ts = window_start_ts + self.window_size.as_millis() as i64;

        let mut start_index = Some(self.last_index);
        let mut end_index = None;

        for (j, sample) in self.series.values.iter().enumerate().skip(self.last_index) {
            if sample.0 >= window_start_ts && sample.0 < (window_end_ts - 1) {
                start_index = Some(j);
                break;
            }
        }

        if let Some(start_index) = start_index {
            for (j, sample) in self.series.values.iter().enumerate().skip(start_index) {
                if sample.0 >= (window_end_ts - 1) {
                    end_index = Some(j - 1);
                    break;
                }
            }
        }

        self.current_window += 1;
        if let Some(start_index) = start_index {
            if let Some(end_index) = end_index {
                if end_index < start_index {
                    // No samples in this window
                    self.last_index += 1;
                    return Some(Window::Empty);
                } else {
                    self.last_index = end_index + 1;
                    return Some(Window::Range(start_index, end_index));
                }
            } else {
                // Last window
                return Some(Window::Range(start_index, self.series.values.len() - 1));
            }
        }

        unreachable!()
    }
}

#[cfg(test)]
mod tests {
    use chrono::{TimeZone, Utc};

    use super::*;

    fn assert_window_sizes(w: &[Window], len: usize, window_size: usize) {
        assert_eq!(w.len(), len, "incorrect number of windows");
        for (i, r) in w.iter().enumerate() {
            assert!(r.is_range(), "missing window for {}", i);
            if let Window::Range(start, end) = r {
                assert_eq!(
                    end - start,
                    window_size - 1,
                    "incorrect window size for {}",
                    i
                );
            }
        }
    }

    fn assert_every_nth(w: &[Window], n: usize, window_size: Option<usize>) {
        for (i, r) in w.iter().enumerate() {
            if i % n == 0 {
                if window_size.is_none() {
                    assert!(r.is_empty());
                    continue;
                }

                if window_size.is_some() {
                    assert!(r.is_range());
                }

                if let Window::Range(start, end) = r {
                    if let Some(window_size) = window_size {
                        assert_eq!(
                            end - start,
                            window_size - 1,
                            "incorrect window size for {}",
                            i
                        );
                    }
                }
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
                    Utc.with_ymd_and_hms(2023, 1, 1, 1, i, j * 10)
                        .unwrap()
                        .timestamp_millis(),
                    Sample::point(c),
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

        assert_every_nth(&windows, 5, Some(1));
    }

    #[test]
    fn windowing_iterator() {
        let mut s = Series::new();

        // Make a 10 minute series with 10 second intervals
        let mut c = 0;
        for i in 0..10 {
            for j in 0..6 {
                s.push_sample(
                    Utc.with_ymd_and_hms(2023, 1, 1, 1, i, j * 10)
                        .unwrap()
                        .timestamp_millis(),
                    Sample::point(c),
                );
                c += 1;
            }
        }

        let iter = s.windows_iter(
            Duration::from_secs(60),
            Utc.with_ymd_and_hms(2023, 1, 1, 1, 0, 0)
                .unwrap()
                .timestamp_millis(),
        );

        for w in iter {
            match w {
                Window::Range(start, end) => {
                    assert_eq!(end - start, 5);
                }
                Window::Empty => {
                    panic!();
                }
            }
        }
    }
}

use std::time::Duration;

use crate::{sample::SampleValue, series::Series};

/// A window is either empty or a range of indices into a series.
#[derive(Debug, Clone)]
pub enum Window {
    Empty,
    Range(usize, usize),
}

impl Window {
    /// Returns true if the window is empty.
    pub fn is_empty(&self) -> bool {
        matches!(self, Self::Empty)
    }

    /// Returns true if the window is a range.
    pub fn is_range(&self) -> bool {
        matches!(self, Self::Range(_, _))
    }
}

#[derive(Clone)]
/// An iterator over windows of a series.
pub struct WindowIter<'a, T: SampleValue> {
    /// The series to iterate over.
    series: &'a Series<T>,

    /// The size of each window.
    window_size: Duration,

    /// The timestamp of the first window.
    start_ts: i64,

    /// The number of windows.
    num_windows: usize,

    /// The index of the current iterated window
    current_window: usize,

    /// The index of the last sample returned.
    last_index: usize,

    /// Next value
    next: Option<Window>,
}

impl<'a, T: SampleValue> WindowIter<'a, T> {
    /// Create a new window iterator.
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
            next: None,
        }
    }

    pub fn aggregate(&'a mut self, f: fn(&[T]) -> T) -> WindowAggregator<'a, T> {
        WindowAggregator { iter: self, f }
    }
}

impl<'a, T: SampleValue> Iterator for WindowIter<'a, T> {
    type Item = Window;

    /// Returns the next window.
    fn next(&mut self) -> Option<Self::Item> {
        if self.current_window >= self.num_windows {
            self.next = None;
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
                    self.next = Some(Window::Empty);
                } else {
                    self.last_index = end_index + 1;
                    self.next = Some(Window::Range(start_index, end_index));
                }
            } else {
                // Last window
                self.next = Some(Window::Range(start_index, self.series.values.len() - 1));
            }
        }

        self.next.clone()
    }
}

pub struct WindowAggregator<'a, T: SampleValue> {
    iter: &'a mut WindowIter<'a, T>,
    f: fn(&[T]) -> T,
}

impl<'a, T> Iterator for WindowAggregator<'a, T>
where
    T: SampleValue,
{
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next().map(|w| match w {
            Window::Empty => T::zero(),
            Window::Range(start, end) => {
                let values = self.iter.series.values[start..=end]
                    .iter()
                    .map(|(_, s)| s.val())
                    .collect::<Vec<T>>();
                (self.f)(&values)
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use chrono::{TimeZone, Utc};

    use crate::{
        sample::Sample,
        window_ops::{max, mean, min},
    };

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
        let windows = s
            .windows_iter(
                Duration::from_secs(60),
                Utc.with_ymd_and_hms(2023, 1, 1, 1, 0, 0)
                    .unwrap()
                    .timestamp_millis(),
            )
            .collect::<Vec<Window>>();

        // Expect 10 windows with 6 samples each
        assert_window_sizes(&windows, 10, 6);

        // Break it into 2 minute windows
        let windows = s
            .windows_iter(
                Duration::from_secs(120),
                Utc.with_ymd_and_hms(2023, 1, 1, 1, 0, 0)
                    .unwrap()
                    .timestamp_millis(),
            )
            .collect::<Vec<Window>>();

        assert_window_sizes(&windows, 5, 12);

        // Break it into 30 second windows
        let windows = s
            .windows_iter(
                Duration::from_secs(30),
                Utc.with_ymd_and_hms(2023, 1, 1, 1, 0, 0)
                    .unwrap()
                    .timestamp_millis(),
            )
            .collect::<Vec<Window>>();

        assert_window_sizes(&windows, 20, 3);

        // Break it into 2 second windows
        let windows = s
            .windows_iter(
                Duration::from_secs(2),
                Utc.with_ymd_and_hms(2023, 1, 1, 1, 0, 0)
                    .unwrap()
                    .timestamp_millis(),
            )
            .collect::<Vec<Window>>();

        println!("{:?}", windows);
        assert_every_nth(&windows, 5, Some(1));
    }

    #[test]
    fn aggregation() {
        let mut s = Series::new();

        // Make a 10 minute series with 10 second intervals
        let mut c = 0.0;
        for i in 0..10 {
            for j in 0..6 {
                s.push_sample(
                    Utc.with_ymd_and_hms(2023, 1, 1, 1, i, j * 10)
                        .unwrap()
                        .timestamp_millis(),
                    Sample::point(c),
                );
                c += 1.0;
            }
        }

        // Break it into 1 minute windows
        let windows = s.windows_iter(
            Duration::from_secs(60),
            Utc.with_ymd_and_hms(2023, 1, 1, 1, 0, 0)
                .unwrap()
                .timestamp_millis(),
        );

        for i in windows.clone().aggregate(max) {
            println!("{:?}", i);
        }

        for i in windows.clone().aggregate(min) {
            println!("{:?}", i);
        }

        for i in windows.clone().aggregate(mean) {
            println!("{:?}", i);
        }
    }
}

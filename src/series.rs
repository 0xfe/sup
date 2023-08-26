use std::{fmt, time::Duration};

use num_traits::Zero;

use crate::{sample::Sample, util::ts_to_utc, window::WindowIter};

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

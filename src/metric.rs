use std::collections::{BTreeMap, HashMap};

use crate::{sample::{SampleValue, SampleValueOp}, AlignedSeries, Interval, RawSeries, TimeStamp, ops};
use derive_more::{Display, From, Into};

#[repr(transparent)]
#[derive(From, Into, Debug, PartialEq, Eq, Clone)]
pub struct TagName(pub String);

#[derive(Debug, Display, Hash, Clone)]
pub enum TagValue {
    String(String),
    Int(i64),
}

pub struct Metric<T: SampleValue> {
    pub name: String,
    pub tags: Vec<(TagName, TagValue)>,
    pub stream: Stream<T>,
}

impl<T: SampleValueOp<T>> Metric<T> {
    pub fn new(name: String) -> Self {
        Self {
            name,
            tags: vec![],
            stream: Stream::new(),
        }
    }

    pub fn add_tag(&mut self, name: TagName, value: TagValue) {
        self.tags.push((name, value));
    }

    pub fn push_raw(&mut self, ts: TimeStamp, value: T) {
        self.stream.push_raw(ts, value);
    }
}

pub struct DownSampler {
    pub id: String, // raw, 1m, 5m, 1h, 24h, 7d
    pub interval: Interval,
    pub ops: Vec<String>,
}

// downsample string: [1m, 5m, 1h, 24h, 7d] [min, max, mean, rate]
// maybe: min-1m, mean-5m, rate-5m

// for any counter metric:
//  - raw: youngest + delta + min-1m, youngest + delta + max-1m, youngest + delta + rate-1m
//  - 1m: min-5m, max-5m, mean-5m, rate-5m
//  - 5m: min-1h, max-1h, mean-1h, rate-1h
//
// for gauge metrics:
//  - raw: min-1m, max-1m, mean-1m, sum-1m
//  - 1m: min-5m, max-5m, mean-5m, sum-5m
//  - 5m: min-1h, max-1h, mean-1h, sum-1h

// like prometheus: rate(metric[5m])

pub struct DownSampleConfigs {
    pub metric: String,
    pub tags: Vec<(TagName, TagValue)>, // maybe ignore for now
}

pub struct Stream<T: SampleValue> {
    pub raw: Vec<RawSeries<T>>,
    pub aligned: HashMap<Interval, BTreeMap<TimeStamp, AlignedSeries<T>>>,
}

impl<T: SampleValueOp<T>> Stream<T> {
    pub fn new() -> Self {
        Self {
            raw: vec![],
            aligned: HashMap::new(),
        }
    }

    pub fn add_raw_series(&mut self, series: RawSeries<T>) {
        self.raw.push(series);
    }

    pub fn new_interval(&mut self, interval: Interval, start_ts: TimeStamp) {
        self.aligned
            .entry(interval)
            .or_insert_with(BTreeMap::new)
            .insert(start_ts, AlignedSeries::new(interval, start_ts));
    }

    pub fn push_raw(&mut self, ts: TimeStamp, value: T) {
        if self.raw.is_empty() {
            self.add_raw_series(RawSeries::new());
        }

        self.raw.last_mut().unwrap().push(ts, value);
    }

    pub fn align(&mut self, interval: Interval, start_ts: TimeStamp, end_ts: Option<TimeStamp>) {
        if self.raw.is_empty() {
            return;
        }

        let raw_series = self.raw.last().unwrap();
        let aligned_series = AlignedSeries::from_raw_series(
            raw_series,
            interval,
            start_ts,
            end_ts,
            crate::ops::element::youngest,
        )
        .unwrap();

        let deltas = aligned_series.sliding_aggregate(2, ops::sample::delta).unwrap();

        self.aligned
            .entry(interval)
            .or_insert_with(BTreeMap::new)
            .insert(start_ts, deltas);
    }
}

impl<T: SampleValueOp<T>> Default for Stream<T> {
    fn default() -> Self {
        Self::new()
    }
}

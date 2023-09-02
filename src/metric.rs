use std::collections::{BTreeMap, HashMap};

use crate::{sample::SampleValue, AlignedSeries, Interval, RawSeries, TimeStamp};
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
    pub series: Stream<T>,
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

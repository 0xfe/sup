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

pub struct Stream<T: SampleValue> {
    pub raw: Vec<RawSeries<T>>,
    pub aligned: HashMap<Interval, BTreeMap<TimeStamp, AlignedSeries<T>>>,
}

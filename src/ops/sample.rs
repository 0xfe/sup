use std::ops::Sub;

use crate::sample::{Sample, SampleValue};

pub type Op<T> = fn(&[Sample<T>]) -> Sample<T>;

pub fn delta<T: SampleValue + Sub<Output = T>>(values: &[Sample<T>]) -> Sample<T> {
    // TODO: check for Zero point
    if values.len() != 2 {
        Sample::Err
    } else {
        let last = values.last().unwrap().val();
        let prev = values.first().unwrap().val();

        if last > prev {
            Sample::Point(last - prev)
        } else {
            // TODO: this should be last from Zero
            Sample::Point(last)
        }
    }
}

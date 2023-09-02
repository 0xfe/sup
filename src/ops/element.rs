use std::ops::{Div, Sub};

use num_traits::NumCast;

use crate::{
    element::Element,
    sample::{Sample, SampleValue},
};

pub type Op<T> = fn(&[Element<T>]) -> Sample<T>;

pub fn from_str<T>(op: &str) -> Option<Op<T>>
where
    T: SampleValue + NumCast + Div<Output = T> + Sub<Output = T>,
{
    match op {
        "max" => Some(max),
        "min" => Some(min),
        "sum" => Some(sum),
        "mean" => Some(mean),
        "oldest" => Some(oldest),
        "youngest" => Some(youngest),
        "delta" => Some(delta),
        _ => None,
    }
}

pub fn max<T: SampleValue>(values: &[Element<T>]) -> Sample<T> {
    let mut max = Sample::Err;
    let mut has_fake = false;

    for elem in values.iter() {
        match elem.1 {
            Sample::Point(v) => {
                if v > max.val() {
                    max = Sample::Point(v);
                }
            }
            Sample::Fake(v) => {
                has_fake = true;
                if v > max.val() {
                    max = Sample::Fake(v);
                }
            }
            Sample::Zero => {
                if T::zero() > max.val() {
                    max = Sample::Point(T::zero());
                }
            }
            Sample::Err => {}
        }
    }

    if has_fake {
        Sample::Fake(max.val())
    } else {
        Sample::Point(max.val())
    }
}

pub fn min<T: SampleValue>(values: &[Element<T>]) -> Sample<T> {
    let mut min = Sample::Err;
    let mut has_fake = false;

    for elem in values.iter() {
        match elem.1 {
            Sample::Point(v) => {
                if v < min.val() {
                    min = Sample::Point(v);
                }
            }
            Sample::Fake(v) => {
                has_fake = true;
                if v < min.val() {
                    min = Sample::Fake(v);
                }
            }
            Sample::Zero => {
                if T::zero() < min.val() {
                    min = Sample::Point(T::zero());
                }
            }
            Sample::Err => {}
        }
    }

    if has_fake {
        Sample::Fake(min.val())
    } else {
        Sample::Point(min.val())
    }
}

pub fn sum<T: SampleValue>(values: &[Element<T>]) -> Sample<T> {
    let mut sum = T::zero();

    for elem in values.iter() {
        sum = sum + elem.1.val();
    }

    Sample::Point(sum)
}

pub fn mean<T: SampleValue + NumCast + Div<Output = T>>(values: &[Element<T>]) -> Sample<T> {
    let mut sum = T::zero();

    for elem in values.iter() {
        sum = sum + elem.1.val();
    }

    Sample::Point(sum / T::from(values.len()).unwrap())
}

pub fn oldest<T: SampleValue>(values: &[Element<T>]) -> Sample<T> {
    if values.is_empty() {
        Sample::Err
    } else {
        values[0].1
    }
}

pub fn youngest<T: SampleValue>(values: &[Element<T>]) -> Sample<T> {
    if values.is_empty() {
        Sample::Err
    } else {
        values[values.len() - 1].1
    }
}

pub fn delta<T: SampleValue + Sub<Output = T>>(values: &[Element<T>]) -> Sample<T> {
    // TODO: check for Zero point
    if values.len() != 2 {
        Sample::Err
    } else {
        let last = values.last().unwrap().1.val();
        let prev = values.first().unwrap().1.val();

        if last > prev {
            Sample::Point(last - prev)
        } else {
            // TODO: this should be last from Zero
            Sample::Point(last)
        }
    }
}

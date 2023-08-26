use std::ops::Div;

use num_traits::NumCast;

use crate::{
    element::Element,
    sample::{Sample, SampleValue},
};

pub type Op<T> = fn(&[Element<T>]) -> Sample<T>;

pub fn max<T: SampleValue>(values: &[Element<T>]) -> Sample<T> {
    let mut max = Sample::Err;

    for elem in values.iter() {
        match elem.1 {
            Sample::Point(v) => {
                if v > max.val() {
                    max = Sample::Point(v);
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

    max
}

pub fn min<T: SampleValue>(values: &[Element<T>]) -> Sample<T> {
    let mut min = Sample::Err;

    for elem in values.iter() {
        match elem.1 {
            Sample::Point(v) => {
                if v < min.val() {
                    min = Sample::Point(v);
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

    min
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

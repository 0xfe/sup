use std::ops::Div;

use num_traits::NumCast;

use crate::sample::SampleValue;

pub type Op<T> = fn(&[T]) -> T;

pub fn max<T: SampleValue>(values: &[T]) -> T {
    let mut max = T::zero();
    (0..values.len()).for_each(|i| {
        let val = values[i];
        if val > max {
            max = val;
        }
    });
    max
}

pub fn min<T: SampleValue>(values: &[T]) -> T {
    if values.is_empty() {
        return T::zero();
    }

    let mut min = values[0];
    (0..values.len()).for_each(|i| {
        let val = values[i];
        if val < min {
            min = val;
        }
    });
    min
}

pub fn sum<T: SampleValue>(values: &[T]) -> T {
    let mut sum = T::zero();
    (0..values.len()).for_each(|i| {
        let val = values[i];
        sum = sum + val;
    });
    sum
}

pub fn mean<T: SampleValue + NumCast + Div<Output = T>>(values: &[T]) -> T {
    if values.is_empty() {
        return T::zero();
    }

    let mut sum = T::zero();
    (0..values.len()).for_each(|i| {
        let val = values[i];
        sum = sum + val;
    });

    sum / T::from(values.len()).unwrap()
}

pub fn oldest<T: SampleValue>(values: &[T]) -> T {
    values[0]
}

pub fn youngest<T: SampleValue>(values: &[T]) -> T {
    values[values.len() - 1]
}
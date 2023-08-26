use std::ops::Div;

use num_traits::NumCast;

use crate::sample::SampleValue;

pub fn max<T: SampleValue>(values: &[T]) -> T {
    let mut max = values[0];
    (1..values.len()).for_each(|i| {
        let val = values[i];
        if val > max {
            max = val;
        }
    });
    max
}

pub fn min<T: SampleValue>(values: &[T]) -> T {
    let mut min = values[0];
    (1..values.len()).for_each(|i| {
        let val = values[i];
        if val < min {
            min = val;
        }
    });
    min
}

pub fn sum<T: SampleValue>(values: &[T]) -> T {
    let mut sum = values[0];
    (1..values.len()).for_each(|i| {
        let val = values[i];
        sum = sum + val;
    });
    sum
}

pub fn mean<T: SampleValue + NumCast + Div<Output = T>>(values: &[T]) -> T {
    let mut sum = values[0];
    (1..values.len()).for_each(|i| {
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

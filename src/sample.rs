use num_traits::{Zero, NumCast};
use std::{fmt, ops::{Sub, Div}};

pub trait SampleValue: Zero + Copy + PartialEq + PartialOrd + NumCast + fmt::Display {}
pub trait SampleValueOp<T>: SampleValue + Div<Output=T> + Sub<Output = T> + Sized {}

impl SampleValue for i32 {}
impl SampleValue for i64 {}
impl SampleValue for i128 {}
impl SampleValue for f32 {}
impl SampleValue for f64 {}

impl SampleValueOp<i32> for i32 {}
impl SampleValueOp<i64> for i64 {}
impl SampleValueOp<i128> for i128 {}
impl SampleValueOp<f32> for f32 {}
impl SampleValueOp<f64> for f64 {}

pub trait SampleEquals {
    fn equals(&self, other: &Self) -> bool;
}

#[derive(Debug, Copy, Clone)]
pub enum Sample<T: SampleValue> {
    Err,
    Zero, // Reset
    Point(T),
    Fake(T), // Extrapolated values
}

impl<T: SampleValue> Sample<T> {
    /// Create a new sample with the given value.
    pub fn point(value: T) -> Self {
        Self::Point(value)
    }

    /// Create a zero-valued sample.
    pub fn zero() -> Self {
        Self::Zero
    }

    /// Returns true if the sample is an error.
    pub fn is_err(&self) -> bool {
        matches!(self, Self::Err)
    }

    /// Returns true if the sample is zero.
    pub fn is_zero(&self) -> bool {
        matches!(self, Self::Zero)
    }

    /// Returns a copy of the sample value.
    pub fn val(&self) -> T {
        match self {
            Self::Err => T::zero(),
            Self::Zero => T::zero(),
            Self::Point(v) => *v,
            Self::Fake(v) => *v,
        }
    }
}

impl<T: SampleValue> fmt::Display for Sample<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Err => write!(f, "Err"),
            Self::Zero => write!(f, "Zero({})", T::zero()),
            Self::Point(v) => write!(f, "Point({})", v),
            Self::Fake(v) => write!(f, "Fake({})", v),
        }
    }
}

impl SampleEquals for Sample<i32> {
    fn equals(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Err, Self::Err) => true,
            (Self::Zero, Self::Zero) => true,
            (Self::Point(v1), Self::Point(v2)) => v1 == v2,
            _ => false,
        }
    }
}

impl SampleEquals for Sample<i64> {
    fn equals(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Err, Self::Err) => true,
            (Self::Zero, Self::Zero) => true,
            (Self::Point(v1), Self::Point(v2)) => v1 == v2,
            _ => false,
        }
    }
}

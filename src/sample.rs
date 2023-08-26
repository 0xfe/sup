use num_traits::Zero;
use std::fmt;

pub trait SampleValue: Zero + Copy + PartialEq + fmt::Display {}

impl SampleValue for i32 {}
impl SampleValue for i64 {}
impl SampleValue for i128 {}
impl SampleValue for f32 {}
impl SampleValue for f64 {}

pub trait SampleEquals {
    fn equals(&self, other: &Self) -> bool;
}

#[derive(Debug, Copy, Clone)]
pub enum Sample<T: SampleValue> {
    Err,
    Zero, // Reset
    Point(T),
}

impl<T: SampleValue> Sample<T> {
    /// Create a new sample with the given millisecond timestamp.
    pub fn point(value: T) -> Self {
        Self::Point(value)
    }

    pub fn zero() -> Self {
        Self::Zero
    }

    pub fn is_err(&self) -> bool {
        matches!(self, Self::Err)
    }

    pub fn val(&self) -> T {
        match self {
            Self::Err => T::zero(),
            Self::Zero => T::zero(),
            Self::Point(v) => *v,
        }
    }
}

impl<T: SampleValue> fmt::Display for Sample<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Err => write!(f, "Err"),
            Self::Zero => write!(f, "Zero({})", T::zero()),
            Self::Point(v) => write!(f, "Point({})", v),
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

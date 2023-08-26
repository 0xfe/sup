use num_traits::Zero;
use std::fmt;

#[derive(Debug)]
pub enum Sample<T> {
    Err,
    Zero, // Reset
    Point(T),
}

impl<T: Zero + Copy> Sample<T> {
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

impl<T: fmt::Display + Zero> fmt::Display for Sample<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Err => write!(f, "Err"),
            Self::Zero => write!(f, "Zero({})", T::zero()),
            Self::Point(v) => write!(f, "Point({})", v),
        }
    }
}

pub mod cpu;
pub mod sample;
pub mod series;
pub mod util;
pub mod window;

#[cfg(test)]
mod tests {
    use super::*;
    use cpu::{cpu_usage, cpu_usage2};

    #[test]
    fn it_works() {
        cpu_usage();
        cpu_usage2();
    }
}

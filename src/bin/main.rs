use sup::{ops, AlignedSeries, RawSeries, TimeStamp};

fn main() {
    println!("Hello, world!");

    // Create a raw series
    let mut series = RawSeries::new();

    // Add values every 10ms
    for i in 1..=10 {
        series.push(TimeStamp::now(), 10 + i);
        std::thread::sleep(std::time::Duration::from_millis(10))
    }

    println!("{}", series);

    let series = AlignedSeries::from_raw_series(
        &series,
        sup::Interval(20),
        series.get(0).unwrap().0.align_millis(100),
        None,
        ops::element::youngest,
    )
    .unwrap();

    println!("\n\n{}", series);

    let deltas = series.sliding_window(2, ops::sample::delta);

    println!("{:?}", deltas)
}

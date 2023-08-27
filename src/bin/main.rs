use sup::{AlignedSeries, RawSeries, TimeStamp};

fn main() {
    println!("Hello, world!");

    // Create a raw series
    let mut series = RawSeries::new();

    // Add values every 10ms
    for _ in 1..=10 {
        series.push(TimeStamp::now(), 10);
        std::thread::sleep(std::time::Duration::from_millis(10))
    }

    println!("{}", series);

    // Align at 100ms boundary
    let start_ts = series.get(0).unwrap().0.millis();
    let remainder = start_ts % 100;

    let series = AlignedSeries::from_raw_series(
        &series,
        sup::Duration(50),
        (start_ts - remainder).into(),
        None,
        sup::ops::sum,
    )
    .unwrap();

    println!("\n\n{}", series);
}

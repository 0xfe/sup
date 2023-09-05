use sup::{metric::Metric, ops, AlignedSeries, RawSeries, TimeStamp};
use sysinfo::{CpuExt, CpuRefreshKind, RefreshKind, SystemExt};

fn main() {
    // Create a raw series
    let mut series = RawSeries::new();

    // Add values every 10ms
    for i in 1..=20 {
        series.push(TimeStamp::now(), 10 + i);
        std::thread::sleep(std::time::Duration::from_millis(10))
    }

    println!("Raw Series ({}): {}", series.len(), series);

    let series = AlignedSeries::from_raw_series(
        &series,
        sup::Interval(20),
        series.get(0).unwrap().0.align_millis(100),
        None,
        ops::element::youngest,
    )
    .unwrap();

    println!("\nAligned Series ({}): {}", series.len(), series);

    let deltas = series.sliding_aggregate(2, ops::sample::delta).unwrap();

    println!("\nDeltas ({}): {}", deltas.len(), deltas);

    let mut metric = Metric::new("cpu_usage".to_string());

    let mut usage =
        sysinfo::System::new_with_specifics(RefreshKind::new().with_cpu(CpuRefreshKind::new()));

    for _ in 0..10 {
        usage.refresh_cpu();
        for (i, cpu) in usage.cpus().iter().enumerate() {
            print!("{}: {:?}% ", i, cpu.cpu_usage());
        }

        metric.push_raw(TimeStamp::now(), usage.cpus().first().unwrap().cpu_usage());
        println!();
        std::thread::sleep(std::time::Duration::from_millis(300));
    }

    let stream = metric.stream.raw.first().unwrap();
    println!("usage ({}): {}", stream.len(), stream);
}

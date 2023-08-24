use sysinfo::{CpuExt, CpuRefreshKind, RefreshKind, SystemExt};

/// CPU usage from /proc/stat
pub fn cpu_usage() {
    let data = std::fs::read_to_string("/proc/stat").unwrap();
    let line = data.lines().next().unwrap();
    let fields = line.split_whitespace().collect::<Vec<_>>();

    let mut total = 0;

    for tm in fields.iter().skip(1) {
        let time = tm.parse::<usize>().unwrap();
        total += time;
    }

    let idle_tm = fields[4].parse::<usize>().unwrap();

    let usage = 100.0 * (1.0 - (idle_tm as f64 / total as f64));

    println!("CPU usage: {:.2}%", usage);
}

/// CPU usage from the sysinfo crate
pub fn cpu_usage2() {
    let mut usage =
        sysinfo::System::new_with_specifics(RefreshKind::new().with_cpu(CpuRefreshKind::new()));

    for _ in 0..5 {
        usage.refresh_cpu();
        for (i, cpu) in usage.cpus().iter().enumerate() {
            print!("{}: {:?}% ", i, cpu.cpu_usage());
        }
        println!();
        std::thread::sleep(std::time::Duration::from_millis(300));
    }
}

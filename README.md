# sup?

`sup` is an experimental in-memory time-series database written in Rust.

# DONE

- Base types: TimeStamp and Duration wrapping i64
  - No more than millisecond granularity needed
- Sample types
  - Zero/Reset types
  - Point<T>
- Unaligned TimeSeries
- Aligned TimeSeries
- Window Primitive and iterator
- Window Aggregation iterator
  - Ops: max, min, mean, oldest, youngest

# TODO

- Raw vs. Derived (w/ parent)
- Sliding windows
- Types:
  - Cumulative
  - Gauge
  - Distribution
- Aggregators (max, min, mean, etc.)
- Query language

# LICENSE

Copyright Mohit Muthanna Cheppudira 2023

"""
Measure how the warehouse design speeds up queries in ClickHouse.

Each pair runs the same question two ways:
  - naive:     table with no partitions and no sort key (reads everything)
  - optimized: partitioned, sorted fact table, or a pre-aggregated data mart

Results (median of several runs, rows read) are printed and saved to
benchmark_results.md, ready to paste into the README.

Usage (on your computer, with the docker stack running):
    pip install clickhouse-connect
    python scripts/benchmark.py
"""

import statistics
import time

import clickhouse_connect

RUNS = 5

PAIRS = [
    (
        "Count one hospital's operations in one month",
        """SELECT count() FROM dwh.fact_operations_naive
           WHERE hospital_id = 5 AND operation_date BETWEEN '2025-03-01' AND '2025-03-31'""",
        """SELECT count() FROM dwh.fact_operations
           WHERE hospital_id = 5 AND operation_date BETWEEN '2025-03-01' AND '2025-03-31'""",
    ),
    (
        "Daily operations per hospital in 2025",
        """SELECT hospital_id, operation_date, count() AS ops
           FROM dwh.fact_operations_naive
           WHERE operation_date >= '2025-01-01'
           GROUP BY hospital_id, operation_date""",
        """SELECT hospital_id, operation_date, sum(operations) AS ops
           FROM dwh.mart_daily_hospital
           WHERE operation_date >= '2025-01-01'
           GROUP BY hospital_id, operation_date""",
    ),
    (
        "Complication rate per operation type (all years)",
        """SELECT operation_name, countIf(has_complication) / count()
           FROM dwh.fact_operations_naive GROUP BY operation_name""",
        """SELECT operation_name, sum(complications) / sum(operations)
           FROM dwh.mart_daily_hospital GROUP BY operation_name""",
    ),
]


def run(client, sql):
    times, read_rows = [], 0
    for _ in range(RUNS):
        start = time.perf_counter()
        result = client.query(sql, settings={"use_query_cache": 0})
        times.append((time.perf_counter() - start) * 1000)
        read_rows = int(result.summary.get("read_rows", 0))
    return statistics.median(times), read_rows


def main():
    client = clickhouse_connect.get_client(
        host="localhost", port=8123, username="default", password="clickhouse"
    )
    lines = [
        "| Query | Naive: time | Naive: rows read | Optimized: time | Optimized: rows read | Speed-up |",
        "| --- | ---: | ---: | ---: | ---: | ---: |",
    ]
    for title, naive_sql, optimized_sql in PAIRS:
        naive_ms, naive_rows = run(client, naive_sql)
        opt_ms, opt_rows = run(client, optimized_sql)
        speedup = naive_ms / opt_ms if opt_ms else float("inf")
        lines.append(
            f"| {title} | {naive_ms:.0f} ms | {naive_rows:,} | {opt_ms:.0f} ms | {opt_rows:,} | {speedup:.1f}x |"
        )

    table = "\n".join(lines)
    print(table)
    with open("benchmark_results.md", "w") as f:
        f.write(table + "\n")
    print("\nSaved to benchmark_results.md")


if __name__ == "__main__":
    main()

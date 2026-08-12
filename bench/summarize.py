#!/usr/bin/env python3
"""Summarizes peer throughput benchmark CSV files.

    usage: summarize.py <results.csv> [baseline.csv]

With one file, prints the median of each scenario. With two, prints the second file's medians
next to the first file's and the relative change, so a buffer rework can be compared against a
baseline captured before it.
"""

import collections
import csv
import statistics
import sys


def load(path):
    scenarios = collections.OrderedDict()

    for row in csv.DictReader(open(path)):
        if row["closed_early"] == "1":
            continue
        scenarios.setdefault(row["label"], []).append(row)

    return collections.OrderedDict(
        (
            label,
            {
                "mb_per_s": statistics.median(float(r["mb_per_s"]) for r in rows),
                "ms_cpu_per_mb": statistics.median(float(r["cpu_s_per_mb"]) for r in rows) * 1000,
                "packets_per_s": statistics.median(float(r["packets_per_s"]) for r in rows),
                "peak_rss_mb": max(int(r["maxrss_kb"]) for r in rows) / 1024,
            },
        )
        for label, rows in scenarios.items()
    )


def main():
    if len(sys.argv) < 2:
        print(__doc__, file=sys.stderr)
        return 2

    current = load(sys.argv[1])
    baseline = load(sys.argv[2]) if len(sys.argv) > 2 else None

    if baseline is None:
        print(f'{"scenario":<16}{"MB/s":>12}{"ms CPU/MB":>12}{"pkts/s":>14}{"peak RSS MB":>13}')
        for label, m in current.items():
            print(f'{label:<16}{m["mb_per_s"]:>12.1f}{m["ms_cpu_per_mb"]:>12.3f}'
                  f'{m["packets_per_s"]:>14.0f}{m["peak_rss_mb"]:>13.1f}')
        return 0

    print(f'{"scenario":<16}{"base MB/s":>12}{"new MB/s":>12}{"change":>10}'
          f'{"base RSS":>10}{"new RSS":>10}')
    for label, m in current.items():
        b = baseline.get(label)
        if b is None:
            continue
        change = (m["mb_per_s"] / b["mb_per_s"] - 1) * 100 if b["mb_per_s"] else float("nan")
        print(f'{label:<16}{b["mb_per_s"]:>12.1f}{m["mb_per_s"]:>12.1f}{change:>9.1f}%'
              f'{b["peak_rss_mb"]:>10.1f}{m["peak_rss_mb"]:>10.1f}')

    return 0


if __name__ == "__main__":
    sys.exit(main())

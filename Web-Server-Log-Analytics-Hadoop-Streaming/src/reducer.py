#!/usr/bin/env python3
"""
reducer.py — Hadoop Streaming Reducer for Web Server Log Analytics

Receives sorted (category, key, count) triples from the mapper via stdin.
Aggregates counts per (category, key) pair.
Outputs three clearly separated sections:
  - IP ADDRESS COUNTS
  - URL COUNTS
  - STATUS CODE COUNTS

Input format (tab-separated):
  IP\t192.168.1.1\t1
  STATUS\t200\t1
  URL\t/index.html\t1

Output format:
  IP ADDRESS COUNTS
  192.168.1.1     4
  ...
  URL COUNTS
  /index.html     16
  ...
  STATUS CODE COUNTS
  200             35
  ...
"""

import sys


def main():
    counts = {}

    # Aggregate all category-key counts from stdin
    for raw_line in sys.stdin:
        line = raw_line.strip()
        if not line:
            continue

        parts = line.split('\t', 2)
        if len(parts) != 3:
            continue

        category, key, count_str = parts

        try:
            count = int(count_str)
        except ValueError:
            continue

        combo = (category, key)
        counts[combo] = counts.get(combo, 0) + count

    # ── Output Section 1: IP Address Counts ──────────────────────
    print("\n IP ADDRESS COUNTS ")
    print("-" * 30)
    for (category, key), total in sorted(counts.items()):
        if category == 'IP':
            print('%-25s %d' % (key, total))

    # ── Output Section 2: URL Counts ─────────────────────────────
    print("\n URL COUNTS ")
    print("-" * 30)
    for (category, key), total in sorted(counts.items()):
        if category == 'URL':
            print('%-25s %d' % (key, total))

    # ── Output Section 3: HTTP Status Code Counts ─────────────────
    print("\n STATUS CODE COUNTS ")
    print("-" * 30)
    for (category, key), total in sorted(counts.items()):
        if category == 'STATUS':
            print('%-25s %d' % (key, total))


if __name__ == '__main__':
    main()

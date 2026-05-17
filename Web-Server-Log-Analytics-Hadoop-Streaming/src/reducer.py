#!/usr/bin/env python3

import sys
def main():

    counts = {}

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

    print("\n IP ADDRESS COUNTS ")
    print("-" * 30)

    for (category, key), total in sorted(counts.items()):

        if category == 'IP':
            print('%-25s %d' % (key, total))

    print("\n URL COUNTS ")
    print("-" * 30)

    for (category, key), total in sorted(counts.items()):

        if category == 'URL':
            print('%-25s %d' % (key, total))

    print("\n STATUS CODE COUNTS ")
    print("-" * 30)

    for (category, key), total in sorted(counts.items()):

        if category == 'STATUS':
            print('%-25s %d' % (key, total))


if __name__ == '__main__':
    main()

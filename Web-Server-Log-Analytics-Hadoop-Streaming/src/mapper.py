#!/usr/bin/env python3

import sys
def parse_log_line(line):

    parts = line.split()

    try:
        ip = parts[0]
        url = parts[6]
        status = parts[8]

        return ip, url, status

    except IndexError:
        return None


def main():

    for raw_line in sys.stdin:

        line = raw_line.strip()

        if not line:
            continue

        result = parse_log_line(line)

        if result is None:
            continue

        ip, url, status = result

        print('IP\t%s\t%s' % (ip, 1))
        print('URL\t%s\t%s' % (url, 1))
        print('STATUS\t%s\t%s' % (status, 1))

if __name__ == '__main__':
    main()

#!/usr/bin/env python3
"""
mapper.py — Hadoop Streaming Mapper for Web Server Log Analytics

Reads Apache Combined Log Format lines from stdin.
Extracts three analytics dimensions per log entry:
  1. IP address  → for traffic source analysis
  2. URL         → for content popularity ranking
  3. HTTP status → for server health monitoring

Emits tab-separated triples: category \t key \t 1

Input format (Apache Combined Log):
  192.168.1.1 - - [01/Mar/2024:10:00:01 +0000] "GET /index.html HTTP/1.1" 200 512

Output format:
  IP\t192.168.1.1\t1
  URL\t/index.html\t1
  STATUS\t200\t1
"""

import sys


def parse_log_line(line):
    """
    Parse a single Apache log line and return (ip, url, status).
    Returns None if the line cannot be parsed.
    """
    parts = line.split()
    try:
        ip     = parts[0]
        url    = parts[6]      # URL is the 7th whitespace-separated field
        status = parts[8]      # HTTP status code is the 9th field
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

        # Emit one record per analytics dimension
        print('IP\t%s\t%s'     % (ip, 1))
        print('URL\t%s\t%s'    % (url, 1))
        print('STATUS\t%s\t%s' % (status, 1))


if __name__ == '__main__':
    main()

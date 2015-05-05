#!/usr/bin/env python

import sys


def read_input(f, separator='\t'):
    for line in f:
        # split the line into words
        yield line.rstrip().split(separator, 1)


def main(separator='\t'):
    data = read_input(sys.stdin)
    for u, v in data:
        key = sorted([int(i) for i in (u, v)])

        if key[0] == int(u):
            print("{}\t{}".format(*key))


if __name__ == '__main__':
    main()

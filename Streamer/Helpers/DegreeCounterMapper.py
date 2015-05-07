#!/usr/bin/env python

import sys

def read_input(f, separator='\t'):
    for line in f:
        # split the line into words
        yield line.rstrip().split(separator, 1)


def main(separator='\t'):
    data = read_input(sys.stdin)
    for _, v in data:
        print("{}\t{}".format(v,1))



if __name__ == '__main__':
    main()

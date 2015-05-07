#!/usr/bin/env python
"""
This is the 2nd mapper.
It takes a node and its neighbours.
Choose one of the neighbours and create an ordered tuple.
Then emit this couple and the list of neighbours.
"""

import sys


def read_input(f, separator='\t'):
    for line in f:
        # split the line into words
        yield line.rstrip().split(separator, 1)


def main(separator='\t'):
    data = read_input(sys.stdin)
    for node, neighbours in data:
        node = int(node)

        neighbours = neighbours.split(" ")
        neighbours = (node, tuple(int(n) for n in neighbours))

        for n in neighbours[1]:
            print("{}\t{}".format(sorted((node, n)), neighbours))

if __name__ == '__main__':
    main()

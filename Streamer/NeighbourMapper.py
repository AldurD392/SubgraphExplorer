#!/usr/bin/env python

import sys
from utils import read_input


def main(separator='\t'):
    """Emit the node and its neighbour."""
    data = read_input(sys.stdin)
    for u, v in data:
        print("{}\t{}".format(u, v))


if __name__ == '__main__':
    main()

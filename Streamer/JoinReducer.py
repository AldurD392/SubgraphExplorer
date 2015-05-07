#!/usr/bin/env python
"""A more advanced Reducer, using Python iterators and generators."""

from itertools import groupby
from operator import itemgetter
import sys


def read_mapper_output(file, separator='\t'):
    for line in file:
        yield line.rstrip().split(separator, 1)


def main(separator='\t'):
    data = read_mapper_output(sys.stdin, separator=separator)
    for key, value in groupby(data, itemgetter(0)):
        nodes = eval(key)

        neighbours_tuple = {eval(i[1]) for i in value}
        print("{}\t{}".format(nodes, neighbours_tuple))

if __name__ == "__main__":
    main()

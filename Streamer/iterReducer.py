#!/usr/bin/env python

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

        neighbours_set = set()


        for i in value:
            for t in eval(i[1]):
                neighbours_set.add(t)
                
        print("{}\t{}".format(nodes, neighbours_set))

if __name__ == "__main__":
    main()
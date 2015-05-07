#!/usr/bin/env python

from __future__ import print_function

from utils import read_input, density
from constants import RHO

from itertools import groupby
from operator import itemgetter
import sys


def main(separator='\t'):
    """
    Join the neighbours_iterables from the reducer.
    Receives as key an ordered iterable of nodes (the subgraph).
    Builds the the next neighbours_iterable filtering duplicates.
    """
    data = read_input(sys.stdin, separator=separator)
    for key, value in groupby(data, itemgetter(0)):
        next_iterable = set()
        for i in value:
            for t in eval(i[1]):
                next_iterable.add(t)

        next_tuple = tuple(next_iterable)
        key = tuple(eval(key))

        #  Check for the density!
        if density(key, next_tuple) >= RHO:
            with open("Output/FOUND", "w") as f:
                print("{}".format(key))
                print("{}".format(key), file=f)
                return

        print("{}\t{}".format(key, next_tuple))

if __name__ == "__main__":
    main()

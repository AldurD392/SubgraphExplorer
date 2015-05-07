#!/usr/bin/env python

from utils import read_input
from itertools import groupby
from operator import itemgetter
import sys


def main(separator='\t'):
    data = read_input(sys.stdin, separator=separator)
    for key, value in groupby(data, itemgetter(0)):
        output = "{}\t({},)".format((key, ), (key, ) + tuple(zip(*value)[1]))
        print(output.replace("'", ""))

if __name__ == "__main__":
    main()

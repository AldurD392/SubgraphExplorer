#!/usr/bin/env python
# encoding: utf-8

"""
Give me a SNAP graph file and I'll convert it to a proper input format.
"""

from __future__ import print_function
import sys

OUTPUT_SUFFIX = "MOD"


def clean_input_file(file_path):
    """Clean the input file s.t. it contains (a b) undirected edge only once. """
    lines = set()
    with open(file_path) as input_file:
        for line in input_file:
            l = line.rstrip().split("\t")
            l = sorted(l)
            lines.add("\t".join(l))

    lines = sorted(lines)
    with open(file_path + OUTPUT_SUFFIX, "w") as output_file:
        for line in lines:
            print(line, file=output_file)


if __name__ == '__main__':
    clean_input_file(sys.argv[1])

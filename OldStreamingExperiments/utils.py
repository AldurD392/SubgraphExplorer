#!/usr/bin/env python
# encoding: utf-8

from collections import Counter


def read_input(f, separator='\t'):
    """Read input from f and return it as a generator."""
    for line in f:
        # split the line into words
        yield line.rstrip().split(separator, 1)


def density(nodes, neighbours_iterable):
    nodes = set(nodes)

    neighbours = list()
    for t in neighbours_iterable:
        neighbours += t[1:]
    c = Counter(neighbours)
    edges = sum(v for k, v in c.items() if k in nodes)

    return (edges / 2.0) / len(nodes)


def test_density():
    assert (density((1, 2, 4, 6), ((1, 2, 3, 4, 6), (2, 1, 4, 6), (4, 1, 2, 5, 6), (6, 1, 2, 4))) == 6 / 4.0)
    assert (density(tuple(range(1, 6)), ((1, 2, 3, 4, 5), (2, 3, 4, 5, 1), (3, 4, 5, 1, 2), (4, 5, 1, 2, 3), (5, 1, 2, 3, 4))) == 2.0)


if __name__ == '__main__':
    test_density()

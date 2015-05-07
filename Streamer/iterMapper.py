#!/usr/bin/env python


from utils import read_input
from constants import EURISTIC_FACTOR

from collections import Counter
import sys


def choose_nodes(nodes, neighbours_iterable):
    neighbours_count = len(neighbours_iterable)
    unpacked_list = []
    for t in neighbours_iterable:
        unpacked_list += t[1:]
    c = Counter(unpacked_list)

    return tuple(k for k, v in c.items() if v >= neighbours_count * EURISTIC_FACTOR and k not in set(nodes))


def main(separator='\t'):
    """
    Choose the next node to be added to the subgraph.
    Take as input:
        - iterable of nodes (ordered) as key
        - iterable of iterable as value
    """
    data = read_input(sys.stdin)
    for nodes, neighbours_iterable in data:
        nodes = eval(nodes)

        neighbours_iterable = eval(neighbours_iterable)
        next_nodes = choose_nodes(nodes, neighbours_iterable)

        for n in next_nodes:
            print("{}\t{}".format(sorted(nodes + (n, )), neighbours_iterable))

if __name__ == '__main__':
    main()

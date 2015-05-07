#!/usr/bin/env python


import sys
# import set

def density(nodes, set_neighbours):
    edges = 0
    for t in set_neighbours:
        for u in node:
            if u in t:
                edges += 1
    return ((edges/2)/len(nodes))

def choose_nodes(set_neighbours):
    set_list = [set(t[1]) for t in set_neighbours]
    return set.intersection(*set_list)


def read_input(f, separator='\t'):
    for line in f:
        # split the line into words
        yield line.rstrip().split(separator, 1)


def main(separator='\t'):
    data = read_input(sys.stdin)
    for nodes, set_neighbours in data:
        nodes = eval(nodes)

        set_neighbours = eval(set_neighbours)
        next_nodes = choose_nodes(set_neighbours)

        for n in next_nodes:
            print("{}\t{}".format(sorted(nodes + [n]),  set_neighbours))

if __name__ == '__main__':
    main()
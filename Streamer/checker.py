#!/usr/bin/env python
# encoding: utf-8

import igraph
import sys
import math
import os

candidate = set(eval(sys.argv[2]))

i = 0
with open(sys.argv[1]) as f:
    g = igraph.Graph().Read_Edgelist(f, False)

    edges = set()
    for n in candidate:
        neigs = set(g.neighbors(n))
        intersect = neigs & candidate
        for v in intersect:
            edges.add(tuple(sorted([n, v])))
        i += len(intersect)

    print("Di Luzio - Francati")
    print(sys.argv[3])
    print(os.path.basename(sys.argv[1]))
    print(len(candidate))
    print(math.floor(i / 2.0))

    for v in candidate:
        print(v)

    for e in sorted(edges):
        print("\t".join((str(s) for s in e)))

    print("# End of configured output")

    g = g.subgraph(candidate, "copy_and_delete")
    g.write_svg("Graph.svg")

    print(len(g.es) / len(g.vs))
    print((i / 2.0) / len(candidate))

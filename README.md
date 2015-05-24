# Subgraph Explorer
Subgraph Explorer is an Hadoop Map-Reduce algorithm able to find the smallest dense-enough subgraph of a graph.

## Documentation
You can find any documentation you could need in the [Docs folder](Docs) of this repository.

Specifically, take a look to our [writeup](Docs/BigDataContestWriteUp.pdf).

## Execution
You'll find a [compiled JAR](Release/BigDataContest-jar-with-dependencies.jar) in the [Release](Release) directory.

To execute it:
```bash
$ git clone https://github.com/AldurD392/SubgraphExplorer.git
$ cd SubgraphExplorer
$ export HADOOP_CLASSPATH=`pwd`/Release/SubgraphExplorer-jar-with-dependencies.jar
$ hadoop jar Release/SubgraphExplorer-jar-with-dependencies.jar \
    -input graph-file.txt -rho RHO_VALUE
```

You can inspect the usage parameters with:
```bash
$ hadoop jar Release/SubgraphExplorer-jar-with-dependencies.jar

Usage: <main class> [options]
  Options:
  * -input
       Input file path.
  * -rho
       The fixed rho value: double value.
    -hefactor
       Heuristic Factor: double value.
       Default: Dinamically adapt it.
    -iter
       Iterate for iter rounds.
       Default: continue until a result is found.
    -knodes
       Number of nodes to emit at each SubgraphMapper stage. 
       This parameter influences the number of rounds.
       Default: 5
```
You can find other helpful resources in the [Helpers directory](Helpers).

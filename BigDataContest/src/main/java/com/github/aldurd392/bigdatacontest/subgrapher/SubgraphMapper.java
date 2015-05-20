package com.github.aldurd392.bigdatacontest.subgrapher;

import com.github.aldurd392.bigdatacontest.datatypes.IntArrayWritable;
import com.github.aldurd392.bigdatacontest.datatypes.NeighbourhoodMap;
import com.github.aldurd392.bigdatacontest.Main;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.*;

public class SubgraphMapper extends Mapper<NullWritable, NeighbourhoodMap, IntWritable, NeighbourhoodMap> {

    private final static Random random = new Random();

    /*
     * Here we return a list containing the most promising neighbours.
     * Those neighbours will be then emitted and on next round will be part
     * of our new subgraph.
     *
     * Note that if probability mode is enabled, then we'll emit those
     * neighbours only by honouring the given probability.
     */
    private static Set<IntWritable> chooseNodes(NeighbourhoodMap neighbourhoodMap) {

        /* We'll count here the occurrences of each neighbour in the map */
        final HashMap<Integer, Integer> counter = new HashMap<>();

        for (Writable writable_neighbours : neighbourhoodMap.values()) {
            IntArrayWritable neighbours = (IntArrayWritable) writable_neighbours;

            for (Writable writable_neighbour : neighbours.get()) {
                IntWritable neighbour = (IntWritable) writable_neighbour;
                Integer neighbour_integer = neighbour.get();

                if (neighbourhoodMap.containsKey(neighbour)) {
                    /*
                    We avoid counting again nodes already in the subgraph,
                    aka in the keys of neighbourhoodMap
                     */
                    continue;
                }

                Integer count = counter.get(neighbour_integer);
                if (count == null) {
                    counter.put(neighbour_integer, 1);
                } else {
                    counter.put(neighbour_integer, count + 1);
                }
            }
        }

        /*
        Now we have the count of how many times one of the neighbours is listed in the neighbourhood.
         */

        /* We'll store here the nodes keyed by their count. */
        final TreeMap<Integer, HashSet<Integer>> nodes_by_count = new TreeMap<>(Collections.reverseOrder());

        /*
        Now, we add to the return set only those element whose count is higher than
        our threshold:
            euristic factor * length
         */
        final int length = neighbourhoodMap.size();
        for (Map.Entry<Integer, Integer> entry : counter.entrySet()) {

            if (entry.getValue() >= Main.inputs.getEuristicFactor() * length) {
                HashSet<Integer> countedNodes = nodes_by_count.get(entry.getValue());

                if (countedNodes == null) {
                    countedNodes = new HashSet<>();
                }

                countedNodes.add(entry.getKey());
                nodes_by_count.put(entry.getValue(), countedNodes);
            }

        }

        /*
        Eventually, we pick the best nodes, in the number we need.
         */
        final int outputSize = Main.inputs.getOutNodes();
        HashSet<IntWritable> finalNodes = new HashSet<>(outputSize);

        for (Map.Entry<Integer, HashSet<Integer>> entry : nodes_by_count.entrySet()) {

            for (Integer node : entry.getValue()) {
                finalNodes.add(new IntWritable(node));

                if (finalNodes.size() == outputSize) {
                    return finalNodes;
                }
            }

        }

        return finalNodes;
    }


    @Override
    public void map(NullWritable key, NeighbourhoodMap value,
                    Context context)
            throws IOException, InterruptedException {

        final double p = Main.inputs.probMode();
        final Configuration conf = context.getConfiguration();
        final int round = conf.getInt("round", -1);

        if (round == 0) {
            /* On first round, we emit ourselves too.
            * We need to do this because Hadoop takes the liberty
            * to ignore our ReaderMapper on first round. */
            IntWritable node = (IntWritable) value.keySet().iterator().next();
            context.write(node, value);
        }

        for (IntWritable node : chooseNodes(value)) {
            if (p > 0 && random.nextDouble() > p) {
                /*
                If probabilistic mode is enabled,
                we honor the probability and skip emitting the node
                 */
                continue;
            }

            context.write(node, value);
        }
    }
}

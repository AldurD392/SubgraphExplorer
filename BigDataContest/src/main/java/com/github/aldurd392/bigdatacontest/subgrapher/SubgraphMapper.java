package com.github.aldurd392.bigdatacontest.subgrapher;

import com.github.aldurd392.bigdatacontest.datatypes.IntArrayWritable;
import com.github.aldurd392.bigdatacontest.datatypes.NeighbourhoodMap;
import com.github.aldurd392.bigdatacontest.Main;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.*;

public class SubgraphMapper extends Mapper<IntArrayWritable, NeighbourhoodMap, IntWritable, NeighbourhoodMap> {

    private final static Random random = new Random();


    /*
     * Here we return a list containing the most promising neighbours.
     * Those neighbours will be then emitted and on next round will be part
     * of our new subgraph.
     *
     * Note that if probability mode is enabled, then we'll emit those
     * neighbours only by the given probability.
     */
    private static List<IntWritable> chooseNodes(NeighbourhoodMap value) {

        HashMap<IntWritable, Integer> counter = new HashMap<>();
        int length = value.size();

        for (Writable writable : value.values()) {
            IntArrayWritable neighbours = (IntArrayWritable) writable;

            for (Writable w : neighbours.get()) {
                IntWritable i = (IntWritable) w;

                if (value.containsKey(i)) {
                    // Avoid counting again nodes already in the subgraph
                    continue;
                }

                Integer c = counter.get(i);
                if (c == null) {
                    counter.put(i, 1);
                } else {
                    counter.put(i, c + 1);
                }
            }
        }


        TreeMap<Integer, HashSet<IntWritable>> nodes = new TreeMap<>(Collections.reverseOrder());
        for (Map.Entry<IntWritable, Integer> entry : counter.entrySet()) {

            if (entry.getValue() > Main.inputs.getEuristicFactor() * length) {
                if (nodes.containsKey(entry.getValue())) {
                    nodes.get(entry.getValue()).add(entry.getKey());
                } else {
                    HashSet<IntWritable> relNodes = new HashSet<>();
                    relNodes.add(entry.getKey());
                    nodes.put(entry.getValue(), relNodes);
                }
            }

        }

        int size = Main.inputs.getOutNodes();
        ArrayList<IntWritable> finalNodes = new ArrayList<>(size);

        for (Map.Entry<Integer, HashSet<IntWritable>> entry : nodes.entrySet()) {
            for (IntWritable itNode : entry.getValue()) {
                finalNodes.add(itNode);
                if (finalNodes.size() == size) {
                    return finalNodes;
                }
            }

        }

        return finalNodes;
    }


    @Override
    public void map(IntArrayWritable key, NeighbourhoodMap value,
                    Context context)
            throws IOException, InterruptedException {

        final double p = Main.inputs.probMode();

        for (IntWritable node : chooseNodes(value)) {
            if (p > 0 && random.nextDouble() > p) {
                continue;
            }

            context.write(node, value);
        }
    }
}

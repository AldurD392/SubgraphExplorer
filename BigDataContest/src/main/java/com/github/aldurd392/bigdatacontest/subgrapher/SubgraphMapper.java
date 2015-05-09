package com.github.aldurd392.bigdatacontest.subgrapher;

import com.github.aldurd392.bigdatacontest.datatypes.IntArrayWritable;
import com.github.aldurd392.bigdatacontest.datatypes.NeighbourhoodMap;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.util.*;

public class SubgraphMapper extends MapReduceBase
        implements Mapper<IntArrayWritable, NeighbourhoodMap, IntArrayWritable, NeighbourhoodMap> {

    private static final double EURISTIC_FACTOR = 3.0/4.0;
    private int numberOfKeys = 0;

    private static Set<IntWritable> chooseNodes(NeighbourhoodMap value) {
        Set<IntWritable> set = new HashSet<>();
        HashMap<Integer, Integer> counter = new HashMap<>();
        int length = value.size();

        for (Writable writable: value.values()) {
            IntArrayWritable neighbours = (IntArrayWritable)writable;

            for (Writable w: neighbours.get()) {
                IntWritable i = (IntWritable) w;
                int _i = i.get();

                Integer c = counter.get(_i);
                if (c == null) {
                    counter.put(_i, 1);
                } else {
                    counter.put(_i, c + 1);
                }
            }
        }

        for (Map.Entry<Integer, Integer> entry: counter.entrySet()) {
            if (entry.getValue() >= EURISTIC_FACTOR * length) {
                set.add(new IntWritable(entry.getKey()));
            }
        }

        return set;
    }

    @Override
    public void map(IntArrayWritable key, NeighbourhoodMap value,
                    OutputCollector<IntArrayWritable, NeighbourhoodMap> output, Reporter reporter)
            throws IOException {
        numberOfKeys++;

        IntWritable[] intKey = (IntWritable[])key.get();
        ArrayList<IntWritable> nodes = new ArrayList<>(Arrays.asList(intKey));

        for (IntWritable node: chooseNodes(value)) {
            ArrayList<IntWritable> copy = (ArrayList<IntWritable>) nodes.clone();
            copy.add(node);
            Collections.sort(copy);
            IntArrayWritable newKey = new IntArrayWritable();
            newKey.set(copy.toArray(new IntWritable[copy.size()]));
            output.collect(newKey, value);
        }

        if (numberOfKeys % 10 == 0) {
            reporter.progress();
        }
    }
}

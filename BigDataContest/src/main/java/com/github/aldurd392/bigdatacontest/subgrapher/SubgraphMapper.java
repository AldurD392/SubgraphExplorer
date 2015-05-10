package com.github.aldurd392.bigdatacontest.subgrapher;

import com.github.aldurd392.bigdatacontest.datatypes.IntArrayWritable;
import com.github.aldurd392.bigdatacontest.datatypes.NeighbourhoodMap;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.*;

public class SubgraphMapper extends Mapper<IntArrayWritable, NeighbourhoodMap, IntArrayWritable, NeighbourhoodMap> {

    private static final double EURISTIC_FACTOR = 3.0/4.0;

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
                    Context context)
            throws IOException, InterruptedException {

        ArrayList<IntWritable> nodes = new ArrayList<>();
        for (Writable w: key.get()) {
            nodes.add((IntWritable) w);
        }

        System.out.println("MAPPEEEEER!");
        for (IntWritable node: chooseNodes(value)) {
            ArrayList<IntWritable> copy = (ArrayList<IntWritable>) nodes.clone();
            copy.add(node);
            Collections.sort(copy);
            IntArrayWritable newKey = new IntArrayWritable();
            newKey.set(copy.toArray(new IntWritable[copy.size()]));
            context.write(newKey, value);
        }
    }
}

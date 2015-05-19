package com.github.aldurd392.bigdatacontest.subgrapher;

import com.github.aldurd392.bigdatacontest.Main;
import com.github.aldurd392.bigdatacontest.datatypes.IntArrayWritable;
import com.github.aldurd392.bigdatacontest.datatypes.NeighbourhoodMap;
import com.github.aldurd392.bigdatacontest.utils.Utils;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class SubgraphReducer extends Reducer<IntWritable, NeighbourhoodMap, IntArrayWritable, NeighbourhoodMap> {

    @Override
    protected void reduce(IntWritable key, Iterable<NeighbourhoodMap> values, Context context)
            throws IOException, InterruptedException {

        NeighbourhoodMap map = new NeighbourhoodMap();
        ArrayList<NeighbourhoodMap> values_cache = new ArrayList<>();
        HashMap<IntWritable, Integer> counter = new HashMap<>();

        for (NeighbourhoodMap neighbourhoodMap : values) {
            NeighbourhoodMap _copy = new NeighbourhoodMap();
            _copy.putAll(neighbourhoodMap);
            values_cache.add(_copy);
            map.putAll(neighbourhoodMap);

            for (Writable k : neighbourhoodMap.keySet()) {
                counter.put((IntWritable) k, 0);
            }
        }
//        System.out.println("Reducer input: " + values_cache);

        if (map.size() == 1) {
            return;
        }
        if (values_cache.size() > 2) {
            for (NeighbourhoodMap neighbourhoodMap : values_cache) {
                for (Writable v : neighbourhoodMap.values()) {
                    IntArrayWritable neighbours = (IntArrayWritable) v;

                    for (Writable w : neighbours.get()) {
                        IntWritable neighbour = (IntWritable) w;

                        Integer count = counter.get(neighbour);
                        if (count != null) {
                            counter.put(neighbour, count + 1);
                        }
                    }
                }
            }

            for (Map.Entry<IntWritable, Integer> entry : counter.entrySet()) {
                if (entry.getValue() < Main.inputs.getEuristicFactor() * (counter.size() - 1)) {
                    map.remove(entry.getKey());
                }
            }
        }
//        System.out.println("Reducer counter: " + counter);
//        System.out.println("Reducer after filtering: " + map);

        IntArrayWritable intArrayWritable = new IntArrayWritable();
        ArrayList<IntWritable> list = new ArrayList<>();
        for (Writable w : map.keySet()) {
            list.add((IntWritable) w);
        }
        IntWritable[] intWritables = list.toArray(new IntWritable[list.size()]);
        intArrayWritable.set(intWritables);

        if (Utils.density(map) >= Main.inputs.getRho()) {
            Utils.writeResultOnFile(intArrayWritable);
        }

        context.write(intArrayWritable, map);
//        System.out.println("Reducer output: " + intArrayWritable + " - " + map);
    }
}

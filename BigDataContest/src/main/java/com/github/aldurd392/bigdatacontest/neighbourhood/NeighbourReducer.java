package com.github.aldurd392.bigdatacontest.neighbourhood;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import com.github.aldurd392.bigdatacontest.datatypes.NeighbourhoodMap;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.*;

import com.github.aldurd392.bigdatacontest.datatypes.IntArrayWritable;


public class NeighbourReducer extends MapReduceBase implements Reducer<IntWritable,IntWritable,IntArrayWritable,NeighbourhoodMap> {

	private final IntArrayWritable subgraph = new IntArrayWritable();

    private final IntWritable[] subgraph_array = new IntWritable[1];
    private final NeighbourhoodMap neighbours_map = new NeighbourhoodMap();
    private final ArrayList<IntWritable> neighbours_list = new ArrayList<>();

    private int numberOfKeys = 0;

    @Override
    public void reduce(IntWritable key, Iterator<IntWritable> value,
                       OutputCollector<IntArrayWritable, NeighbourhoodMap> outputCollector, Reporter reporter)
            throws IOException {
        numberOfKeys++;

        subgraph_array[0] = key;
        subgraph.set(subgraph_array);

        while (value.hasNext()) {
            IntWritable i = new IntWritable(value.next().get());
            neighbours_list.add(i);
        }

        IntArrayWritable neighbours = new IntArrayWritable();
        neighbours.set(neighbours_list.toArray(new IntWritable[neighbours_list.size()]));
        neighbours_map.put(key, neighbours);

        outputCollector.collect(subgraph, neighbours_map);

        neighbours_list.clear();
        neighbours_map.clear();

        if (numberOfKeys % 10 == 0) {
            reporter.progress();
        }
    }
}

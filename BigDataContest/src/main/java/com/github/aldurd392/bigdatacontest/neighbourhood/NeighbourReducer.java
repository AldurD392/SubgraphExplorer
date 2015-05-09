package com.github.aldurd392.bigdatacontest.neighbourhood;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.*;

import com.github.aldurd392.bigdatacontest.datatypes.IntArrayWritable;


public class NeighbourReducer extends MapReduceBase implements Reducer<IntWritable,IntWritable,IntArrayWritable,IntArrayWritable> {

	private final IntArrayWritable subgraph = new IntArrayWritable();
	private final IntArrayWritable neighbours = new IntArrayWritable();

    private final IntWritable[] subgraph_array = new IntWritable[1];
    private ArrayList<IntWritable> neighbours_list = new ArrayList<>();

    private int numberOfKeys = 0;

    @Override
    public void reduce(IntWritable key, Iterator<IntWritable> value,
                       OutputCollector<IntArrayWritable, IntArrayWritable> outputCollector, Reporter reporter)
            throws IOException {
        numberOfKeys++;

        subgraph_array[0] = key;
        subgraph.set(subgraph_array);

        while (value.hasNext()) {
            IntWritable i = value.next();
            neighbours_list.add(i);
        }

        neighbours.set(neighbours_list.toArray(new IntWritable[0]));
        neighbours_list.clear();
        outputCollector.collect(subgraph, neighbours);

        if (numberOfKeys % 10 == 0) {
            reporter.progress();
        }
    }
}

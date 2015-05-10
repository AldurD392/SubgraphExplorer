package com.github.aldurd392.bigdatacontest.neighbourhood;

import java.io.IOException;
import java.util.ArrayList;

import com.github.aldurd392.bigdatacontest.datatypes.NeighbourhoodMap;
import org.apache.hadoop.io.IntWritable;

import com.github.aldurd392.bigdatacontest.datatypes.IntArrayWritable;
import org.apache.hadoop.mapreduce.Reducer;


public class NeighbourReducer extends Reducer<IntWritable,IntWritable,IntArrayWritable,NeighbourhoodMap> {

	private final IntArrayWritable subgraph = new IntArrayWritable();

    private final IntWritable[] subgraph_array = new IntWritable[1];
    private final NeighbourhoodMap neighbours_map = new NeighbourhoodMap();
    private final ArrayList<IntWritable> neighbours_list = new ArrayList<>();

    @Override
    public void reduce(IntWritable key, Iterable<IntWritable> value,
                       Context context)
            throws IOException, InterruptedException {

        subgraph_array[0] = key;
        subgraph.set(subgraph_array);

        for (IntWritable aValue : value) {
            IntWritable i = new IntWritable(aValue.get());
            neighbours_list.add(i);
        }

        IntArrayWritable neighbours = new IntArrayWritable();
        neighbours.set(neighbours_list.toArray(new IntWritable[neighbours_list.size()]));
        neighbours_map.put(key, neighbours);

        context.write(subgraph, neighbours_map);

        neighbours_list.clear();
        neighbours_map.clear();
    }
}

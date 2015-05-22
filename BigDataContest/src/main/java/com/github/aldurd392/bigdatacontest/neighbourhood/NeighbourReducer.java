package com.github.aldurd392.bigdatacontest.neighbourhood;

import com.github.aldurd392.bigdatacontest.datatypes.IntArrayWritable;
import com.github.aldurd392.bigdatacontest.datatypes.NeighbourhoodMap;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;


public class NeighbourReducer extends Reducer<IntWritable,IntWritable,NullWritable,NeighbourhoodMap> {

    private final NeighbourhoodMap neighbours_map = new NeighbourhoodMap();
    private final ArrayList<IntWritable> neighbours_list = new ArrayList<>();

    @Override
    public void reduce(IntWritable key, Iterable<IntWritable> value,
                       Context context)
            throws IOException, InterruptedException {

        for (IntWritable aValue : value) {
            IntWritable i = new IntWritable(aValue.get());
            neighbours_list.add(i);
        }
        
        // Filter nodes with degree == 1
        if(neighbours_list.size() > 1){
	        IntArrayWritable neighbours = new IntArrayWritable();
	        neighbours.set(neighbours_list.toArray(new IntWritable[neighbours_list.size()]));
	        neighbours_map.put(key, neighbours);
	        
	        
	        context.write(NullWritable.get(), neighbours_map);
        }
        
        neighbours_list.clear();
        neighbours_map.clear();
        
    }
}

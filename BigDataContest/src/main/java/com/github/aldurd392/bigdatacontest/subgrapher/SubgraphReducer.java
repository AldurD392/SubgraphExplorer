package com.github.aldurd392.bigdatacontest.subgrapher;

import com.github.aldurd392.bigdatacontest.datatypes.IntArrayWritable;
import com.github.aldurd392.bigdatacontest.datatypes.NeighbourhoodMap;import com.github.aldurd392.bigdatacontest.utils.Utils;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;

public class SubgraphReducer extends Reducer<IntWritable,NeighbourhoodMap,IntArrayWritable,NeighbourhoodMap> {

    private static final int RHO = 2;
    @Override
    protected void reduce(IntWritable key, Iterable<NeighbourhoodMap> values, Context context)
            throws IOException, InterruptedException {
    	
        NeighbourhoodMap map = new NeighbourhoodMap();

        for (NeighbourhoodMap value : values) {
            map.putAll(value);
        }

        if (map.size() == 1) {
            return;
        }

        IntArrayWritable intArrayWritable = new IntArrayWritable();
        ArrayList<IntWritable> list = new ArrayList<>();
        for (Writable w: map.keySet()) {
            list.add((IntWritable) w);
        }
        IntWritable[] intWritables = list.toArray(new IntWritable[list.size()]);
        intArrayWritable.set(intWritables);
        
        if (Utils.density(map) >= RHO){
            Utils.writeResultOnFile(intArrayWritable);
        }

        context.write(intArrayWritable, map);
    }
}

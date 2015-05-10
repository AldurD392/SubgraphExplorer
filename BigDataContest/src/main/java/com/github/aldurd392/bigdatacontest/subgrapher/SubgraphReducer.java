package com.github.aldurd392.bigdatacontest.subgrapher;

import com.github.aldurd392.bigdatacontest.datatypes.IntArrayWritable;
import com.github.aldurd392.bigdatacontest.datatypes.NeighbourhoodMap;import com.github.aldurd392.bigdatacontest.utils.Utils;

import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class SubgraphReducer extends Reducer<IntArrayWritable,NeighbourhoodMap,IntArrayWritable,NeighbourhoodMap> {

    private static final int RHO = 2;
    @Override
    protected void reduce(IntArrayWritable key, Iterable<NeighbourhoodMap> values, Context context)
            throws IOException, InterruptedException {
    	
        NeighbourhoodMap map = new NeighbourhoodMap();

        for (NeighbourhoodMap value : values) {
            map.putAll(value);
        }
        
        if (Utils.density(map) >= RHO){
        		Utils.writeResultOnFile(key);
        }

//        System.out.println("REDUCEEEER!: " + key + " " + map);
        context.write(key, map);
    }
}

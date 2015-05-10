package com.github.aldurd392.bigdatacontest.subgrapher;

import com.github.aldurd392.bigdatacontest.datatypes.IntArrayWritable;
import com.github.aldurd392.bigdatacontest.datatypes.NeighbourhoodMap;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class SubgraphReducer extends Reducer<IntArrayWritable,NeighbourhoodMap,IntArrayWritable,NeighbourhoodMap> {

    private static final int RHO = 2;
    @Override
    protected void reduce(IntArrayWritable key, Iterable<NeighbourhoodMap> values, Context context)
            throws IOException, InterruptedException {

        NeighbourhoodMap map = new NeighbourhoodMap();
        Iterator<NeighbourhoodMap> iterator = values.iterator();

        while (iterator.hasNext()) {
            map.putAll(iterator.next());
        }

        System.out.println("REDUCEEEER!: " + key + " " + map);
        context.write(key, map);
    }
}

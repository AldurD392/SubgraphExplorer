package com.github.aldurd392.bigdatacontest.subgrapher;

import com.github.aldurd392.bigdatacontest.datatypes.IntArrayWritable;
import com.github.aldurd392.bigdatacontest.datatypes.NeighbourhoodMap;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class SubgraphReducer extends Reducer<IntArrayWritable,NeighbourhoodMap,IntArrayWritable,NeighbourhoodMap> {

    @Override
    protected void reduce(IntArrayWritable key, Iterable<NeighbourhoodMap> values, Context context)
            throws IOException, InterruptedException {

        NeighbourhoodMap map = new NeighbourhoodMap();
        while (values.iterator().hasNext()) {
            map.putAll(values.iterator().next());
        }

        context.write(key, map);
    }
}

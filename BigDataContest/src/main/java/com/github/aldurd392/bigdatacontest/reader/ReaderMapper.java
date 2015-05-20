package com.github.aldurd392.bigdatacontest.reader;

import com.github.aldurd392.bigdatacontest.datatypes.NeighbourhoodMap;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * BigDataContest - com.github.aldurd392.bigdatacontest.reader
 * Created by aldur on 10/05/15.
 */
public class ReaderMapper extends Mapper<NullWritable, NeighbourhoodMap, IntWritable, NeighbourhoodMap> {

    /*
    This is our reader mapper.
    It works this by side with the NeighbourhoodMapper,
    and at each iteration step it delivers to the reducer the neighbours of
    the key node.
    This let us speed up our algorithm "exponentially".
     */
    @Override
    public void map(NullWritable key, NeighbourhoodMap value, Context context)
            throws IOException, InterruptedException {
        IntWritable node = (IntWritable) value.keySet().iterator().next();
        context.write(node, value);
    }
}

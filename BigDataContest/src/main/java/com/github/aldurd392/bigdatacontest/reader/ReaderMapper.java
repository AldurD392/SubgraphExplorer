package com.github.aldurd392.bigdatacontest.reader;

import com.github.aldurd392.bigdatacontest.datatypes.IntArrayWritable;
import com.github.aldurd392.bigdatacontest.datatypes.NeighbourhoodMap;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * BigDataContest - com.github.aldurd392.bigdatacontest.reader
 * Created by aldur on 10/05/15.
 */
public class ReaderMapper extends Mapper<IntArrayWritable, NeighbourhoodMap, IntWritable, NeighbourhoodMap> {

    @Override
    public void map(IntArrayWritable key, NeighbourhoodMap value, Context context)
            throws IOException, InterruptedException  {
        Writable[] writables = key.get();
        IntWritable w = (IntWritable)writables[0];
        context.write(w, value);
    }
}

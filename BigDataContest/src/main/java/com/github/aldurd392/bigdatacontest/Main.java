/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.github.aldurd392.bigdatacontest;

import java.io.IOException;

import com.github.aldurd392.bigdatacontest.neighbourhood.NeighbourMapper;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

import com.github.aldurd392.bigdatacontest.datatypes.IntArrayWritable;
import com.github.aldurd392.bigdatacontest.neighbourhood.NeighbourReducer;

public class Main {

    public static void main(String[] args)
            throws IOException, InterruptedException, ClassNotFoundException {

        // First round: ------ CountEdges -------
        JobConf neighbourhoodConfg = new JobConf(new Configuration());

        neighbourhoodConfg.setJobName("Neighbourhood");

        FileInputFormat.setInputPaths(neighbourhoodConfg, new Path(args[0]));
        FileOutputFormat.setOutputPath(neighbourhoodConfg, new Path(args[1]));

        neighbourhoodConfg.setMapperClass(NeighbourMapper.class);
        neighbourhoodConfg.setReducerClass(NeighbourReducer.class);

        neighbourhoodConfg.setInputFormat(KeyValueTextInputFormat.class);

        neighbourhoodConfg.setMapOutputKeyClass(IntWritable.class);
        neighbourhoodConfg.setMapOutputValueClass(IntWritable.class);
        neighbourhoodConfg.setOutputKeyClass(IntArrayWritable.class);
        neighbourhoodConfg.setOutputValueClass(IntArrayWritable.class);

        JobClient.runJob(neighbourhoodConfg);
    }
}

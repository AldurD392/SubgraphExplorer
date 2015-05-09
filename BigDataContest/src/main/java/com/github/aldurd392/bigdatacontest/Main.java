/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.github.aldurd392.bigdatacontest;

import java.io.IOException;

import com.github.aldurd392.bigdatacontest.datatypes.NeighbourhoodMap;
import com.github.aldurd392.bigdatacontest.neighbourhood.NeighbourMapper;
import com.github.aldurd392.bigdatacontest.subgrapher.SubgraphMapper;
import com.github.aldurd392.bigdatacontest.subgrapher.SubgraphReducer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

import com.github.aldurd392.bigdatacontest.datatypes.IntArrayWritable;
import com.github.aldurd392.bigdatacontest.neighbourhood.NeighbourReducer;
import org.apache.hadoop.mapred.jobcontrol.Job;
import org.apache.hadoop.mapred.jobcontrol.JobControl;

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
        neighbourhoodConfg.setOutputFormat(SequenceFileOutputFormat.class);

        neighbourhoodConfg.setMapOutputKeyClass(IntWritable.class);
        neighbourhoodConfg.setMapOutputValueClass(IntWritable.class);
        neighbourhoodConfg.setOutputKeyClass(IntArrayWritable.class);
        neighbourhoodConfg.setOutputValueClass(NeighbourhoodMap.class);

        Job neighbourhoodJob = new Job(neighbourhoodConfg);

        // Next rounds: ------ Build the subgraph -------
        JobConf subgraphConfg = new JobConf(new Configuration());
        subgraphConfg.setJobName("Subgrapher");

        FileInputFormat.setInputPaths(subgraphConfg, new Path(args[1]));
        FileOutputFormat.setOutputPath(subgraphConfg, new Path(args[2]));

        subgraphConfg.setInputFormat(SequenceFileInputFormat.class);
        subgraphConfg.setOutputFormat(TextOutputFormat.class);

        subgraphConfg.setMapperClass(SubgraphMapper.class);
        subgraphConfg.setReducerClass(SubgraphReducer.class);

//        subgraphConfg.setMapOutputKeyClass(IntArrayWritable.class);
//        subgraphConfg.setMapOutputValueClass(NeighbourhoodMap.class);
        subgraphConfg.setOutputKeyClass(IntArrayWritable.class);
        subgraphConfg.setOutputValueClass(NeighbourhoodMap.class);

        Job subgraphJob = new Job(subgraphConfg);

        // Job controller -----
        JobControl controller = new JobControl("BigDataContest");
        controller.addJob(neighbourhoodJob);
        controller.addJob(subgraphJob);
        subgraphJob.addDependingJob(neighbourhoodJob);

        controller.run();
    }
}

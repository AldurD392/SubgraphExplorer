/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.github.aldurd392.bigdatacontest;

import java.io.IOException;
import java.util.ArrayList;

import com.github.aldurd392.bigdatacontest.datatypes.IntArrayWritable;
import com.github.aldurd392.bigdatacontest.datatypes.NeighbourhoodMap;
import com.github.aldurd392.bigdatacontest.neighbourhood.NeighbourMapper;
import com.github.aldurd392.bigdatacontest.subgrapher.SubgraphMapper;
import com.github.aldurd392.bigdatacontest.subgrapher.SubgraphReducer;
import com.github.aldurd392.bigdatacontest.utils.Utils;
import com.github.aldurd392.bigdatacontest.neighbourhood.NeighbourReducer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Main extends Configured implements Tool {

    private static Configuration mConfig;

    private final static String TEMPORARY_OUTPUT_DIRECTORY = "output";
    private final static String SUBGRAPHER_NAME = "subgrapher";

    public static void main(String[] args)
            throws Exception {
        int res = ToolRunner.run(new Configuration(), new Main(), args);
        System.exit(res);
    }

    private Job neighbourhoodJob(Configuration conf, String input) throws IOException {
        // First round: ------ CountEdges -------
        Job neighbourhoodJob = Job.getInstance(conf);
        neighbourhoodJob.setJobName("Neighbourhood");
        neighbourhoodJob.setJarByClass(Main.class);

        FileInputFormat.addInputPath(neighbourhoodJob, new Path(input));
        FileOutputFormat.setOutputPath(neighbourhoodJob, new Path(TEMPORARY_OUTPUT_DIRECTORY + "_" + 0));

        neighbourhoodJob.setMapperClass(NeighbourMapper.class);
        neighbourhoodJob.setReducerClass(NeighbourReducer.class);

        neighbourhoodJob.setInputFormatClass(KeyValueTextInputFormat.class);
        neighbourhoodJob.setOutputFormatClass(SequenceFileOutputFormat.class);

        neighbourhoodJob.setMapOutputKeyClass(IntWritable.class);
        neighbourhoodJob.setMapOutputValueClass(IntWritable.class);
        neighbourhoodJob.setOutputKeyClass(IntArrayWritable.class);
        neighbourhoodJob.setOutputValueClass(NeighbourhoodMap.class);

        return neighbourhoodJob;
    }

    private Job subgraphJob(Configuration conf, String input, String ouput, String jobName) throws IOException {
        Job subgraphJob = Job.getInstance(conf);
        subgraphJob.setJobName(jobName);
        subgraphJob.setJarByClass(IntArrayWritable.class);

        FileInputFormat.setInputPaths(subgraphJob, new Path(input));
        FileOutputFormat.setOutputPath(subgraphJob, new Path(ouput));

        subgraphJob.setInputFormatClass(SequenceFileInputFormat.class);
        subgraphJob.setOutputFormatClass(SequenceFileOutputFormat.class);

        subgraphJob.setMapperClass(SubgraphMapper.class);
        subgraphJob.setReducerClass(SubgraphReducer.class);

        subgraphJob.setMapOutputKeyClass(IntArrayWritable.class);
        subgraphJob.setMapOutputValueClass(NeighbourhoodMap.class);
        subgraphJob.setOutputKeyClass(IntArrayWritable.class);
        subgraphJob.setOutputValueClass(NeighbourhoodMap.class);

        return subgraphJob;
    }

    private String getInputFolder(int i) {
        return TEMPORARY_OUTPUT_DIRECTORY + "_" + i;
    }

    private String getOuputFolder(int i) {
        return TEMPORARY_OUTPUT_DIRECTORY + "_" + (++i);
    }

    private Job iSubgraphJob(int i, Configuration conf) throws IOException {
        String input = getInputFolder(i);
        String jobName = SUBGRAPHER_NAME + "_" + i;
        String output = getOuputFolder(i);

        return subgraphJob(conf, input, output, jobName);
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = this.getConf();
        JobControl controller = new JobControl("BigDataContest");

        Job neighbourhoodJob = neighbourhoodJob(conf, args[0]);
        ControlledJob neighbourhoodControlledJob = new ControlledJob(neighbourhoodJob, null);
        controller.addJob(neighbourhoodControlledJob);

        // Next rounds: ------ Build the subgraph -------
        int i = -1;

        ControlledJob oldSubgraphJob = neighbourhoodControlledJob;
        boolean results;

        while (!(results = Utils.resultFound()) && Utils.previousResults(getOuputFolder(i))) {
            ArrayList<ControlledJob> dependencies = new ArrayList<>(1);
            dependencies.add(oldSubgraphJob);
            Job newSubgraphJob = iSubgraphJob(++i, conf);
            ControlledJob newSubgraphControlledJob = new ControlledJob(newSubgraphJob, dependencies);
            controller.addJob(newSubgraphControlledJob);
            
            Thread t = new Thread(controller);
            t.start();

            while (!controller.allFinished()) {
                Thread.sleep(1000);
            }            
            
            oldSubgraphJob = newSubgraphControlledJob;
        }

        if (results) {
            System.out.println("We have found the result! :D");
        } else {
            System.out.println("Sorry, no results are possible. :(");
        }

        return 0;
    }

    @Override
    public void setConf(Configuration configuration) {
        mConfig = configuration;
    }

    @Override
    public Configuration getConf() {
        return mConfig;
    }
}

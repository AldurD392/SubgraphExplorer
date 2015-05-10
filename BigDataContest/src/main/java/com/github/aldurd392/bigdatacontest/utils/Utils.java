package com.github.aldurd392.bigdatacontest.utils;

import com.github.aldurd392.bigdatacontest.datatypes.IntArrayWritable;
import com.github.aldurd392.bigdatacontest.datatypes.NeighbourhoodMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;

import java.io.IOException;
import java.util.HashSet;

/**
 * BigDataContest - com.github.aldurd392.bigdatacontest.utils
 * Created by aldur on 10/05/15.
 */
public class Utils {
    public static final String resultFileName = "_FOUND";
    public static final String pathResultFile = "result";

    public static double density(NeighbourhoodMap neighbourhood) {
        HashSet<Integer> nodes_set = new HashSet<>();
        for (Writable w : neighbourhood.keySet()) {
            IntWritable i = (IntWritable)w;
            nodes_set.add(i.get());
        }

        int i = 0;
        for (Writable w: neighbourhood.values()) {
            IntArrayWritable array_writable = (IntArrayWritable)w;
            for (Writable w_node: array_writable.get()) {
                IntWritable node = (IntWritable)w_node;
                if (nodes_set.contains(node.get())) {
                    i++;
                }
            }
        }

        return (i / 2.0) / nodes_set.size();
    }

    public static void writeResultOnFile(IntArrayWritable result) throws IOException {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        Path outFile = new Path(pathResultFile + "/" + resultFileName);

        if (fs.exists(outFile)) {
            System.out.println("Result File Already Exists!!!!!!");
            System.out.println("RESULT not written: " + result);
            System.exit(0);
        }

        FSDataOutputStream out = fs.create(outFile);
        out.writeChars(result.toString());
        out.close();
    }

    public static boolean resultFound() throws IOException {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        Path resultFile = new Path(pathResultFile + "/" + resultFileName);
        return fs.exists(resultFile);
    }

    public static boolean previousResults(String currentOuputDirectory) throws IOException {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);

        Writable w = new IntArrayWritable();

        FileStatus[] statuses;
        try {
            statuses = fs.listStatus(new Path(currentOuputDirectory));
        } catch (java.io.FileNotFoundException e) {
            // The first time we don't have previous result!
            return true;
        }

        for (FileStatus status: statuses) {
            if (status.getPath().getName().equals("_SUCCESS")) {
                continue;
            }

            SequenceFile.Reader reader = new SequenceFile.Reader(fs, status.getPath(), conf);
            if (reader.next(w)) {
                reader.close();
                return true;
            }

            reader.close();
        }

        return false;
    }
}

package com.github.aldurd392.bigdatacontest.utils;

import com.github.aldurd392.bigdatacontest.Main;
import com.github.aldurd392.bigdatacontest.datatypes.IntArrayWritable;
import com.github.aldurd392.bigdatacontest.datatypes.NeighbourhoodMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;

/**
 * BigDataContest - com.github.aldurd392.bigdatacontest.utils
 * Created by aldur on 10/05/15.
 */
public class Utils {
    private static final String resultFileName = "_FOUND";
    private static final String pathResultFile = "result";
    private static final String successFileName = "_SUCCESS";

    private static final double smoothing_factor = 5;

    public static IntArrayWritable density(NeighbourhoodMap neighbourhood) {
    		HashSet<IntWritable> nodes_set = new HashSet<>();
    		for (Writable w : neighbourhood.keySet()) {
            IntWritable i = (IntWritable) w;
            nodes_set.add(i);
        }
        
        int num_nodes = nodes_set.size();

        int i = 0;
        for (Writable w : neighbourhood.values()) {
            IntArrayWritable array_writable = (IntArrayWritable) w;
            for (Writable w_node : array_writable.get()) {
                IntWritable node = (IntWritable) w_node;
                if (nodes_set.contains(node)) {
                    i++;
                }
            }
        }
        
        // Final edges count.
        i = i/2;
        
        if(i/num_nodes >= Main.inputs.getRho()){
            
        		IntArrayWritable intArrayWritable = new IntArrayWritable();
        		
        		// Check if the subgraph founded is a clique.
            if (i == ((num_nodes - 1)*num_nodes)/2){
            		double diff = i/num_nodes - Main.inputs.getRho();
            		System.out.println("Filtro di fattore: " +diff);
            		
            		Iterator<IntWritable> it = nodes_set.iterator();
            		for(; diff >= 0.5; diff-=0.5){
            			it.next();
            			it.remove();
            		}
            }
            
       		IntWritable[] nodes_array = nodes_set.toArray(new IntWritable[nodes_set.size()]);
         	intArrayWritable.set(nodes_array);
        		return intArrayWritable;
        }
        	
        return null;
    }

    public static void writeResultOnFile(IntArrayWritable result) throws IOException {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        Path outFile = new Path(pathResultFile + "/" + resultFileName);

        FSDataOutputStream out;
        if (!fs.exists(outFile)) {
            out = fs.create(outFile);
            out.close();
        }

        out = fs.create(new Path(pathResultFile + "/" + result.hashCode()));
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

        for (FileStatus status : statuses) {
            if (status.getPath().getName().equals(successFileName)) {
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

    public static double euristicFactorFunction(double rho) {
        return Math.pow(rho, -(Math.log10(rho) / smoothing_factor));
    }
}

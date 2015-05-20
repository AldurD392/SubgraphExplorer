package com.github.aldurd392.bigdatacontest.utils;

import com.github.aldurd392.bigdatacontest.datatypes.IntArrayWritable;
import com.github.aldurd392.bigdatacontest.datatypes.NeighbourhoodMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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

    /**
     * Given the neighbourhood map, calculate the density of the subgraph.
     * @param neighbourhood our subgraph representation
     * @param rho the required density value
     * @return null if the supplied subgraph didn't meet density requirements,
     * otherwise the densest subgraph found that still meets the requirements.
     */
    public static IntArrayWritable density(NeighbourhoodMap neighbourhood, double rho) {
        final int neighbourhood_size = neighbourhood.size();

        final HashSet<IntWritable> neighbourhood_nodes = new HashSet<>(neighbourhood_size);
        for (Writable writable_node: neighbourhood.keySet()) {
            IntWritable node = (IntWritable) writable_node;
            neighbourhood_nodes.add(node);
        }

        /*
         * We have the number of nodes in the subgraph.
         * Let's count the number of edges too.
         */
        double edges_count = 0;
        for (Writable writable_neighbours : neighbourhood.values()) {
            IntArrayWritable neighbours = (IntArrayWritable) writable_neighbours;

            for (Writable writable_neighbour : neighbours.get()) {
                IntWritable neighbour = (IntWritable) writable_neighbour;

                if (neighbourhood_nodes.contains(neighbour)) {
                    edges_count++;
                }
            }
        }
        // We counted each edge twice, so better divide by two here.
        edges_count = edges_count / 2.0;

        if (edges_count / neighbourhood_size >= rho) {
            /*
             * Once we know that the subgraph we're dealing with meets our density requirements,
             * we check if it's a clique (by comparing the number of edges and the number of nodes).
             * In that case we can shrink its size until it's minimal (our goal).
             */
            if (edges_count == ((neighbourhood_size - 1) * neighbourhood_size) / 2) {
                Iterator<IntWritable> neighbourhood_nodes_iterator = neighbourhood_nodes.iterator();
                for (double diff = edges_count / neighbourhood_size - rho; diff >= 0.5; diff -= 0.5) {
                    neighbourhood_nodes_iterator.next();
                    neighbourhood_nodes_iterator.remove();
                }
            }

            IntWritable[] nodes_array = neighbourhood_nodes.toArray(new IntWritable[neighbourhood_nodes.size()]);
            final IntArrayWritable densestSubgraph = new IntArrayWritable();
            densestSubgraph.set(nodes_array);

            return densestSubgraph;
        }

        /* If the subgraph didn't meet required density value, we return null. */
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

    /**
     * This function selects a proper euristicFactor given the desired rho
     * @param rho the desired density.
     * @return an euristic factor that is inversely proportional to rho.
     */
    public static double euristicFactorFunction(double rho) {
        return Math.pow(rho, -(Math.log10(rho) / smoothing_factor));
    }
}

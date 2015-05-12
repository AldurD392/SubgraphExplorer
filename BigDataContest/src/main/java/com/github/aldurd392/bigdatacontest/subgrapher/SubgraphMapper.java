package com.github.aldurd392.bigdatacontest.subgrapher;

import com.github.aldurd392.bigdatacontest.datatypes.IntArrayWritable;
import com.github.aldurd392.bigdatacontest.datatypes.NeighbourhoodMap;
import com.github.aldurd392.bigdatacontest.Main;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.*;

public class SubgraphMapper extends Mapper<IntArrayWritable, NeighbourhoodMap, IntWritable, NeighbourhoodMap> {

	private Random rndm = new Random();
	
	
    private static IntWritable chooseNodes(NeighbourhoodMap value) {
    	
        HashMap<IntWritable, Integer> counter = new HashMap<>();
        int length = value.size();

        for (Writable writable: value.values()) {
            IntArrayWritable neighbours = (IntArrayWritable)writable;

            for (Writable w: neighbours.get()) {
                IntWritable i = (IntWritable) w;
                
                if (value.containsKey(i)){
                    continue;
                }

                Integer c = counter.get(i);
                if (c == null) {
                    counter.put(i, 1);
                } else {
                    counter.put(i, c + 1);
                }
            }
        }

        Integer maxValue = Integer.MIN_VALUE;
        IntWritable maxKey = null;

        for (Map.Entry<IntWritable, Integer> entry: counter.entrySet()) {
            if (entry.getValue() > maxValue) {
                maxValue = entry.getValue();
                maxKey = entry.getKey();
            }
        }

        if (maxValue >= Main.inputs.getEuristicFactor() * length) {
            return maxKey;
        }

        return null;
    }

    @Override
    public void map(IntArrayWritable key, NeighbourhoodMap value,
                    Context context)
            throws IOException, InterruptedException {
    	
    		if(Main.inputs.probMode() && rndm.nextDouble() > Main.inputs.getEuristicFactor()){
    			return;
    		}
    		
        IntWritable newKey = chooseNodes(value);
        
        if (newKey != null) {
            context.write(newKey, value);
        }
    }
}

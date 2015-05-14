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
	
	
    private static ArrayList<IntWritable> chooseNodes(NeighbourhoodMap value) {
    	
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
        
        
        TreeMap<Integer, ArrayList<IntWritable>> nodes = new TreeMap<>(Collections.reverseOrder());
        
        for (Map.Entry<IntWritable, Integer> entry: counter.entrySet()) {
        	
        		if (entry.getValue() > Main.inputs.getEuristicFactor() * length){
        			if (nodes.containsKey(entry.getValue())){
        				nodes.get(entry.getValue()).add(entry.getKey());
        			}
        			else {
        				ArrayList<IntWritable> relNodes = new ArrayList<IntWritable>();
        				relNodes.add(entry.getKey());
        				nodes.put(entry.getValue(), relNodes);
        			}
        		}
        	
        }
        
        ArrayList<IntWritable> finalNodes = new ArrayList<>();
        
        for (Map.Entry<Integer, ArrayList<IntWritable>> entry : nodes.entrySet()){
        		for (IntWritable itNode: entry.getValue()){
        			finalNodes.add(itNode);
        			if (finalNodes.size() == Main.inputs.getOutNodes()) return finalNodes;
        		}
        		
        }
        return finalNodes;
    }

    
    @Override
    public void map(IntArrayWritable key, NeighbourhoodMap value,
                    Context context)
            throws IOException, InterruptedException {
    	
    		if(Main.inputs.probMode() && rndm.nextDouble() > Main.inputs.getEuristicFactor()){
    			return;
    		}
    		
        for (IntWritable node : chooseNodes(value)){
            context.write(node, value);
        }
    }
}

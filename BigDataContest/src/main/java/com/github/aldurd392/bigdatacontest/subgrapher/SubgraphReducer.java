package com.github.aldurd392.bigdatacontest.subgrapher;

import com.github.aldurd392.bigdatacontest.Main;
import com.github.aldurd392.bigdatacontest.datatypes.IntArrayWritable;
import com.github.aldurd392.bigdatacontest.datatypes.NeighbourhoodMap;
import com.github.aldurd392.bigdatacontest.utils.Utils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.PriorityQueue;

public class SubgraphReducer extends Reducer<IntWritable, NeighbourhoodMap, NullWritable, NeighbourhoodMap> {

    @Override
    protected void reduce(IntWritable key, Iterable<NeighbourhoodMap> values, Context context)
            throws IOException, InterruptedException {
        final NeighbourhoodMap subgraphMap = new NeighbourhoodMap();
        int mapIterableCounter = 0;

        final Configuration conf = context.getConfiguration();
        final int round = conf.getInt("round", -1);

        Double heuristicFactor = Main.inputs.getHeuristicFactor();
        if (heuristicFactor == null) {
            heuristicFactor = Utils.getHeuristicFactorValue(round);
        }

        for (NeighbourhoodMap neighbourhoodMap : values) {
            subgraphMap.putAll(neighbourhoodMap);
            mapIterableCounter++;
        }

        if (subgraphMap.size() == 1 || mapIterableCounter == 1) {
            /*
            We ignore data coming from ReaderMapper
            that no one asked and wouldn't be used.
            We also ignore those who asked for a node with degree 1.
             */
            return;
        }

        /* We are preparing to filter nodes in map.
        If the original values iterable contained only two elements,
        there is no need for filtering, because we're dealing with
        our previous subgraph and the output from ReaderMapper.
         */
        if (mapIterableCounter > 2) {
            /*
            With this map we'll count all the occurrences of the
            key nodes (and only of them) in our map among the neighbourhoods.
            */
            final HashMap<IntWritable, Integer> counter = new HashMap<>();
            for (Writable k : subgraphMap.keySet()) {
                counter.put((IntWritable) k, 0);
            }

            for (Writable writable_neighbours : subgraphMap.values()) {
                IntArrayWritable neighbours = (IntArrayWritable) writable_neighbours;

                for (Writable writable_neighbour : neighbours.get()) {
                    IntWritable neighbour = (IntWritable) writable_neighbour;

                    Integer count = counter.get(neighbour);
                    if (count != null) {
                        counter.put(neighbour, count + 1);
                    }
                }
            }

            /*
            The referencesMap and the minHeap will be useful for our filtering process.
            We'll use the minHeap to iterate through the nodes by minimum counts.
            We'll use the referencesMap to update the counts of the other nodes after filtering.
             */
            final HashMap<IntWritable, Entry<IntWritable, Integer>> referencesMap = new HashMap<>(counter.size());
            final PriorityQueue<Entry<IntWritable, Integer>> minHeap = new PriorityQueue<>(subgraphMap.size(),
                    new Comparator<Entry<IntWritable, Integer>>() {
                @Override 
                public int compare(Entry<IntWritable, Integer> o1, Entry<IntWritable, Integer> o2) {
                    return o1.getValue().compareTo(o2.getValue());
                }
            });

            for (Entry<IntWritable, Integer> entry : counter.entrySet()) {
                minHeap.add(entry);
                referencesMap.put(entry.getKey(), entry);
            }

            Entry<IntWritable, Integer> minHeapEntry;
            while ((minHeapEntry = minHeap.poll()) != null) {

                /*
                This is the filtering step:
                we remove form the subgraph all those nodes that are not promising,
                and that have been added because of an erroneous merge.
                 */
                if (minHeapEntry.getValue() < (heuristicFactor * (subgraphMap.size() - 1))) {
                    IntWritable minHeapEntryKey = minHeapEntry.getKey();
                    IntArrayWritable neighbours = (IntArrayWritable) subgraphMap.remove(minHeapEntryKey);
                    referencesMap.remove(minHeapEntryKey);

                    /* Update every other counter that depended on the key of the entry. */
                    for (Writable writable_neighbour : neighbours.get()) {
                        IntWritable neighbour = (IntWritable) writable_neighbour;

                        Entry<IntWritable, Integer> referenceEntry = referencesMap.get(neighbour);
                        if (referenceEntry != null) {
                            minHeap.remove(referenceEntry);
                            referenceEntry.setValue(referenceEntry.getValue() - 1);
                            minHeap.add(referenceEntry);
                        }
                    }
                } else {
                    /* When we stop removing nodes, we know that we can move on to the next step. */
                    break;
                }
            }
        }

        IntArrayWritable result = Utils.density(subgraphMap, Main.inputs.getRho());
        if (result != null) {
            Utils.writeResultOnFile(result);
        }

        context.write(NullWritable.get(), subgraphMap);
    }
}

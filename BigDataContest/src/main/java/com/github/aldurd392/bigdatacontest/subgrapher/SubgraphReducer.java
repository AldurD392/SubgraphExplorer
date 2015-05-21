package com.github.aldurd392.bigdatacontest.subgrapher;

import com.github.aldurd392.bigdatacontest.Main;
import com.github.aldurd392.bigdatacontest.datatypes.IntArrayWritable;
import com.github.aldurd392.bigdatacontest.datatypes.NeighbourhoodMap;
import com.github.aldurd392.bigdatacontest.utils.Utils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map.Entry;
import java.util.PriorityQueue;

public class SubgraphReducer extends Reducer<IntWritable, NeighbourhoodMap, NullWritable, NeighbourhoodMap> {

    private final static double euristicFactor = Main.inputs.getEuristicFactor();

    @Override
    protected void reduce(IntWritable key, Iterable<NeighbourhoodMap> values, Context context)
            throws IOException, InterruptedException {
        final NeighbourhoodMap subgraphMap = new NeighbourhoodMap();
        int mapIterableCounter = 0;

        for (NeighbourhoodMap neighbourhoodMap : values) {
            subgraphMap.putAll(neighbourhoodMap);
            mapIterableCounter++;
        }

        if (subgraphMap.size() == 1) {
            /*
            We ignore data coming from ReaderMapper
            that no one asked and wouldn't be used.
             */
            return;
        }

        /* We are preparing to filter nodes in map.
        If the original values itarable contained only two elements,
        there is no need for filtering, because we're dealing with
        our previous subgraph and the ouput from readermapper.
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
//            System.out.println("Reducer input: " + key + " " + subgraphMap);

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

            final HashMap<IntWritable, Entry<IntWritable, Integer>> referencesMap = new HashMap<>(counter.size());
            final PriorityQueue<Entry<IntWritable, Integer>> minHeap = new PriorityQueue<>(
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

//            System.out.println(subgraphMap);
            Entry<IntWritable, Integer> minHeapEntry;
            while ((minHeapEntry = minHeap.poll()) != null) {

                if (minHeapEntry.getValue() < (euristicFactor * (subgraphMap.size() - 1))) {
                    IntWritable minHeapEntryKey = minHeapEntry.getKey();
//                    System.out.println(minHeapEntry.getKey() + " : " + minHeapEntry.getValue());
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
                    break;
                }
            }

//            System.out.println("Reducer after filtering: " + subgraphMap);
        }

        IntArrayWritable result = Utils.density(subgraphMap, Main.inputs.getRho());
        if (result != null) {
            // Edges count.
            HashSet<Integer> nodes_set = new HashSet<>();

            for (Writable w : result.get()) {
                IntWritable i = (IntWritable) w;
                nodes_set.add(i.get());
            }

            int i = 0;
            for (Entry<Writable, Writable> entry : subgraphMap.entrySet()) {
                IntWritable currKey = (IntWritable) entry.getKey();
                if (nodes_set.contains(currKey.get())) {
                    IntArrayWritable array_writable = (IntArrayWritable) entry.getValue();
                    for (Writable w_node : array_writable.get()) {
                        IntWritable node = (IntWritable) w_node;
                        if (nodes_set.contains(node.get())) {
                            i++;
                        }
                    }
                }
            }

            // Normalize edges count.
            i = i / 2;

            System.out.println("Densita: " + (double) i / result.get().length);
            System.out.println("Nodi: " + result);
            System.out.println("#Nodi: " + result.get().length);
            System.out.println("#Archi: " + i);

            Utils.writeResultOnFile(result);
        }

        context.write(NullWritable.get(), subgraphMap);
    }
}

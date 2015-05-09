package com.github.aldurd392.bigdatacontest.subgrapher;

import com.github.aldurd392.bigdatacontest.datatypes.IntArrayWritable;
import com.github.aldurd392.bigdatacontest.datatypes.NeighbourhoodMap;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.util.Iterator;

public class SubgraphReducer extends MapReduceBase
        implements Reducer<IntArrayWritable,NeighbourhoodMap,IntArrayWritable,NeighbourhoodMap> {

        private int numberOfKeys = 0;

        @Override
        public void reduce(IntArrayWritable key, Iterator<NeighbourhoodMap> iterator,
                           OutputCollector<IntArrayWritable, NeighbourhoodMap> outputCollector, Reporter reporter)
                throws IOException {
                numberOfKeys++;

                NeighbourhoodMap map = new NeighbourhoodMap();
                while (iterator.hasNext()) {
                        map.putAll(iterator.next());
                }

                outputCollector.collect(key, map);

                if (numberOfKeys % 10 == 0) {
                        reporter.progress();
                }
        }
}

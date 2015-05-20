package com.github.aldurd392.bigdatacontest.test;

import com.github.aldurd392.bigdatacontest.datatypes.IntArrayWritable;
import com.github.aldurd392.bigdatacontest.datatypes.NeighbourhoodMap;
import com.github.aldurd392.bigdatacontest.utils.Utils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.junit.Assert;
import org.junit.Test;

import java.util.*;

/**
 * BigDataContest - com.github.aldurd392.bigdatacontest.test
 * Created by aldur on 10/05/15.
 */
public class UtilsTest {

    private void smallDensity() {
        HashMap<Writable, Writable> map = new HashMap<>();

        IntWritable[] intWritableArray = new IntWritable[]{
                new IntWritable(2),
                new IntWritable(3),
                new IntWritable(4),
                new IntWritable(6),
        };
        IntArrayWritable arrayWritable = new IntArrayWritable();
        arrayWritable.set(intWritableArray);
        map.put(new IntWritable(1), arrayWritable);

        intWritableArray = new IntWritable[]{
                new IntWritable(1),
                new IntWritable(4),
                new IntWritable(6),
        };
        arrayWritable = new IntArrayWritable();
        arrayWritable.set(intWritableArray);
        map.put(new IntWritable(2), arrayWritable);

        intWritableArray = new IntWritable[]{
                new IntWritable(1),
                new IntWritable(2),
                new IntWritable(5),
                new IntWritable(6),
        };
        arrayWritable = new IntArrayWritable();
        arrayWritable.set(intWritableArray);
        map.put(new IntWritable(4), arrayWritable);

        intWritableArray = new IntWritable[]{
                new IntWritable(1),
                new IntWritable(2),
                new IntWritable(4),
        };
        arrayWritable = new IntArrayWritable();
        arrayWritable.set(intWritableArray);
        map.put(new IntWritable(6), arrayWritable);

        NeighbourhoodMap neighbourhoodMap = new NeighbourhoodMap();
        neighbourhoodMap.putAll(map);

        IntArrayWritable subgraph = new IntArrayWritable();
        subgraph.set(map.keySet().toArray(new Writable[map.size()]));


        final double rho = 6.0 / 4;
        IntArrayWritable densestSubgraph = Utils.density(neighbourhoodMap, rho);
        Assert.assertNotNull(densestSubgraph);

        Writable[] writable_densestSubgraph = densestSubgraph.get();
        HashSet<Writable> set_densestSubgraph = new HashSet<>(Arrays.asList(writable_densestSubgraph));
        HashSet<Writable> set_neighbourhoodMap = new HashSet<>(map.keySet());

        Assert.assertEquals(set_densestSubgraph, set_neighbourhoodMap);
    }

    private void bigDensity() {
        HashMap<Writable, Writable> map = new HashMap<>();

        IntWritable[] intWritableArray = new IntWritable[]{
                new IntWritable(2),
                new IntWritable(3),
                new IntWritable(4),
                new IntWritable(5),
        };
        IntArrayWritable arrayWritable = new IntArrayWritable();
        arrayWritable.set(intWritableArray);
        map.put(new IntWritable(1), arrayWritable);

        intWritableArray = new IntWritable[]{
                new IntWritable(3),
                new IntWritable(4),
                new IntWritable(5),
                new IntWritable(1),
        };
        arrayWritable = new IntArrayWritable();
        arrayWritable.set(intWritableArray);
        map.put(new IntWritable(2), arrayWritable);

        intWritableArray = new IntWritable[]{
                new IntWritable(4),
                new IntWritable(5),
                new IntWritable(1),
                new IntWritable(2),
        };
        arrayWritable = new IntArrayWritable();
        arrayWritable.set(intWritableArray);
        map.put(new IntWritable(3), arrayWritable);

        intWritableArray = new IntWritable[]{
                new IntWritable(5),
                new IntWritable(1),
                new IntWritable(2),
                new IntWritable(3),
        };
        arrayWritable = new IntArrayWritable();
        arrayWritable.set(intWritableArray);
        map.put(new IntWritable(4), arrayWritable);

        intWritableArray = new IntWritable[]{
                new IntWritable(1),
                new IntWritable(2),
                new IntWritable(3),
                new IntWritable(4),
        };
        arrayWritable = new IntArrayWritable();
        arrayWritable.set(intWritableArray);
        map.put(new IntWritable(5), arrayWritable);
        NeighbourhoodMap neighbourhoodMap = new NeighbourhoodMap();
        neighbourhoodMap.putAll(map);

        IntArrayWritable subgraph = new IntArrayWritable();
        subgraph.set(map.keySet().toArray(new Writable[map.size()]));

        final double rho = 2.0;
        IntArrayWritable densestSubgraph = Utils.density(neighbourhoodMap, rho);
        Assert.assertNotNull(densestSubgraph);

        Writable[] writable_densestSubgraph = densestSubgraph.get();
        HashSet<Writable> set_densestSubgraph = new HashSet<>(Arrays.asList(writable_densestSubgraph));
        HashSet<Writable> set_neighbourhoodMap = new HashSet<>(map.keySet());

        Assert.assertEquals(set_densestSubgraph, set_neighbourhoodMap);
    }

    @Test
    public void densityTest() {
        smallDensity();
        bigDensity();
    }

    @Test
    public void euristicFunctionTest() {
        Assert.assertEquals(1, Utils.euristicFactorFunction(1), 10E-5);
        Assert.assertEquals(0.265, Utils.euristicFactorFunction(50), 10E-2);
    }
}

package com.github.aldurd392.bigdatacontest.test;

import com.github.aldurd392.bigdatacontest.datatypes.IntArrayWritable;
import com.github.aldurd392.bigdatacontest.datatypes.NeighbourhoodMap;
import com.github.aldurd392.bigdatacontest.utils.Utils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

/**
 * BigDataContest - com.github.aldurd392.bigdatacontest.test
 * Created by aldur on 10/05/15.
 */
public class UtilsTest {

    private void smallDensity() {
        String neighbourhoodMapString = "{1:[2, 3, 4, 6], 2:[1, 4, 6], 4:[1, 2, 5, 6], 6:[1, 2, 4]}";
        NeighbourhoodMap neighbourhoodMap = NeighbourhoodMap.fromString(neighbourhoodMapString);
        Assert.assertNotNull(neighbourhoodMap);

        IntArrayWritable subgraph = IntArrayWritable.fromString("[1, 2, 4, 6]");
        Assert.assertNotNull(subgraph);

        final double rho = 6.0 / 4;
        IntArrayWritable densestSubgraph = Utils.density(neighbourhoodMap, rho);
        Assert.assertNotNull(densestSubgraph);

        HashSet<Writable> set_densestSubgraph = new HashSet<>(Arrays.asList(densestSubgraph.get()));
        HashSet<Writable> set_neighbourhoodMap = new HashSet<>(Arrays.asList(subgraph.get()));

        Assert.assertEquals(set_densestSubgraph, set_neighbourhoodMap);
    }

    private void bigDensity() {
        String neighbourhoodMapString = "{1:[2, 3, 4, 5], 2:[3, 4, 5, 1], 3:[4, 5, 1, 2], 4:[5, 1, 2, 3], 5:[1, 2, 3, 4]}";
        NeighbourhoodMap neighbourhoodMap = NeighbourhoodMap.fromString(neighbourhoodMapString);
        Assert.assertNotNull(neighbourhoodMap);

        IntArrayWritable subgraph = IntArrayWritable.fromString("[1, 2, 3, 4, 5]");
        Assert.assertNotNull(subgraph);

        final double rho = 2.0;
        IntArrayWritable densestSubgraph = Utils.density(neighbourhoodMap, rho);
        Assert.assertNotNull(densestSubgraph);

        HashSet<Writable> set_densestSubgraph = new HashSet<>(Arrays.asList(densestSubgraph.get()));
        HashSet<Writable> set_neighbourhoodMap = new HashSet<>(Arrays.asList(subgraph.get()));

        Assert.assertEquals(set_densestSubgraph, set_neighbourhoodMap);
    }

    @Test
    public void densityTest() {
        smallDensity();
        bigDensity();
    }

    @Test
    public void IntArrayWritableFromStringTest() {
        IntWritable[] intWritableArray = new IntWritable[]{
                new IntWritable(1),
                new IntWritable(2),
                new IntWritable(3),
                new IntWritable(4),
        };

        IntArrayWritable arrayWritable = new IntArrayWritable();
        arrayWritable.set(intWritableArray);

        String toString = arrayWritable.toString();
        IntArrayWritable fromString = IntArrayWritable.fromString(toString);
        Assert.assertNotNull(fromString);

        IntWritable[] intWritableArrayFromString = (IntWritable[]) fromString.get();
        Assert.assertArrayEquals(intWritableArray, intWritableArrayFromString);
    }

    @Test
    public void NeighbourhoodMapFromStringTest() {
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

        String toString = neighbourhoodMap.toString();
        NeighbourhoodMap fromString = NeighbourhoodMap.fromString(toString);

        Assert.assertNotNull(fromString);
        Assert.assertEquals(map.size(), fromString.size());

        for (Map.Entry<Writable, Writable> entry : fromString.entrySet()) {
            IntWritable intWritableEntryKey = (IntWritable) entry.getKey();
            Assert.assertTrue(map.containsKey(intWritableEntryKey));

            IntArrayWritable intArrayWritableEntryValue = (IntArrayWritable) entry.getValue();
            IntArrayWritable intArrayWritableMap = (IntArrayWritable) map.get(intWritableEntryKey);

            Assert.assertArrayEquals(intArrayWritableEntryValue.get(), intArrayWritableMap.get());
        }
    }
}

package com.github.aldurd392.bigdatacontest.datatypes;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Writable;

public class NeighbourhoodMap extends MapWritable {

    /**
     * Create a NeighbourhoodMap from a String. Useful for our tests.
     * @param string a string of the format outputted by NeighbourhoodMap.toString()
     * @return a new NeighbourhoodMap or null on error.
     */
    public static NeighbourhoodMap fromString(String string) {
        String[] items = string.replaceAll("\\{", "").replaceAll("\\}", "").split("\\], ");

        NeighbourhoodMap map = new NeighbourhoodMap();
        for (String item : items) {
            String[] split_item = item.split(":");

            if (split_item.length != 2) {
                return null;
            }

            try {
                IntWritable key = new IntWritable(Integer.parseInt(split_item[0]));
                IntArrayWritable value = IntArrayWritable.fromString(split_item[1]);

                if (value == null) {
                    return null;
                }

                map.put(key, value);
            } catch (NumberFormatException e) {
                return null;
            }

        }

        return map;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();

        for (Entry<Writable, Writable> entry : this.entrySet()) {
            sb.append(entry.getKey()).append(":").append(entry.getValue()).append(", ");
        }

        return "{" + sb.toString().substring(0, sb.length() - 2) + "}";
    }
}

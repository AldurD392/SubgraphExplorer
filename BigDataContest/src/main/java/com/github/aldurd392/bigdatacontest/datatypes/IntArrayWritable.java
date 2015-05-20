package com.github.aldurd392.bigdatacontest.datatypes;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;

public class IntArrayWritable extends ArrayWritable {

    public IntArrayWritable() {
        super(IntWritable.class);
    }

    /**
     * Create a IntArrayWritable from a String. Useful for our tests.
     * @param string a string of the format outputted by IntArrayWritable.toString()
     * @return a new IntArrayWritable or null on error.
     */
    public static IntArrayWritable fromString(String string) {
        String[] items = string.replaceAll("\\[", "").replaceAll("\\]", "").split(", ");

        IntWritable[] results = new IntWritable[items.length];
        for (int i = 0; i < items.length; i++) {
            try {
                results[i] = new IntWritable(Integer.parseInt(items[i]));
            } catch (NumberFormatException e) {
                return null;
            }
        }

        IntArrayWritable intArrayWritable = new IntArrayWritable();
        intArrayWritable.set(results);

        return intArrayWritable;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();

        for (String s : super.toStrings()) {
            sb.append(s).append(", ");
        }

        String fromSb = sb.toString();
        return "[" + fromSb.substring(0, fromSb.length() - 2) + "]";
    }
}


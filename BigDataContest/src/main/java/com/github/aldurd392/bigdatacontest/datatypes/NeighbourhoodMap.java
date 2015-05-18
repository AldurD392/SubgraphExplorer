package com.github.aldurd392.bigdatacontest.datatypes;

import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Writable;

public class NeighbourhoodMap extends MapWritable {

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();

        for (Entry<Writable, Writable> entry : this.entrySet()) {
            sb.append(entry.getKey()).append(":").append(entry.getValue());
        }

        return "{" + sb.toString() + "}";
    }
}

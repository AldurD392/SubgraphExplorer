/*
 * The MIT License
 *
 * Copyright 2015 aldur.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
package com.github.aldurd392.bigdatacontest.neighbourhood;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class NeighbourMapper extends Mapper<Text, Text, IntWritable, IntWritable> {

    private final IntWritable u = new IntWritable();
    private final IntWritable v = new IntWritable();

    /*
    We require the input file to be formatted as follows:
    - a\tb means an undirected edge between a and b.
    This edge has to appear only once:
    i.e. there must not be b\t a in the input file.
     */
    @Override
    public void map(Text key, Text value, Context context)
            throws IOException, InterruptedException {
        // We ignore lines starting with "#"
        if (key.toString().startsWith("#")) {
            return;
        }

        u.set(Integer.parseInt(key.toString()));
        v.set(Integer.parseInt(value.toString()));

        context.write(u, v);
        context.write(v, u);
    }
}

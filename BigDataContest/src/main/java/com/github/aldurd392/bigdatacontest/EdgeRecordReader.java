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
package com.github.aldurd392.bigdatacontest;

import java.io.IOException;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 *
 * @author aldur
 */
public class EdgeRecordReader extends RecordReader {

	@Override
	public void initialize(InputSplit is, TaskAttemptContext tac) 
		throws IOException, InterruptedException {
		throw new UnsupportedOperationException("Not supported yet."); 
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		throw new UnsupportedOperationException("Not supported yet.");
	}

	@Override
	public Object getCurrentKey() throws IOException, InterruptedException {
		throw new UnsupportedOperationException("Not supported yet.");
	}

	@Override
	public Object getCurrentValue() throws IOException, InterruptedException {
		throw new UnsupportedOperationException("Not supported yet.");
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		throw new UnsupportedOperationException("Not supported yet.");
	}

	@Override
	public void close() throws IOException {
		throw new UnsupportedOperationException("Not supported yet.");
	}
}

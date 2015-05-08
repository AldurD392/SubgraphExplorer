package DegreesFilter;

import java.io.IOException;

import org.apache.hadoop.io.ShortWritable;
import org.apache.hadoop.mapreduce.Mapper;

import CustomTypes.LongWritableArray;

public class EdgesCountMapper extends Mapper<LongWritableArray, ShortWritable, LongWritableArray, ShortWritable> {

	public void map(LongWritableArray key, ShortWritable value, Context context) throws IOException, InterruptedException {
		context.write(key, value);
	}	
}

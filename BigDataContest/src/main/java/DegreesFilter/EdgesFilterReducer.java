package DegreesFilter;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.ShortWritable;
import org.apache.hadoop.mapreduce.Reducer;

import CustomTypes.LongWritableArray;

public class EdgesFilterReducer extends Reducer<LongWritableArray, ShortWritable, LongWritable, LongWritable> {

    public void reduce(LongWritableArray key, Iterable<ShortWritable> values, Context context) throws IOException, InterruptedException {
    		
    	int sum = 0;
        for (ShortWritable one : values) {
        		sum += one.get();
    		}
        
        if(sum == 2){
        		
        		LongWritable[] a = (LongWritable[]) key.get();
        		context.write(a[0], a[1]);
        }
    }
}

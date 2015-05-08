package DegreesFilter;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.ShortWritable;
import org.apache.hadoop.mapreduce.Reducer;

import CustomTypes.LongWritableArray;


public class NeighbourReducer extends Reducer<LongWritable, LongWritable, LongWritableArray, ShortWritable> {
	
	private final static ShortWritable one = new ShortWritable((short)1);
	private LongWritableArray pair = new LongWritableArray();
	private ArrayList<LongWritable> itCache = null;

    public void reduce(LongWritable key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        
    		int size = 0;
    		itCache = new ArrayList<LongWritable>();
        for (LongWritable node : values) {
        		size++;
        		itCache.add(node);
		}
        
        if(size > 1){
        		
        		for (LongWritable v : itCache){
        			
        			if(v.get() < key.get()){
        				LongWritable[] a = {v , key};
        				pair.set(a);

        			} else {
        				LongWritable[] a = {key , v};
        				pair.set(a);
        			}
        			context.write(pair, one);
        		}
        }
    }
}

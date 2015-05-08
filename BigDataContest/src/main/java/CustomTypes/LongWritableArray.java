package CustomTypes;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.LongWritable;

public class LongWritableArray extends ArrayWritable{
	 
    public LongWritableArray(){
    		super(LongWritable.class);
    	}
    
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        for (String s : super.toStrings()) {
            sb.append(s).append(" ");
        }

        return "[" + sb.toString() + "]";
    }
}


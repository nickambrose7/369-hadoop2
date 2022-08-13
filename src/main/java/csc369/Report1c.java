package csc369;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;
import java.util.ArrayList;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.junit.internal.runners.SuiteMethod;

public class Report1c {
	public static final Class OUTPUT_KEY_CLASS = IntWritable.class;
    public static final Class OUTPUT_VALUE_CLASS = Text.class; 

    public static class MapperImpl extends Mapper<LongWritable, Text, IntWritable, Text> {

        @Override
	protected void map(LongWritable key, Text value, // map function args: (line in the file, string at that line)
			   Context context) throws IOException, InterruptedException {
        // this is all just getting our info from the last output file.
	    String[] sa = value.toString().split("\t"); // gets us the hostname context split it by tabs.
	    Text country = new Text();
		country.set(sa[0]);
        IntWritable count = new IntWritable(Integer.parseInt(sa[1]));
	    context.write(count, country); 
        }
    }

    
    public static class ReducerImpl extends Reducer<IntWritable, Text, Text, IntWritable> {
	private IntWritable result = new IntWritable();
        @Override
	protected void reduce(IntWritable count, Iterable<Text> country,
			      Context context) throws IOException, InterruptedException {
            Iterator<Text> itr = country.iterator();
            while (itr.hasNext()) {
                result.set(count.get() * -1); // need to negate so that we sort decending.
			    context.write(itr.next(), result);// write out county and flipped result.
            }
			
        }
    }


}

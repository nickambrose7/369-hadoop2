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

public class Report1b {
	public static final Class OUTPUT_KEY_CLASS = Text.class; 
    public static final Class OUTPUT_VALUE_CLASS = IntWritable.class;

    public static class MapperImpl extends Mapper<LongWritable, Text, Text, IntWritable> {

        @Override
	protected void map(LongWritable key, Text value, // map function args: (line in the file, string at that line)
			   Context context) throws IOException, InterruptedException {
        // this is all just getting our info from the last output file.
	    String[] sa = value.toString().split("\t"); // gets us the hostname context split it by tabs.
	    Text country = new Text();
		country.set(sa[0]);
        IntWritable count = new IntWritable(Integer.parseInt(sa[1]));
	    context.write(country, count); 
        }
    }

    
    public static class ReducerImpl extends Reducer<Text, IntWritable, Text, IntWritable> {
	private IntWritable result = new IntWritable();
        @Override
	protected void reduce(Text country, Iterable<IntWritable> counts,
			      Context context) throws IOException, InterruptedException {
			int sum = 0;
            Iterator<IntWritable> itr = counts.iterator();
            while (itr.hasNext()) {
                sum  += itr.next().get();
            }
			result.set(sum * -1); // need to negate so that we sort decending.
			context.write(country, result);
        }
    }


}

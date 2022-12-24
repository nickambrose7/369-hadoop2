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

public class Report1 {
	// order does matter for the command line.
	// Both mappers must emit the same type!!!
	// These below must line up with the mapper output!
    public static final Class OUTPUT_KEY_CLASS = Text.class;
    public static final Class OUTPUT_VALUE_CLASS = Text.class;

    // SHOULD BE USING THIS ONE FOR THE CSV Mapper for hostname.csv file
    public static class HostNameMapper extends Mapper<Text, Text, Text, Text> {
	@Override
        public void map(Text key, Text value, Context context)  throws IOException, InterruptedException {
	    context.write(key, value); // emit(hostname, Country) 
	} 
    }

    //  this if for the access.log
    public static class AccessLogMapper extends Mapper<LongWritable, Text, Text, Text> {
	@Override
        public void map(LongWritable key, Text value, Context context)  throws IOException, InterruptedException {
		String text[] = value.toString().split(" "); //split on space.
		String hostname = text[0]; // hostname is the first item
		String one = "1"; // need to emit a 1
		context.write(new Text(hostname), new Text(one));// emit(Hostname, 1) text format
	}
    }


    //  Reducer: just one reducer class to perform the "join"
    public static class JoinReducer extends  Reducer<Text, Text, Text, IntWritable> {

	@Override
	    public void reduce(Text key, Iterable<Text> values, Context context)  throws IOException, InterruptedException {
			
			
			// for (Text val : values) {
			// 	context.write(key, val);
			// }
			// ArrayList<String> name = new ArrayList();
			// ArrayList<String> messages = new ArrayList();
			Text country = new Text(); // will hold our county info
			int request_count = 0; // keep track of how many ones are in values.
			for (Text val : values) {
				if (val.toString().equals("1")) { //if value == "1"
					request_count += 1; // add to request counter
				}
				else {
					country.set(val); // we have found our country name
				}
			}
			IntWritable out = new IntWritable();
			out.set(request_count); // set our request count to output
			context.write(country, out);
		}	
    } 


}

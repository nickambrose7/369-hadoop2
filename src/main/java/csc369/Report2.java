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

public class Report2 {
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
		String url = text[6]; // Url is the 6th item 
		context.write(new Text(hostname), new Text(url));// emit(Hostname, URL) text format
	}
    }


    //  Reducer: just one reducer class to perform the "join"
    public static class JoinReducer extends  Reducer<Text, Text, Text, Text> {

	@Override
	    public void reduce(Text key, Iterable<Text> values, Context context)  throws IOException, InterruptedException {
			
			
			// for (Text val : values) {
			// 	context.write(key, val);
			// }
			// ArrayList<String> name = new ArrayList();
			// ArrayList<String> messages = new ArrayList();
			Text country = new Text(); // will hold our county info
			boolean is_country = false;
			boolean is_URL = false;
			for (Text val : values) { // check that country and url are present.
				if (val.toString().substring(0, 1).equals("/")) { // if no / then its a country
					is_URL = true;
				}
				else {
					is_country = true;
					country.set(val);// will be only one country per hostname.
				}
				if (is_URL && is_country) {
					break;
				}
			}
			if (is_URL && is_country) {// only care when theres a URL and country
				for (Text val : values) {
					if (val.toString().substring(0, 1).equals("/")) {// we want to write bc url
						context.write(country, val);
					}
				}// this for loop doesn't write when val is a country.
			}
			
		}	
    } 


}

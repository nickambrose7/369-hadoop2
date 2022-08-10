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
    public static final Class OUTPUT_KEY_CLASS = Text.class;
    public static final Class OUTPUT_VALUE_CLASS = Text.class;

    // SHOULD BE USING THIS ONE FOR THE CSV Mapper for hostname.csv file
    public static class UserMapper extends Mapper<Text, Text, Text, Text> {
	@Override
        public void map(Text key, Text value, Context context)  throws IOException, InterruptedException {
	    context.write(key, value); // can just output key bc it maps to line number.
	} 
    }

    //  this if for the access.log
    public static class MessageMapper extends Mapper<LongWritable, Text, Text, Text> {
	@Override
        public void map(LongWritable key, Text value, Context context)  throws IOException, InterruptedException {
		String text[] = value.toString().split(" ");
		String hostname = text[0];
		String one = "1";
		context.write(new Text(hostname), new Text(one));
	}
    }


    //  Reducer: just one reducer class to perform the "join"
    public static class JoinReducer extends  Reducer<Text, Text, Text, Text> {

	@Override
	    public void reduce(Text key, Iterable<Text> values, Context context)  throws IOException, InterruptedException {
			
			
			for (Text val : values) {
				context.write(key, val);
			}// ArrayList<String> name = new ArrayList();
			// ArrayList<String> messages = new ArrayList();
			// Text country = new Text();
			// int sum = 0;
			// Text comp = new Text();
			// comp.set("1");
			// for (Text val : values) {
			// 	if (val.equals(comp)) {
			// 		sum += 1;
			// 	}
			// 	else {
			// 		country.set(val);
			// 	}
			// }
			// IntWritable out = new IntWritable();
			// out.set(sum);
			// context.write(country, out);
		}	
    } 


}

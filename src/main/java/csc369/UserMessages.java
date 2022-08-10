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

public class UserMessages {

    public static final Class OUTPUT_KEY_CLASS = Text.class;
    public static final Class OUTPUT_VALUE_CLASS = Text.class;

    // Mapper for User file, ILLISTRATION OF THE TWO DIFFERNT WAYS YOU CAN TAKE INPUT FROM A CSV
	// This one will take input from csv and the first item is the key and the second comma separted 
	// item is the value.
    public static class UserMapper extends Mapper<Text, Text, Text, Text> {
	@Override
        public void map(Text key, Text value, Context context)  throws IOException, InterruptedException {
	    String name = value.toString();
	    String out = "A\t"+name;
	    context.write(key, new Text(out)); // can just output key bc it maps to line number.
	} 
    }

    // Mapper for messages file
    public static class MessageMapper extends Mapper<LongWritable, Text, Text, Text> {
		// This is where the key is the line number and the value is the line, so we split by comma.
	@Override
        public void map(LongWritable key, Text value, Context context)  throws IOException, InterruptedException {
	    String text[] = value.toString().split(","); // split two values by comma
	    if (text.length == 2) { //if valid line
		String id = text[0]; //id is the first item
		String message = text[1]; // message is the second item
		String out = "B\t"+ message; // we use b to denote the second mapper.
		context.write(new Text(id), new Text(out)); // write the id with B + the message
	    }
	}
    }
	//  Reducer: just one reducer class to perform the "join"
    // how will things arrive to the reducer?
	// Default is reducer sorts based on key in ascending order. 
	public static class JoinReducer extends  Reducer<Text, Text, Text, Text> {

	@Override
	    public void reduce(Text key, Iterable<Text> values, Context context)  throws IOException, InterruptedException {
	    ArrayList<String> name = new ArrayList();
	    ArrayList<String> messages = new ArrayList();

	    for (Text val : values) {
			context.write(key, val);
	    }
	}
    } 


}

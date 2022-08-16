package csc369;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Partitioner;

public class Report2c {

    public static final Class OUTPUT_KEY_CLASS = CountryCountPair.class;
    public static final Class OUTPUT_VALUE_CLASS = Text.class;

    // read a text file that contains (y, m, d, temperature) readings
    public static class MapperImpl extends Mapper<LongWritable, Text, CountryCountPair, Text> {
          @Override
          public void map(LongWritable key, Text value, Context
                          context) throws IOException, InterruptedException {
                String line = value.toString();
                String[] tokens = line.split(";"); // index 0 is country, index 1 is url and count
                String[] tokens2 = tokens[1].split("\t"); // index 0 is URL index 1 is count

                String count = tokens2[1];
                String country = tokens[0];
                String url = tokens2[0];
                context.write(new CountryCountPair(country, count), new Text(url));
                //context.write(new Text("This is tokens 1: "), new Text(url));
            }
    }
    
    public static class ReducerImpl extends Reducer<CountryCountPair, Text, Text, IntWritable> {

            @Override
            protected void reduce(CountryCountPair key,
                                  Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
                for (Text value : values) {
                    context.write(new Text(key.getCountry() + " " + value), key.getCount());
                    //context.write(key, value);
                }
            }
    }

}

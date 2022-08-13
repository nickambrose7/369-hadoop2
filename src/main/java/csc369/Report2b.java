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

public class Report2b {

    public static final Class OUTPUT_KEY_CLASS = UrlCountryPair.class;
    public static final Class OUTPUT_VALUE_CLASS = IntWritable.class;

    // read a text file that contains (y, m, d, temperature) readings
    public static class MapperImpl extends Mapper<LongWritable, Text, UrlCountryPair, IntWritable> {
          @Override
          public void map(LongWritable key, Text value, Context
                          context) throws IOException, InterruptedException {
                String line = value.toString();
                String[] tokens = line.split("\t");

                String url = tokens[1];
                String country = tokens[0];
                context.write(new UrlCountryPair(url, country), new IntWritable(1));
            }
    }
    

    // // controls the reducer to which a particular (key, value) is sent
    // public static class PartitionerImpl extends Partitioner<UrlCountryPair, IntWritable> {
    //     @Override
    //     public int getPartition(UrlCountryPair pair,
    //                             IntWritable temperature,
    //                             int numberOfPartitions) {
    //         return Math.abs((pair.getUrl().hashCode() + pair.getCountry().hashCode()) % numberOfPartitions);
    //     } // Will this all go to one reducer?
    // }
    
    // // used to group (year,month,day) data by (year,month)
    // public static class GroupingComparator extends WritableComparator {
    //     public GroupingComparator() {
    //         super(UrlCountryPair.class, true);
    //     }
        
    //     @Override
    //     public int compare(WritableComparable wc1,
    //                        WritableComparable wc2) {
    //         UrlCountryPair pair = (UrlCountryPair) wc1;
    //         UrlCountryPair pair2 = (UrlCountryPair) wc2;
    //         return pair.getCountry().compareTo(pair2.getCountry());
    //     }
    // }

    // // used to perform secondary sort on temperature
    // public static class SortComparator extends WritableComparator {
    //     protected SortComparator() {
    //         super(UrlCountryPair.class, true);
    //     }
        
    //     @Override
    //     public int compare(WritableComparable wc1,
    //                        WritableComparable wc2) {
    //         UrlCountryPair pair = (UrlCountryPair) wc1;
    //         UrlCountryPair pair2 = (UrlCountryPair) wc2;
    //         return pair.compareTo(pair2);
    //     }
    // }
    
    // output one line for each month, with the temperatures sorted for that month
    public static class ReducerImpl extends Reducer<UrlCountryPair, IntWritable, Text, IntWritable> {

            @Override
            protected void reduce(UrlCountryPair key,
                                  Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
                int sum = 0;
                for (IntWritable value : values) {
                    sum += value.get(); // Need to sum all the 1's up
                }
                
                context.write(new Text(key.getCountry() + " " + key.getUrl()), new IntWritable(sum));
            }
    }

}

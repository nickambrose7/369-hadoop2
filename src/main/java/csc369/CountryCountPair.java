package csc369;

import java.io.IOException;
import java.io.DataInput;
import java.io.DataOutput;
import org.apache.hadoop.io.*;

public class CountryCountPair
    implements Writable, WritableComparable<CountryCountPair> {
    
    private final Text country = new Text();
    private final IntWritable count = new IntWritable();
    
    public CountryCountPair() {
    }
    
    public CountryCountPair(String country, String count) {
        this.count.set(Integer.parseInt(count));
        this.country.set(country);
    }
    
    @Override
    public void write(DataOutput out) throws IOException{
        count.write(out);
        country.write(out);
    }
    
    @Override
    public void readFields(DataInput in) throws IOException {
        count.readFields(in);
        country.readFields(in);
    }
    
    @Override
    public int compareTo(CountryCountPair pair) {
        if (country.compareTo(pair.getCountry()) == 0) {
            return -1 * count.compareTo(pair.getCount());
        }
        return country.compareTo(pair.getCountry());
    }
    
    public IntWritable getCount() {
        return count;
    }
    
    public Text getCountry() {
        return country;
    }
    
}

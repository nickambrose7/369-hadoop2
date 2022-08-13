package csc369;

import java.io.IOException;
import java.io.DataInput;
import java.io.DataOutput;
import org.apache.hadoop.io.*;

public class UrlCountryPair
    implements Writable, WritableComparable<UrlCountryPair> {
    
    private final Text url = new Text();
    private final Text country = new Text();
    
    public UrlCountryPair() {
    }
    
    public UrlCountryPair(String url, String country) {
        this.url.set(url);
        this.country.set(country);
    }
    
    @Override
    public void write(DataOutput out) throws IOException{
        url.write(out);
        country.write(out);
    }
    
    @Override
    public void readFields(DataInput in) throws IOException {
        url.readFields(in);
        country.readFields(in);
    }
    
    @Override
    public int compareTo(UrlCountryPair pair) {
        if (url.compareTo(pair.getUrl()) == 0) {
            return country.compareTo(pair.country);
        }
        return url.compareTo(pair.getUrl());
    }
    
    public Text getUrl() {
        return url;
    }
    
    public Text getCountry() {
        return country;
    }
    
}

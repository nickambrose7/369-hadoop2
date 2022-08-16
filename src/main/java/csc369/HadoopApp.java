package csc369;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;

public class HadoopApp {

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();
        conf.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator",",");
        
        Job job = new Job(conf, "Hadoop example");
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

	if (otherArgs.length < 3) {
	    System.out.println("Expected parameters: <job class> [<input dir>]+ <output dir>");
	    System.exit(-1);
	} else if ("UserMessages".equalsIgnoreCase(otherArgs[0])) {

	    MultipleInputs.addInputPath(job, new Path(otherArgs[1]),
					KeyValueTextInputFormat.class, UserMessages.UserMapper.class );
	    MultipleInputs.addInputPath(job, new Path(otherArgs[2]),
					TextInputFormat.class, UserMessages.MessageMapper.class ); 

	    job.setReducerClass(UserMessages.JoinReducer.class);

	    job.setOutputKeyClass(UserMessages.OUTPUT_KEY_CLASS);
	    job.setOutputValueClass(UserMessages.OUTPUT_VALUE_CLASS);
	    FileOutputFormat.setOutputPath(job, new Path(otherArgs[3]));

	} else if ("WordCount".equalsIgnoreCase(otherArgs[0])) {
	    job.setReducerClass(WordCount.ReducerImpl.class);
	    job.setMapperClass(WordCount.MapperImpl.class);
	    job.setOutputKeyClass(WordCount.OUTPUT_KEY_CLASS);
	    job.setOutputValueClass(WordCount.OUTPUT_VALUE_CLASS);
	    FileInputFormat.addInputPath(job, new Path(otherArgs[1]));
	    FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));
	} else if ("AccessLog".equalsIgnoreCase(otherArgs[0])) {
	    job.setReducerClass(AccessLog.ReducerImpl.class);
	    job.setMapperClass(AccessLog.MapperImpl.class);
	    job.setOutputKeyClass(AccessLog.OUTPUT_KEY_CLASS);
	    job.setOutputValueClass(AccessLog.OUTPUT_VALUE_CLASS);
	    FileInputFormat.addInputPath(job, new Path(otherArgs[1]));
	    FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));
	} else if ("Report1".equalsIgnoreCase(otherArgs[0])) {

	    MultipleInputs.addInputPath(job, new Path(otherArgs[1]),
					KeyValueTextInputFormat.class, Report1.HostNameMapper.class );
	    MultipleInputs.addInputPath(job, new Path(otherArgs[2]),
					TextInputFormat.class, Report1.AccessLogMapper.class ); 

	    job.setReducerClass(Report1.JoinReducer.class);

	    job.setOutputKeyClass(Report1.OUTPUT_KEY_CLASS);
	    job.setOutputValueClass(Report1.OUTPUT_VALUE_CLASS);
	    FileOutputFormat.setOutputPath(job, new Path(otherArgs[3]));

	} else if ("Report1b".equalsIgnoreCase(otherArgs[0])) { // same as lab1
		job.setReducerClass(Report1b.ReducerImpl.class);
		job.setMapperClass(Report1b.MapperImpl.class);
		job.setOutputKeyClass(Report1b.OUTPUT_KEY_CLASS);
		job.setOutputValueClass(Report1b.OUTPUT_VALUE_CLASS);
		FileInputFormat.addInputPath(job, new Path(otherArgs[1])); // must add input and output path
	    FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));
	} else if ("Report1c".equalsIgnoreCase(otherArgs[0])) { // same as lab1
		job.setReducerClass(Report1c.ReducerImpl.class);
		job.setMapperClass(Report1c.MapperImpl.class);
		job.setOutputKeyClass(Report1c.OUTPUT_KEY_CLASS);
		job.setOutputValueClass(Report1c.OUTPUT_VALUE_CLASS);
		FileInputFormat.addInputPath(job, new Path(otherArgs[1])); // must add input and output path
	    FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));
	} else if ("Report2".equalsIgnoreCase(otherArgs[0])) {

	    MultipleInputs.addInputPath(job, new Path(otherArgs[1]),
					KeyValueTextInputFormat.class, Report2.HostNameMapper.class );
	    MultipleInputs.addInputPath(job, new Path(otherArgs[2]),
					TextInputFormat.class, Report2.AccessLogMapper.class ); 

	    job.setReducerClass(Report2.JoinReducer.class);

	    job.setOutputKeyClass(Report2.OUTPUT_KEY_CLASS);
	    job.setOutputValueClass(Report2.OUTPUT_VALUE_CLASS);
	    FileOutputFormat.setOutputPath(job, new Path(otherArgs[3]));

	} else if ("Report2b".equalsIgnoreCase(otherArgs[0])) {
	    job.setReducerClass(Report2b.ReducerImpl.class);
	    job.setMapperClass(Report2b.MapperImpl.class);        
	    job.setOutputKeyClass(Report2b.OUTPUT_KEY_CLASS);
	    job.setOutputValueClass(Report2b.OUTPUT_VALUE_CLASS);
	    FileInputFormat.addInputPath(job, new Path(otherArgs[1]));
	    FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));
	} else if ("Report2c".equalsIgnoreCase(otherArgs[0])) {
	    job.setReducerClass(Report2c.ReducerImpl.class);
	    job.setMapperClass(Report2c.MapperImpl.class);        
	    job.setOutputKeyClass(Report2c.OUTPUT_KEY_CLASS);
	    job.setOutputValueClass(Report2c.OUTPUT_VALUE_CLASS);
	    FileInputFormat.addInputPath(job, new Path(otherArgs[1]));
	    FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));
	}
	else {
	    System.out.println("Unrecognized job: " + otherArgs[0]);
	    System.exit(-1);
	}
        System.exit(job.waitForCompletion(true) ? 0: 1);
    }

}

import java.io.*;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;


public class Twitter {
    public static  class ARD5625Mapper1 extends Mapper<Object,Text,IntWritable,IntWritable>{
	    @Override
	    public void map(Object key, Text value, Context context )
	  throws IOException, InterruptedException {

	    Scanner s = new Scanner(value.toString()).useDelimiter(",");
	    int id = s.nextInt();
	    int follower_id  = s.nextInt();
	    context.write(new IntWritable(follower_id), new IntWritable(id));
	    s.close();
    }
    }


    public static class ARD5625Reducer1 extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable>{
	    @Override
	    public void reduce( IntWritable follower_id, Iterable<IntWritable> ids, Context context ) throws IOException, InterruptedException {
		   int  count = 0;
		   for(IntWritable n : ids){
			   count++;
		   }
		   context.write(follower_id, new IntWritable(count));
	    }
    }

    public static class ARD5625Mapper2 extends Mapper<Object,Text,IntWritable,IntWritable>{
	    @Override
	    public void map (Object key, Text value, Context context ) throws IOException, InterruptedException {
		    Scanner s = new Scanner(value.toString()).useDelimiter("\t");
		    int follower_id = s.nextInt();
		    int count = s.nextInt();
		    context.write(new IntWritable(count),new IntWritable(1));
		    s.close();
	    }
    }

    public static class ARD5625Reducer2 extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable>{
	   @Override
	   public void reduce( IntWritable count, Iterable<IntWritable> values, Context context ) throws IOException, InterruptedException {
		   int sum = 0;
		   for(IntWritable v : values){
			   sum = sum + v.get();
		   }
		   context.write(count, new IntWritable(sum));
	   } 
    }

    public static void main ( String[] args ) throws Exception {
	    Job ard5625_job1 = Job.getInstance();
	    ard5625_job1.setJobName("ARD5625_Job1");
	    ard5625_job1.setJarByClass(Twitter.class);
	    ard5625_job1.setOutputKeyClass(IntWritable.class);
	    ard5625_job1.setOutputValueClass(IntWritable.class);
	    ard5625_job1.setMapOutputKeyClass(IntWritable.class);
	    ard5625_job1.setMapOutputValueClass(IntWritable.class);
	    ard5625_job1.setMapperClass(ARD5625Mapper1.class);
	    ard5625_job1.setReducerClass(ARD5625Reducer1.class);
	    ard5625_job1.setInputFormatClass(TextInputFormat.class);
	    ard5625_job1.setOutputFormatClass(TextOutputFormat.class);
	    FileInputFormat.setInputPaths(ard5625_job1, new Path(args[0]));
	    FileOutputFormat.setOutputPath(ard5625_job1, new Path(args[1]));
	    ard5625_job1.waitForCompletion(true);

	    Job ard5625_job2 = Job.getInstance();
	    ard5625_job2.setJobName("ARD5625_Job2");
	    ard5625_job2.setJarByClass(Twitter.class);
	    ard5625_job2.setOutputKeyClass(IntWritable.class);
	    ard5625_job2.setOutputValueClass(IntWritable.class);
	    ard5625_job2.setMapOutputKeyClass(IntWritable.class);
	    ard5625_job2.setMapOutputValueClass(IntWritable.class);
	    ard5625_job2.setMapperClass(ARD5625Mapper2.class);
	    ard5625_job2.setReducerClass(ARD5625Reducer2.class);
	    ard5625_job2.setInputFormatClass(TextInputFormat.class);
	    ard5625_job2.setOutputFormatClass(TextOutputFormat.class);
	    FileInputFormat.setInputPaths(ard5625_job2, new Path(args[1]));
	    FileOutputFormat.setOutputPath(ard5625_job2, new Path(args[2]));
	    ard5625_job2.waitForCompletion(true);
    }
}

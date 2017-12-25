package cs.Lab2.TreesParis;


import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
//import org.apache.hadoop.mapreduce.Mapper;
//import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.ToolRunner;

public class NumberTypeTreeDriver extends Configured implements Tool{
	public static void main(String[] args) throws Exception {
	      System.out.println(Arrays.toString(args));
	      int res = ToolRunner.run(new Configuration(), new NumberTypeTreeDriver(), new String [] {"/home/cloudera/Downloads/arbres.csv", "/home/cloudera/arbres"});
	      System.exit(res);
	   }

	   @Override
	   public int run(String[] args) throws Exception {
	      System.out.println(Arrays.toString(args));
	      Job job = new Job(getConf(), "NumberTypeTree");
	      job.setJarByClass(NumberTypeTreeDriver.class);
	      job.setOutputKeyClass(Text.class);
	      job.setOutputValueClass(IntWritable.class);

	      job.setMapperClass(NumberTypeTreeMap.class);
	      job.setReducerClass(NumberTypeTreeReduce.class);

	      job.setInputFormatClass(TextInputFormat.class);
	      job.setOutputFormatClass(TextOutputFormat.class);

	      FileInputFormat.addInputPath(job, new Path(args[0]));
	      FileOutputFormat.setOutputPath(job, new Path(args[1]));

	      job.waitForCompletion(true);
	      
	      return 0;
	   }
}
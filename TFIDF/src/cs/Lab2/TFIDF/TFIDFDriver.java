package cs.Lab2.TFIDF;
	
	import java.util.Arrays;

	import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.io.DoubleWritable;
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

	public class TFIDFDriver extends Configured implements Tool{
	
	public static void main(String[] args) throws Exception {
		      System.out.println(Arrays.toString(args));
		      int res = ToolRunner.run(new Configuration(), new TFIDFDriver(), new String [] {"/home/cloudera/texts", "/home/cloudera/TFIDFresults"});
		      System.exit(res);
		   }

		   @Override
		   public int run(String[] args) throws Exception {
		      System.out.println(Arrays.toString(args));
		      Job job1 = new Job(getConf(), "WordCount");
		      
		      Path outputFilePath1 = new Path("/home/cloudera/path1");
		      Path outputFilePath2 = new Path("/home/cloudera/path2");
		      Path outputFilePath3 = new Path("/home/cloudera/path3");
			    
		    
		      job1.setJarByClass(TFIDFDriver.class);
		      job1.setOutputKeyClass(Text.class);
		      job1.setOutputValueClass(IntWritable.class);

		      job1.setMapperClass(Round1Map.class);
		      job1.setReducerClass(Round1Reduce.class);

		      job1.setInputFormatClass(TextInputFormat.class);
		      job1.setOutputFormatClass(TextOutputFormat.class);

		      FileInputFormat.addInputPath(job1, new Path(args[0]));
		      FileOutputFormat.setOutputPath(job1, outputFilePath1);
		      
		      job1.waitForCompletion(true);
		      
		     
		      
		      Job job2 = new Job(getConf(), "WordCount");
		      job2.setJarByClass(TFIDFDriver.class);
		      job2.setOutputKeyClass(Text.class);
		      job2.setOutputValueClass(Text.class);

		      job2.setMapperClass(Round2Map.class);
		      job2.setReducerClass(Round2Reduce.class);

		      job2.setInputFormatClass(TextInputFormat.class);
		      job2.setOutputFormatClass(TextOutputFormat.class);

		      FileInputFormat.addInputPath(job2, outputFilePath1);
		      FileOutputFormat.setOutputPath(job2, outputFilePath2);

		      job2.waitForCompletion(true);
		      
		      Job job3 = new Job(getConf(), "WordCount");
		      job3.setJarByClass(TFIDFDriver.class);
		      job3.setOutputKeyClass(Text.class);
		      job3.setOutputValueClass(Text.class);

		      job3.setMapperClass(Round3Map.class);
		      job3.setReducerClass(Round3Reduce.class);

		      job3.setInputFormatClass(TextInputFormat.class);
		      job3.setOutputFormatClass(TextOutputFormat.class);

		      FileInputFormat.addInputPath(job3, outputFilePath2);
		      FileOutputFormat.setOutputPath(job3, outputFilePath3);

		      job3.waitForCompletion(true);
		      
		      Job job4 = new Job(getConf(), "WordCount");
		      job4.setJarByClass(TFIDFDriver.class);
		      job4.setOutputKeyClass(DoubleWritable.class);
		      job4.setOutputValueClass(Text.class);

		      job4.setMapperClass(SortTFIDFMap.class);
		      job4.setReducerClass(SortTFIDFReduce.class);

		      job4.setInputFormatClass(TextInputFormat.class);
		      job4.setOutputFormatClass(TextOutputFormat.class);

		      FileInputFormat.addInputPath(job4, outputFilePath3);
		      FileOutputFormat.setOutputPath(job4, new Path(args[1]));

		      job4.waitForCompletion(true);
		      
		     FileSystem fs = FileSystem.newInstance(getConf());
		     if (fs.exists(outputFilePath1))
		      {fs.delete(outputFilePath1,true);};
			 if (fs.exists(outputFilePath2))
			      {fs.delete(outputFilePath2,true);};
			 if (fs.exists(outputFilePath3))
			    {fs.delete(outputFilePath3,true);};
		      
		      return 0;
		    
		   }
	}


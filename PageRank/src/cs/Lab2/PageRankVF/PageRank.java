package cs.Lab2.PageRankVF;


import java.io.IOException;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class PageRank extends Configured implements Tool{
    
    // utility attributes
   // public static NumberFormat NF = new DecimalFormat("00");
    public static Set<String> NODES = new HashSet<String>();
    public static String LINKS_SEPARATOR = "|";
    
    // configuration values
    public static Double DAMPING = 0.85;
    public static int ITERATIONS = 10;
    public static String IN_PATH = "";
    public static String OUT_PATH = "";
    
    
    /**
     * This is the main class run against the Hadoop cluster.
     * It will run all the jobs needed for the PageRank algorithm.
     */
    public static void main(String[] args) throws Exception {
		      System.out.println(Arrays.toString(args));
		      int res = ToolRunner.run(new Configuration(), new PageRank(), new String [] {"/home/cloudera/inputPR", "/home/cloudera/PRanky"});
		      // to test : args is new String [] {"/home/cloudera/inputPR", "/home/cloudera/PRank"});
		      System.exit(res);
		   }

		   public int run(String[] args) throws Exception {
				      
		      System.out.println(Arrays.toString(args));
		      Job job1 = new Job(getConf(), "PR");
		      
		      Path outputFilePath1 = new Path("/home/cloudera/iter0");
		      
			    
		    
		      job1.setJarByClass(PageRank.class);
		      job1.setOutputKeyClass(Text.class);
		      job1.setOutputValueClass(Text.class);

		      job1.setMapperClass(Round1Map.class);
		      job1.setReducerClass(Round1Reduce.class);

		      job1.setInputFormatClass(TextInputFormat.class);
		      job1.setOutputFormatClass(TextOutputFormat.class);

		      FileInputFormat.addInputPath(job1, new Path(args[0]));
		      FileOutputFormat.setOutputPath(job1, outputFilePath1);
		      
		      job1.waitForCompletion(true);
		     String str = "/home/cloudera/iter";
		     for (int i = 0; i < ITERATIONS; i++) 
		    {
		   String str1 = new String(str + Integer.toString(i+1));
		      Path outputFilePath2 = new Path(str1);
		      
		      Job job2 = new Job(getConf(), "PR");
		      job2.setJarByClass(PageRank.class);
		      job2.setOutputKeyClass(Text.class);
		      job2.setOutputValueClass(Text.class);

		      job2.setMapperClass(Round2Map.class);
		      job2.setReducerClass(Round2Reduce.class);

		      job2.setInputFormatClass(TextInputFormat.class);
		      job2.setOutputFormatClass(TextOutputFormat.class);

		      FileInputFormat.addInputPath(job2, outputFilePath1);
		      FileOutputFormat.setOutputPath(job2, outputFilePath2);
		     outputFilePath1 = new Path(str1);
		      job2.waitForCompletion(true);
		      
		      
		    }
		      
		      
		      Job job3 = new Job(getConf(), "PR");
		      job3.setJarByClass(PageRank.class);
		      job3.setOutputKeyClass(DoubleWritable.class);
		      job3.setOutputValueClass(Text.class);

		      job3.setMapperClass(Round3Map.class);
		     
		      job3.setInputFormatClass(TextInputFormat.class);
		      job3.setOutputFormatClass(TextOutputFormat.class);

		      FileInputFormat.addInputPath(job3, outputFilePath1);
		      FileOutputFormat.setOutputPath(job3, new Path(args[1]));

		      job3.waitForCompletion(true);

		      
		      return 0;
		    
		   }
		   //could add a delete paths option
}
package cs.Lab2.TreesParis;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class HighestTreeTypeReduce extends Reducer<Text, Text, Text,DoubleWritable>{
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException{
		double heightmax = 0;
		for(Text val: values){
			double temp = Double.parseDouble(val.toString());
			if ( temp > heightmax)
			{
				heightmax = temp; 
			}
		}
		context.write(key, new DoubleWritable(heightmax));
	}
}

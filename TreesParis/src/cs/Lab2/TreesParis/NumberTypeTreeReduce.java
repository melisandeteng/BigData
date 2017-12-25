package cs.Lab2.TreesParis;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class NumberTypeTreeReduce extends Reducer<Text, IntWritable, Text, IntWritable>{
	@Override
	//Groups number of occurrences of trees of the same type
	public void reduce(Text key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException{
		int sum = 0;
		for(IntWritable val: values){
			sum += val.get();
		}
		context.write(key, new IntWritable(sum));
	}
}
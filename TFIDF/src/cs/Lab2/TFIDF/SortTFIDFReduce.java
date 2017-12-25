package cs.Lab2.TFIDF;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class SortTFIDFReduce extends Reducer<DoubleWritable, Text, Text, DoubleWritable>{
	public void reduce(DoubleWritable key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException{
		for (Text word_docid: values){
			double tfidf = -1*key.get();
			context.write(word_docid, new DoubleWritable(tfidf));
		}
	}
}

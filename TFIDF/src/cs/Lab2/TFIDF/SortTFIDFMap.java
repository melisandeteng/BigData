package cs.Lab2.TFIDF;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class SortTFIDFMap extends Mapper<LongWritable, Text, DoubleWritable, Text>{
//the aim is to have the output of Round3Reduce by TFIDF descending order
	
	public void map(LongWritable key, Text value, Context context) 
			throws IOException, InterruptedException{
		for (String line: value.toString().split("\n")){
			String word_docid = line.split("\t")[0];
			double neg_TFIDF = Double.parseDouble(line.split("\t")[1]) * (-1);
			context.write(new DoubleWritable(neg_TFIDF), new Text(word_docid));
		}
	}
}

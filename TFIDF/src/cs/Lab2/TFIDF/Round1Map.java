package cs.Lab2.TFIDF;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
public class Round1Map extends Mapper<LongWritable, Text, Text, IntWritable>{
	//Output is wordUUUUdocid 1 
	//also, lowercase everything to have TF IDF result that is case insensitive
	private final static IntWritable ONE = new IntWritable(1);
	private Text word_docid = new Text();
	
	public void map(LongWritable key, Text value, Context context) 
			throws IOException, InterruptedException{
		String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
		for (String line: value.toString().toLowerCase().split("[:;,.?! \'\"()-]")){
			word_docid.set(line+"UUUU"+fileName);
			context.write(word_docid,  ONE);
		}
	}	
}

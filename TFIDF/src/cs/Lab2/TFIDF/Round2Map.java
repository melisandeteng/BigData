package cs.Lab2.TFIDF;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class Round2Map extends Mapper<LongWritable, Text, Text, Text>{
	
	/* 
	 * Reformat input: wordUUUUdocid	wordcount  (separator is TAB)
	 * to
	 * Output:  docid(docname)	wordUUUUwordcount
	 * 
	 */
	public void map(LongWritable key, Text value, Context context) 
			throws IOException, InterruptedException{
		
		String[] inform =  value.toString().split("\t");
		String[] info = inform[0].toString().split("UUUU");
		Text word = new Text(info[0]);
		Text fileName = new Text(info[1]);
		Text key2 = fileName;
		Text val2 = new Text(word+"UUUU"+inform[1]);
		context.write(key2, val2);
		}
	}	
//inform is wordUUUUdocid tab wordcount
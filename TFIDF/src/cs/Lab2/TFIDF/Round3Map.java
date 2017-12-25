package cs.Lab2.TFIDF;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class Round3Map extends Mapper<LongWritable, Text, Text, Text>{
	//input is wordUUUUdocid wordcountUUUUsum
	//output is word docidUUUUwordcountUUUUwordperdoc
	public void map(LongWritable key, Text value, Context context) 
			throws IOException, InterruptedException{
		
		String[] inform =  value.toString().split("\t");
		String[] word_docid =  inform[0].toString().split("UUUU");
		String[] wordcount_wordsperdoc = inform[1].toString().split("UUUU");
		String word = word_docid[0];
		String docid = word_docid[1];
		String wordcount = wordcount_wordsperdoc[0];
		String wordsperdoc = wordcount_wordsperdoc[1];
		Text docid_wordcount_wordperdoc = new Text(docid+"UUUU"+wordcount+"UUUU"+wordsperdoc);
		context.write(new Text(word), docid_wordcount_wordperdoc);
		}
}

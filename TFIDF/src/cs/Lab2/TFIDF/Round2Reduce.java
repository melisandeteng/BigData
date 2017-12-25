package cs.Lab2.TFIDF;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.Reducer;

public class Round2Reduce extends Reducer<Text, Text, Text, Text>{
	/*Input is 
	*docid(docname)	wordUUUUwordcount
	*Output is 
	*wordUUUUdocid wordcountUUUUsumofwordcount
	*/
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException{
		int sum = 0;
		List<String> cache = new ArrayList<String>();
	
		for(Text val: values){
			cache.add(val.toString());
			String[] word_wordcount = val.toString().split("UUUU");
			String word = word_wordcount[0];
			int wordcount = Integer.parseInt(word_wordcount[1]);
			sum += wordcount;
			
		}
		
		for(String val: cache){
			String[] word_wordcount = val.split("UUUU");
			Text keyS = new Text(word_wordcount[0]+"UUUU"+ key.toString());
			Text valS = new Text(word_wordcount[1]+ "UUUU"+ sum);
			context.write(keyS, valS);
			
		}
	}
}
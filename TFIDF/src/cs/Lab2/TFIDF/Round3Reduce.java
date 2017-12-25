package cs.Lab2.TFIDF;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.Reducer;

public class Round3Reduce extends Reducer<Text, Text, Text, DoubleWritable>{
	/*Input format is:
	**word docnameUUUUwordcountUUUUwordperdoc
	*
	*output is word@text	 tfidf (separator is tab)
	*/ 
	
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException{
		int totaldocs = 2;
				
		int docsperword = 0;	
		Map<String, String> tempDocID = new HashMap<String, String>();
		for(Text docinfo: values){
		
			String docID = docinfo.toString().split("UUUU")[0];
			String counts = docinfo.toString().split("UUUU")[1] +"UUUU"+ docinfo.toString().split("UUUU")[2];
			tempDocID.put(docID, counts);
			docsperword += 1; 
		}
		for(String docid: tempDocID.keySet()){
			
			float wordCount = Float.parseFloat(tempDocID.get(docid).split("UUUU")[0]);
			float wordsPerDoc = Float.parseFloat(tempDocID.get(docid).split("UUUU")[1]);
			double tfidf = (wordCount/wordsPerDoc)*Math.log(totaldocs/docsperword);
			context.write(new Text(key.toString() +"@"+ docid), new DoubleWritable(tfidf));
		}
	}
}
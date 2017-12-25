package cs.Lab2.PageRankVF;


import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import cs.Lab2.PageRankVF.PageRank;

import java.io.IOException;

public class Round2Map extends Mapper<LongWritable, Text, Text, Text> {
    
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        
        /* PageRank calculation algorithm 
         * Input file format (separator is TAB):
         * 
         *     <title>    <page-rank>    <link1>,<link2>,<link3>,<link4>,... ,<linkN>
         * 
         * Output has 2 kind of records:
         * One record composed by the collection of links of each page:
         *     
         *     <title>   SEPARATOR<link1>,<link2>,<link3>,<link4>, ... , <linkN>
         *     
         * Another record contains link, page-rank and number of outlinks from that link
         *  
         *     <link>    <page-rank>    <total-links>
         */
        
        String[] info = value.toString().split("\t");
 
        
        // extract informations
        String links = "";
        String page =info[0];
        String pageRank =info[1];
        if (info.length == 3)
        {	links = info[2];
        
        
        
        String[] allPagesLinked = links.split(",");
        for (String otherPage : allPagesLinked) { 
            Text pageRankWithTotalLinks = new Text(pageRank + "\t" + Integer.toString(allPagesLinked.length));
            context.write(new Text(otherPage), pageRankWithTotalLinks); 
        }
        }
        
        context.write(new Text(page), new Text("SEPARATOR"+ links));
        
    }
    
}
package cs.Lab2.PageRankVF;
import cs.Lab2.PageRankVF.PageRank;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Round2Reduce extends Reducer<Text, Text, Text, Text> {
    
    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, 
                                                                                InterruptedException {
        
        /* PageRank calculation algorithm (reducer)
         * Input file format has 2 kind of records (separator is "SEPARATOR"): 
         * One record composed by the collection of links of each page:
         * 
         *     <title>   SEPARATOR<link1>,<link2>,<link3>,<link4>, ... , <linkN>
         *     
         * Another record composed with the linked page, the page rank of the source page 
         * and the total amount of out links of the source page:
         *
         *     <link>    <page-rank>    <total-links>
         */
        
        String links = "";
        double sumShareOtherPR = 0.0;
        
        for (Text value : values) {
 
            String content = value.toString();
            
            if (content.startsWith("SEPARATOR")) {
                // if this value contains node links append them to the 'links' string

            	String link = content.substring("SEPARATOR".length());
                links += link;
            } else {
                
                String[] split = content.split("\t");
                
                double pageRank = Double.parseDouble(split[0]);
                int totalLinks = Integer.parseInt(split[1]);
                
                // add the contribution of all the pages having an outlink pointing tp the page
                // the final pagerank value before submitting the result to the next job.
                sumShareOtherPR += (pageRank / totalLinks);
            }

        }
        
        double newRank = (PageRank.DAMPING)* sumShareOtherPR + (1-PageRank.DAMPING)/(PageRank.NODES.size());
        context.write(key, new Text(newRank + "\t" + links));
        
    }

}
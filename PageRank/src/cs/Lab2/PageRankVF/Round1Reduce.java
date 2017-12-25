
package cs.Lab2.PageRankVF;


import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import cs.Lab2.PageRankVF.PageRank;

public class Round1Reduce extends Reducer<Text, Text, Text, Text> {
    
    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        
        /* 
         * Output format is(separator is TAB):
         *     <title>    <page-rank>    <link1>,<link2>,<link3>,<link4>,...,<linkN>
         */
        
        boolean first = true;
        //initialize Page Rank to 1/total number of pages 
        
        String links = (1.0/ PageRank.NODES.size()) + "\t";

        for (Text value : values) {
            if (!first) 
                links += ",";
            links += value.toString();
            first = false;
        }

        context.write(key, new Text(links));
    }
}

package cs.Lab2.PageRankVF;


import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import cs.Lab2.PageRankVF.PageRank;


import java.io.IOException;

public class Round1Map extends Mapper<LongWritable, Text, Text, Text> {
    
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        
        /* Input format is (separator is TAB):
         * 
         *     <nodeA>    <nodeB> 
         * which denotes an edge going from <nodeA> to <nodeB>.
         * We will also collect all the distinct nodes in our graph: this is needed to compute the initialize the 
         * pagerank value in Round1Reduce
         */
        Text in_node = new Text();
        Text out_node = new Text();
        //need to skip comment lines starting with #
        if (value.charAt(0) != '#') {
            String[] edge = value.toString().split("\t");
        	String in = edge[0];
        	String out = edge[1];
        	in_node.set(in);
        	out_node.set(out);
        	context.write(in_node, out_node);
            // add the current source node to the node list so we can 
            // compute the total number of nodes
            PageRank.NODES.add(in);
            // also add the target node to the same list: we may have a target node 
            // with no outlinks
            PageRank.NODES.add(out);
            
        }
 
    }
    
}
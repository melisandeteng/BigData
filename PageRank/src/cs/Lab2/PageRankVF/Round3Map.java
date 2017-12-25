package cs.Lab2.PageRankVF;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class Round3Map extends Mapper<LongWritable, Text, DoubleWritable,  Text> {
    
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        
        /* Order the results
         * Input file format (separator is TAB):
         * 
         *     <title>    <page-rank>    <link1>,<link2>,<link3>,<link4>,... ,<linkN>
         * 
         * A reducer is ot needed
         * Output is -PageRank	link 
         * (PR is displayed with "-" sign)
         */
        
        String info[] = value.toString().split("\t");
        
        // extract tokens from the current line
        String page = info[0];
        //multiply by -1 to have the results in ascending order of PR
        float pageRank = (-1)*Float.parseFloat(info[1]);
        
        context.write(new DoubleWritable(pageRank), new Text(page));
        
    }
       
}


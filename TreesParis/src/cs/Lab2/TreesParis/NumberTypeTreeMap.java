package cs.Lab2.TreesParis;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class NumberTypeTreeMap extends Mapper <LongWritable, Text, Text, IntWritable> {
	
	//output is pairs :  TypeTree  1
	//Type is "ESPECE"
	private final static IntWritable ONE = new IntWritable(1);
	private Text typeTree = new Text();
	
	@Override
	public void map(LongWritable key, Text value, Context context) 
			throws IOException, InterruptedException{
		String[] tree =  value.toString().split(";");
		String type = tree[3];
		if (!type.equals("")&& !type.equals("ESPECE") && !type.equals(" ")){
			typeTree.set(type);
			context.write(typeTree,  ONE);
			}
		}
	}

	
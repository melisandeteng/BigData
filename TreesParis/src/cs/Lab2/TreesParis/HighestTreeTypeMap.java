package cs.Lab2.TreesParis;


import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class HighestTreeTypeMap extends Mapper <LongWritable, Text, Text, Text> {
	
	//output is pairs :  TypeTree  HeightTree 
	// Type here is "ESPECE"
	private final static Text height = new Text();
	private Text typeTree = new Text();
	
	@Override
	public void map(LongWritable key, Text value, Context context) 
			throws IOException, InterruptedException{
			String[] info = value.toString().split(";");
			//Get tree "ESPECE"
			String heightTree = info[6];
			//Get tree "HAUTEUR"
			String type = info[3];
			if (!type.equals("")&& !type.equals("ESPECE") && !type.equals(" ") && !heightTree.equals("")){
				typeTree.set(type);
				height.set(heightTree);
				context.write(typeTree, height);
			}
		}
	}

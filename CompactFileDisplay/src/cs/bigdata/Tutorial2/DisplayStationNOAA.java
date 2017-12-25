package cs.bigdata.Tutorial2;

import java.io.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;



public class DisplayStationNOAA {

	public static String usaf;
	public static String name;
	public static String countryID;
	public static String  elevation;
	
	public static void readLine(String line){
		DisplayStationNOAA.usaf = line.substring(1, 1+6);
		DisplayStationNOAA.name = line.substring(13, 13+29);
		DisplayStationNOAA.countryID = line.substring(43, 43+2);
		DisplayStationNOAA.elevation = line.substring(74, 74+7);
	}
	
	public static void main(String[] args) throws IOException {
		
		int linecount = 0;
		String localSrc = "/home/cloudera/Downloads/isd-history.txt";
		//Open the file with Hadoop API
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		Path path = new Path(localSrc);
		FSDataInputStream in = fs.open(path);
		
		try{
			InputStreamReader isr = new InputStreamReader(in);
			//@SuppressWarnings("resource")
			BufferedReader br = new BufferedReader(isr);
			// read line by line
			String line = br.readLine();
			
			while (line !=null){
				if (linecount > 21)
				{
					DisplayStationNOAA.readLine(line);
					System.out.println("Station" + (linecount-21)
							+ " USAF: " + DisplayStationNOAA.usaf 
							+ " , name: " + DisplayStationNOAA.name
							+ " , countryID: " + DisplayStationNOAA.countryID
							+ " , elevation: " + DisplayStationNOAA.elevation);
				}
				linecount++;
				line = br.readLine();
			}
		}

		finally{
			//close the file
			in.close();
			fs.close();
		}
	}
}
 
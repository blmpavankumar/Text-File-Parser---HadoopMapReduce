/*
Author: Pavankumar Barur Lingaraj
Advanced Database Systems - 91.673 Final Project
Computer Science, University of Massachusetts, Lowell

This class sets up a reducer job for query 2
--All the <key, value> pairs are received from all mapper jobs.
--Columns are filtered based on required condition.(Project Operation)
--Result columns are written to output file.

 */
package advanced.database.systems;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class ProjectReducer2 extends MapReduceBase implements
Reducer<Text, IntWritable, Text, IntWritable> {

	public void reduce(Text key, @SuppressWarnings("rawtypes") Iterator values, @SuppressWarnings("rawtypes") OutputCollector output,
			Reporter reporter) throws IOException {
		System.out.println("Reducer Job For Query 2 Starting...\n");
		String pop = new String();
		File ofile = new File("/home/training/Desktop/project/output/query2");
		@SuppressWarnings("unused")
		boolean b = false;
		if (!ofile.exists()) {
			b = ofile.mkdirs();
		}
		FileWriter fw = new FileWriter("/home/training/Desktop/project/output/query2/output.txt",true);
		while (values.hasNext()) {
			pop = values.next().toString();
			fw.append("City: "+key.toString()+"; District: "+pop+"\n");
		}
		fw.close();
		System.out.println("Reducer Job For Query 2 Finished\n");
	}

}


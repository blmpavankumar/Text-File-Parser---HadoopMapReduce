/*
Author: Pavankumar Barur Lingaraj
Advanced Database Systems - 91.673 Final Project
Computer Science, University of Massachusetts, Lowell

This class sets up a mapper job for query 3
--Reads each line of countrylanguage.txt are split based on delimiter ‘,’ and stored in an array as separate columns.
--First two columns which represent country code and respective language are mapped and sent as <key, value> pair to reducer.

 */
package advanced.database.systems;
import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;


public class ProjectMapper3 extends MapReduceBase implements
Mapper<LongWritable, Text, Text, IntWritable> {

	private Text word = new Text();
	private Text t1 = new Text();
	private Text t2 = new Text();
	
@SuppressWarnings("unchecked")
@Override
public void map(LongWritable key, Text value, @SuppressWarnings("rawtypes") OutputCollector output,
	Reporter reporter) throws IOException {
	System.out.println("Mapper Job For Query 3 Starting...\n");
	String[] fields=null;
	String line = value.toString();
	StringTokenizer tokenizer = new StringTokenizer(line);	
	while (tokenizer.hasMoreTokens()) {
		word.set(tokenizer.nextToken());
	    fields=line.split("\\,");
	    t1 = new Text(fields[0]);
	    t2 = new Text(fields[1]);
	    output.collect(t1, t2);
	}
	System.out.println("Mapper Job For Query 3 Finished\n");
 }
}

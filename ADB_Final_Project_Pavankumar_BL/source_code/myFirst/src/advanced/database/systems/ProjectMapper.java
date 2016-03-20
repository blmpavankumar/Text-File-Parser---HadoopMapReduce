/*
Author: Pavankumar Barur Lingaraj
Advanced Database Systems - 91.673 Final Project
Computer Science, University of Massachusetts, Lowell

This class sets up a mapper job for query 1
--Read each line of city.txt are split based on delimiter ‘,’ and stored in an array as separate columns.
--Column 2 and 5 are mapped and sent as <key, value> pair to reducer.

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
public class ProjectMapper extends MapReduceBase implements
Mapper<LongWritable, Text, Text, IntWritable> {
	private Text word = new Text();
	private Text t1 = new Text();
	private Text t2 = new Text();

	@SuppressWarnings("unchecked")
	@Override
	public void map(LongWritable key, Text value, @SuppressWarnings("rawtypes") OutputCollector output,
		Reporter reporter) throws IOException {
		System.out.println("Mapper Job For Query 1 Starting...\n");
		String[] fields=null;
		String line = value.toString();
		StringTokenizer tokenizer = new StringTokenizer(line);
		while (tokenizer.hasMoreTokens()) {
			word.set(tokenizer.nextToken());
			fields=line.split("\\,");
			t1 = new Text(fields[1]);
			t2 = new Text(fields[4]);
			output.collect(t1, t2);
		}
		System.out.println("Mapper Job For Query 1 Finished\n");
	}
}

/*
Author: Pavankumar Barur Lingaraj
Advanced Database Systems - 91.673 Final Project
Computer Science, University of Massachusetts, Lowell

This class sets up an environment for map reduce jobs of 4th query by,
--setting driver job
--setting input paths
--setting output paths
--setting mapper class
--setting reducer class
 */
package advanced.database.systems;
import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
        
public class ProjectDriver4 {
 //mapper class       
 public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
    private Text t1 = new Text();
    String[] fields=null;    
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    	System.out.println("Mapper Job For Query 4 Starting...\n");
    	String line = value.toString();
        StringTokenizer tokenizer = new StringTokenizer(line);
        while (tokenizer.hasMoreTokens()) {
            word.set(tokenizer.nextToken());
            fields=line.split("\\,");
            t1 = new Text(fields[3]);
            context.write(t1, one);
        }
        System.out.println("Mapper Job For Query 4 Finished\n");
    }
 } 
 //reducer class       
 public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
	 
	 private IntWritable result = new IntWritable();
	 String o1=null;
	 private Text t1 = new Text();
	 public void reduce(Text key, Iterable<IntWritable> values,
             Context context
             ) throws IOException, InterruptedException {
		System.out.println("Reducer Job For Query 4 Starting...\n");
    	int sum = 0;
        for (IntWritable val : values) {
          sum += val.get();
        }
        result.set(sum);
        o1="District : "+key+"; Number of Cities : ";
        t1 = new Text(o1);
        context.write(t1, result);
        System.out.println("Reducer Job For Query 4 Finished\n");
    }
 }
        
 public static void main(String a,String b) throws Exception {
    Configuration conf = new Configuration();
        
    Job job = new Job(conf, "Query4");
    //delete output path if already existing
    Path outputDir = new Path( b + "query4");
	outputDir.getFileSystem( conf ).delete( outputDir, true );
	// specify output types
	System.out.println("Specifying output types\n");
	job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    // specify a mapper class
    System.out.println("Configuring Mapper 4 class\n");   
    job.setMapperClass(Map.class);
    // specify a reducer class
	System.out.println("configuring Reducer 4 class\n");
    job.setReducerClass(Reduce.class); 
    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);
    // specify input and output directories
	System.out.println("Specifying input and output directories\n");
    FileInputFormat.addInputPath(job, new Path(a));
    FileOutputFormat.setOutputPath(job, new Path( b + "query4"));
        
    job.waitForCompletion(true);
 }
        
}
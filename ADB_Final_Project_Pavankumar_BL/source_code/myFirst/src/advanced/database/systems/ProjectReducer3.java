/*
Author: Pavankumar Barur Lingaraj
Advanced Database Systems - 91.673 Final Project
Computer Science, University of Massachusetts, Lowell

This class sets up a reducer job for query 3
--All the <key, value> pairs are received from all mapper jobs.
--Reads each line of country.txt are split based on delimiter ‘,’ and first two columns which represent country code and respective country name are stored in an array.
--Join operation performed on received <key, value> pairs and stored columns to extract required results. (Natural Join Operation)
--Results are written to output file.

 */
package advanced.database.systems;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class ProjectReducer3 extends MapReduceBase implements
Reducer<Text, IntWritable, Text, IntWritable> {

	public void reduce(Text key, @SuppressWarnings("rawtypes") Iterator values, @SuppressWarnings("rawtypes") OutputCollector output,
		Reporter reporter) throws IOException {
		System.out.println("Reducer Job For Query 3 Starting...\n");
		String lang = new String();
		String code=new String();
		File ofile = new File("/home/training/Desktop/project/output/query3");
		@SuppressWarnings("unused")
		boolean b = false;
		if (!ofile.exists()) {
			b = ofile.mkdirs();
		}
		BufferedReader reader = new BufferedReader(new FileReader("/home/training/Desktop/project/input/country.txt"));
		String line2;
		String[] fields3=null;
		String[][] fields2=new String[1000][1000];
		int i=0,k,length;
		while ((line2 = reader.readLine()) != null)
		{
			fields3=line2.split("\\,");
			fields2[i][0]=fields3[0];
			fields2[i][1]=fields3[1];
			i++;
		}
		length=i;
		reader.close();
		FileWriter fw = new FileWriter("/home/training/Desktop/project/output/query3/output.txt",true);
		while (values.hasNext()) 
		{
			lang = values.next().toString();
			code=key.toString();
			for(k=0;k<length;k++)
			{
				if(fields2[k][0].equals(code) && lang.equals("English"))
				{
					fw.append("Country: "+fields2[k][1]+"; Language: "+lang+"\n");
				}
			}

		}
		fw.close();
		System.out.println("Reducer Job For Query 3 Finished\n");
	}

}


/*
Author: Pavankumar Barur Lingaraj
Advanced Database Systems - 91.673 Final Project
Computer Science, University of Massachusetts, Lowell

This class sets up an environment for all map reduce jobs by,
--providing user with choice to select a query
--setting driver job for each query
--setting input paths for each query
--setting output paths for each query
--setting mapper class for each query
--setting reducer class for each query
 */
package advanced.database.systems;
import java.io.File;
import java.io.FileWriter;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import java.util.Scanner;

public class ProjectDriver {

	public static void main(String[] args) throws Exception {
		System.out.println("Welcome to Hadoop MapReduce Project Demo !\n");
		Scanner input= new Scanner(System.in);
		String in=null;
		do
		{
			System.out.println("Enter your choice : Press 1, 2, 3, 4, 5 or 6 based on your choice.\n");
			System.out.println("1. Find cities whose population is larger than 300,000\n");
			System.out.println("2. Find all the name of the cities and corresponding district\n");
			System.out.println("3. Find all countries whose official language is English.\n");
			System.out.println("4. Find how many cities each district has\n");
			System.out.println("5. To Run All The Above 4 Queries At One Shot!\n");
			System.out.println("6. Exit\n");
			System.out.println("Enter your Choice Below : ");
			in=input.next();
			if(in.equals("1")){
				System.out.println("Starting Driver 1...\n");
			    JobClient client = new JobClient();
				JobConf conf = new JobConf(ProjectDriver.class);	
				//delete output path if already existing
				Path outputDir = new Path( args[1] + "query1");
				outputDir.getFileSystem( conf ).delete( outputDir, true );
				// specify output types
				System.out.println("Specifying output types\n");
				conf.setOutputKeyClass(Text.class);
				conf.setOutputValueClass(Text.class);
				// specify input and output directories
				System.out.println("Specifying input and output directories\n");
				FileInputFormat.addInputPath(conf, new Path(args[0]));
			    FileOutputFormat.setOutputPath(conf, new Path(args[1]+ "query1"));
				// specify a mapper class
			    System.out.println("Configuring Mapper 1 class\n");
				conf.setMapperClass(ProjectMapper.class);
				// specify a reducer class
				System.out.println("configuring Reducer 1 class\n");
				conf.setReducerClass(ProjectReducer.class);
				client.setConf(conf);		
				JobClient.runJob(conf);
				//delete unwanted files
				File file = new File("/home/training/Desktop/project/output/query1/part-00000");
				File file2 = new File("/home/training/Desktop/project/output/query1/_SUCCESS");
				if(file.exists())
					file.delete();
				if(file2.exists())
					file2.delete();
				//create and write readme file
				FileWriter fwd = new FileWriter("/home/training/Desktop/project/output/query1/readme.txt",true);
				fwd.append("-------------------------------------------------------------------------------------------------------------------------------------\n");
				fwd.append("Author : Pavankumar Barur Lingaraj\n");
				fwd.append("Subject : Adavanced Database Systems, Final Project\n");
				fwd.append("Job : Computing Selection by MapReduce\n");
				fwd.append("Question : Find cities whose population is larger than 300,000\n");
				fwd.append("Input : city.txt file\n");
				fwd.append("Output : output.txt file is generated showing the results of question in the following format\n");
				fwd.append("City: (city name) ; Population: (population)\n");
				fwd.append("-------------------------------------------------------------------------------------------------------------------------------------\n");
				fwd.close();
				System.out.println("\nJob Complete! Open Output Directory to View Results.\n");
				System.out.println("If you want to continue, Make your choice again, else press 6 to Exit\n");
			}
	
			
			else if(in.equals("2")){
				    System.out.println("Starting Driver 2...\n");
					JobClient client = new JobClient();
					JobConf conf = new JobConf(ProjectDriver.class);			   
					Path outputDir = new Path( args[1] + "query2");
					outputDir.getFileSystem( conf ).delete( outputDir, true );
					System.out.println("Specifying output types\n");
					conf.setOutputKeyClass(Text.class);
					conf.setOutputValueClass(Text.class);
					System.out.println("Specifying input and output directories\n");
					FileInputFormat.addInputPath(conf, new Path(args[0]));
				    FileOutputFormat.setOutputPath(conf, new Path(args[1] + "query2"));
				    System.out.println("Configuring Mapper 2 class\n");
					conf.setMapperClass(ProjectMapper2.class);
					System.out.println("Configuring Reducer 2 class\n");
					conf.setReducerClass(ProjectReducer2.class);
					client.setConf(conf);		
					JobClient.runJob(conf);
					File file = new File("/home/training/Desktop/project/output/query2/part-00000");
					File file2 = new File("/home/training/Desktop/project/output/query2/_SUCCESS");
					if(file.exists())
						file.delete();
					if(file2.exists())
						file2.delete();
					FileWriter fwd = new FileWriter("/home/training/Desktop/project/output/query2/readme.txt",true);
					fwd.append("-------------------------------------------------------------------------------------------------------------------------------------\n");
					fwd.append("Author : Pavankumar Barur Lingaraj\n");
					fwd.append("Subject : Adavanced Database Systems, Final Project\n");
					fwd.append("Job : Computing Projection by MapReduce\n");
					fwd.append("Question : Find all the name of the cities and corresponding district\n");
					fwd.append("Input : city.txt file\n");
					fwd.append("Output : output.txt file is generated showing the results of question in the following format\n");
					fwd.append("City: (city name) ; District: (district name)\n");
					fwd.append("-------------------------------------------------------------------------------------------------------------------------------------\n");
					fwd.close();
					System.out.println("\nJob Complete! Open Output Directory to View Results.\n");
					System.out.println("If you want to continue, Make your choice again, else press 6 to Exit\n");
				}
	
			else if(in.equals("3")){
					System.out.println("Starting Driver 3...\n");
				    JobClient client = new JobClient();
					JobConf conf = new JobConf(ProjectDriver.class);			   
					Path outputDir = new Path( args[1] + "query3");
					outputDir.getFileSystem( conf ).delete( outputDir, true );
					System.out.println("Specifying output types\n");
					conf.setOutputKeyClass(Text.class);
					conf.setOutputValueClass(Text.class);
					System.out.println("Specifying input and output directories\n");
					FileInputFormat.setInputPaths(conf, "/home/training/Desktop/project/input/countrylanguage.txt");
				    FileOutputFormat.setOutputPath(conf, new Path(args[1] + "query3"));
				    System.out.println("Configuring Mapper 3 class\n");
					conf.setMapperClass(ProjectMapper3.class);
					System.out.println("Configuring Reducer 3 class\n");
					conf.setReducerClass(ProjectReducer3.class);
					client.setConf(conf);		
					JobClient.runJob(conf);
					File file = new File("/home/training/Desktop/project/output/query3/part-00000");
					File file2 = new File("/home/training/Desktop/project/output/query3/_SUCCESS");
					if(file.exists())
						file.delete();
					if(file2.exists())
						file2.delete();
					FileWriter fwd = new FileWriter("/home/training/Desktop/project/output/query3/readme.txt",true);
					fwd.append("-------------------------------------------------------------------------------------------------------------------------------------\n");
					fwd.append("Author : Pavankumar Barur Lingaraj\n");
					fwd.append("Subject : Adavanced Database Systems, Final Project\n");
					fwd.append("Job : Computing Natural Join by MapReduce\n");
					fwd.append("Question : Find all countries whose official language is English\n");
					fwd.append("Input : 2 files : country.txt and countrylanguage.txt\n");
					fwd.append("Output : output.txt file is generated showing the results of question in the following format\n");
					fwd.append("Country: (country name) ; Language: (language)\n");
					fwd.append("-------------------------------------------------------------------------------------------------------------------------------------\n");
					fwd.close();
					System.out.println("\nJob Complete! Open Output Directory to View Results.\n");
					System.out.println("If you want to continue, Make your choice again, else press 6 to Exit\n");
				}
	
			
			else if(in.equals("4")){
				System.out.println("Starting Driver 4...\n");
				//instantiate driver and pass arguments
				ProjectDriver4.main(args[0],args[1]);
				File f = new File("/home/training/Desktop/project/output/query4/part-r-00000"); 
	            f.renameTo(new File("/home/training/Desktop/project/output/query4/output.txt"));
				File file2 = new File("/home/training/Desktop/project/output/query4/_SUCCESS");
				if(file2.exists())
					file2.delete();
				FileWriter fwd = new FileWriter("/home/training/Desktop/project/output/query4/readme.txt",true);
				fwd.append("-------------------------------------------------------------------------------------------------------------------------------------\n");
				fwd.append("Author : Pavankumar Barur Lingaraj\n");
				fwd.append("Subject : Adavanced Database Systems, Final Project\n");
				fwd.append("Job : Aggregation by MapReduce\n");
				fwd.append("Question : Find how many cities each district has \n");
				fwd.append("Input : city.txt file\n");
				fwd.append("Output : output.txt file is generated showing the results of question in the following format\n");
				fwd.append("District: (district name) ; Number of cities: (count of number of cities that particular district has)\n");
				fwd.append("-------------------------------------------------------------------------------------------------------------------------------------\n");
				fwd.close();    
				System.out.println("\nJob Complete! Open Output Directory to View Results.\n");
				System.out.println("If you want to continue, Make your choice again, else press 6 to Exit\n");
			}
			
			else if(in.equals("5")){
				System.out.println("Starting Driver 1...\n");
			    JobClient client = new JobClient();
				JobConf conf = new JobConf(ProjectDriver.class);			   
				Path outputDir = new Path( args[1] + "query1");
				outputDir.getFileSystem( conf ).delete( outputDir, true );
				System.out.println("Specifying output types\n");
				conf.setOutputKeyClass(Text.class);
				conf.setOutputValueClass(Text.class);
				System.out.println("Specifying input and output directories\n");
				FileInputFormat.addInputPath(conf, new Path(args[0]));
			    FileOutputFormat.setOutputPath(conf, new Path(args[1]+ "query1"));
			    System.out.println("Configuring Mapper 1 class\n");
				conf.setMapperClass(ProjectMapper.class);
				System.out.println("Configuring Reducer 1 class\n");
				conf.setReducerClass(ProjectReducer.class);
				client.setConf(conf);						
				JobClient.runJob(conf);
				File file = new File("/home/training/Desktop/project/output/query1/part-00000");
				File file2 = new File("/home/training/Desktop/project/output/query1/_SUCCESS");
				if(file.exists())
					file.delete();
				if(file2.exists())
					file2.delete();
				FileWriter fwd = new FileWriter("/home/training/Desktop/project/output/query1/readme.txt",true);
				fwd.append("-------------------------------------------------------------------------------------------------------------------------------------\n");
				fwd.append("Author : Pavankumar Barur Lingaraj\n");
				fwd.append("Subject : Adavanced Database Systems, Final Project\n");
				fwd.append("Job : Computing Selection by MapReduce\n");
				fwd.append("Question : Find cities whose population is larger than 300,000\n");
				fwd.append("Input : city.txt file\n");
				fwd.append("Output : output.txt file is generated showing the results of question in the following format\n");
				fwd.append("City: (city name) ; Population: (population)\n");
				fwd.append("-------------------------------------------------------------------------------------------------------------------------------------\n");
				fwd.close();
				
				System.out.println("Starting Driver 2...\n");
				JobClient client2 = new JobClient();
				JobConf conf2 = new JobConf(ProjectDriver.class);			   
				Path outputDir2 = new Path( args[1] + "query2");
				outputDir2.getFileSystem( conf2 ).delete( outputDir2, true );
				System.out.println("Specifying output types\n");
				conf2.setOutputKeyClass(Text.class);
				conf2.setOutputValueClass(Text.class);
				System.out.println("Specifying input and output directories\n");
				FileInputFormat.addInputPath(conf2, new Path(args[0]));
			    FileOutputFormat.setOutputPath(conf2, new Path(args[1] + "query2"));
			    System.out.println("Configuring Mapper 2 class\n");
			    conf2.setMapperClass(ProjectMapper2.class);
			    System.out.println("Configuring Reducer 2 class\n");
			    conf2.setReducerClass(ProjectReducer2.class);
				client2.setConf(conf2);		
				JobClient.runJob(conf2);
				File file21 = new File("/home/training/Desktop/project/output/query2/part-00000");
				File file22 = new File("/home/training/Desktop/project/output/query2/_SUCCESS");
				if(file21.exists())
					file21.delete();
				if(file22.exists())
					file22.delete();
				FileWriter fwd21 = new FileWriter("/home/training/Desktop/project/output/query2/readme.txt",true);
				fwd21.append("-------------------------------------------------------------------------------------------------------------------------------------\n");
				fwd21.append("Author : Pavankumar Barur Lingaraj\n");
				fwd21.append("Subject : Adavanced Database Systems, Final Project\n");
				fwd21.append("Job : Computing Projection by MapReduce\n");
				fwd21.append("Question : Find all the name of the cities and corresponding district\n");
				fwd21.append("Input : city.txt file\n");
				fwd21.append("Output : output.txt file is generated showing the results of question in the following format\n");
				fwd21.append("City: (city name) ; District: (district name)\n");
				fwd21.append("-------------------------------------------------------------------------------------------------------------------------------------\n");
				fwd21.close();
				
				System.out.println("Starting Driver 3...\n");
			    JobClient client3 = new JobClient();
				JobConf conf3 = new JobConf(ProjectDriver.class);			   
				Path outputDir3 = new Path( args[1] + "query3");
				outputDir3.getFileSystem( conf3 ).delete( outputDir3, true );
				System.out.println("Specifying output types\n");
				conf3.setOutputKeyClass(Text.class);
				conf3.setOutputValueClass(Text.class);
				System.out.println("Specifying input and output directories\n");
				FileInputFormat.setInputPaths(conf3, "/home/training/Desktop/project/input/countrylanguage.txt");
			    FileOutputFormat.setOutputPath(conf3, new Path(args[1] + "query3"));
			    System.out.println("Configuring Mapper 3 class\n");
				conf3.setMapperClass(ProjectMapper3.class);
				System.out.println("Configuring Reducer 3 class\n");
				conf3.setReducerClass(ProjectReducer3.class);
				client3.setConf(conf3);		
				JobClient.runJob(conf3);
				File file31 = new File("/home/training/Desktop/project/output/query3/part-00000");
				File file23 = new File("/home/training/Desktop/project/output/query3/_SUCCESS");
				if(file31.exists())
					file31.delete();
				if(file23.exists())
					file23.delete();
				FileWriter fwd3 = new FileWriter("/home/training/Desktop/project/output/query3/readme.txt",true);
				fwd3.append("-------------------------------------------------------------------------------------------------------------------------------------\n");
				fwd3.append("Author : Pavankumar Barur Lingaraj\n");
				fwd3.append("Subject : Adavanced Database Systems, Final Project\n");
				fwd3.append("Job : Computing Natural Join by MapReduce\n");
				fwd3.append("Question : Find all countries whose official language is English\n");
				fwd3.append("Input : 2 files : country.txt and countrylanguage.txt\n");
				fwd3.append("Output : output.txt file is generated showing the results of question in the following format\n");
				fwd3.append("Country: (country name) ; Language: (language)\n");
				fwd3.append("-------------------------------------------------------------------------------------------------------------------------------------\n");
				fwd3.close();
				
				System.out.println("Starting Driver 4...\n");
				ProjectDriver4.main(args[0],args[1]);
				File f = new File("/home/training/Desktop/project/output/query4/part-r-00000"); 
	            f.renameTo(new File("/home/training/Desktop/project/output/query4/output.txt"));
				File file24 = new File("/home/training/Desktop/project/output/query4/_SUCCESS");
				if(file24.exists())
					file24.delete();
				FileWriter fwd4 = new FileWriter("/home/training/Desktop/project/output/query4/readme.txt",true);
				fwd4.append("-------------------------------------------------------------------------------------------------------------------------------------\n");
				fwd4.append("Author : Pavankumar Barur Lingaraj\n");
				fwd4.append("Subject : Adavanced Database Systems, Final Project\n");
				fwd4.append("Job : Aggregation by MapReduce\n");
				fwd4.append("Question : Find how many cities each district has \n");
				fwd4.append("Input : city.txt file\n");
				fwd4.append("Output : output.txt file is generated showing the results of question in the following format\n");
				fwd4.append("District: (district name) ; Number of cities: (count of number of cities that particular district has)\n");
				fwd4.append("-------------------------------------------------------------------------------------------------------------------------------------\n");
				fwd4.close();
				System.out.println("\nJob Complete! Open Output Directory to View Results.\n");
				System.out.println("If you want to continue, Make your choice again, else press 6 to Exit\n");
			}
			else 
			{
				if(!in.equals("6"))
				System.out.println("Oops! That's Incorrect Choice! Please Try Again\n");
			}
		}while(!in.equals("6"));
		System.out.println("Process Complete! Thank You...\n");
			
	 }
}

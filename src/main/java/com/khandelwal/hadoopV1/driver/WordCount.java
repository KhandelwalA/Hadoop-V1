package com.khandelwal.hadoopV1.driver;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.khandelwal.hadoopV1.mapper.WordMapper;
import com.khandelwal.hadoopV1.reducer.WordReducer;

/**
 * This is driver class of WordCount project having main method.
 * 
 * @author Abhishek
 *
 */
public class WordCount extends Configured implements Tool {
	/**
	 * This method is defined in Tool class of hadoop
	 * 
	 * @param String
	 *            [] arg0 takes runtime arguments of which first argument value
	 *            is input file or directory in HDFS and second argument value
	 *            is output directory
	 */
	public int run(String[] args) throws Exception {
		if (args.length < 2) {
			System.out.println("Either or both input file/directory and output directory is not passed as command line argument");
			return -1;
		}

		/*
		 * Initializing JobConf object with constructor argument which should be
		 * the driver class and match with driver class name to be provided in
		 * command while processing HDFS file
		 * 
		 * command is: hadoop jar wordCount.jar WordCount {inputFile}
		 * {outputDirectory} where in wordCount is the name of jar having driver
		 * class, Mapper class and Reducer class WordCount is Driver class name
		 * having main method {inputFile} is the name of HDFS file or Directory
		 * of files to be processed {outputDirectory} is the directory where out
		 * gets generated
		 */
		JobConf job = new JobConf(WordCount.class);

		/*
		 * Sets the job name
		 */
		job.setJobName("WordCount_HadoopV1");

		/*
		 * Sets the input file/Directory in Path object
		 */
		Path inputFilePath = new Path(args[0]);

		/*
		 * Sets outputDirectory in Path object
		 */
		Path outputDirectoryPath = new Path(args[1]);

		/*
		 * Sets input file path in JobConf object
		 */
		FileInputFormat.setInputPaths(job, inputFilePath);

		/*
		 * Sets output file path in JobConf object
		 */
		FileOutputFormat.setOutputPath(job, outputDirectoryPath);

		/*
		 * Sets Mapper class
		 */
		job.setMapperClass(WordMapper.class);

		/*
		 * Sets key type of the mapper output, that means after processing input
		 * split mapper will generate key value pairs of which key is of this
		 * type
		 */
		job.setMapOutputKeyClass(Text.class);

		/*
		 * Sets value type of the mapper output, that means after processing
		 * input split mapper will generate key value pairs of which value is of
		 * this type
		 */
		job.setMapOutputValueClass(IntWritable.class);

		/*
		 * Sets Reducer class
		 */
		job.setReducerClass(WordReducer.class);

		/*
		 * Sets key type of the reducer output, that means after
		 * combining/reducing output of all the mappers, reducer will generate
		 * key value pairs of which key is of this type
		 */
		job.setOutputKeyClass(Text.class);

		/*
		 * Sets value type of the reducer output, that means after
		 * combining/reducing output of all the mappers, reducer will generate
		 * key value pairs of which value is of this type
		 */
		job.setOutputValueClass(IntWritable.class);

		/*
		 * This will execute the job
		 */
		JobClient.runJob(job);

		return 0;
	}

	public static void main(String args[]) throws Exception {

		/*
		 * ToolRunner's run method takes object of implementation class of Tool
		 * interface and command line interface which are input file/dir and
		 * output dir and calls the run method from Tool interface
		 * implementation class
		 */
		int exitCode = ToolRunner.run(new WordCount(), args);

		System.exit(exitCode);
	}
}

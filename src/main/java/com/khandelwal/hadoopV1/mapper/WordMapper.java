package com.khandelwal.hadoopV1.mapper;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

/**
 * This is Mapper class of WordCount project. This class extends MapReduceBase
 * class which implements close and configure methods of Closeable and
 * JobConfigure interfaces respectively and works as a bridge between Mapper and
 * Reducer to pass intermediate data from Mapper to Reducer. This also
 * implements Mapper interface
 * 
 * @author Abhishek
 *
 */
public class WordMapper extends MapReduceBase implements
		Mapper<LongWritable, Text, Text, IntWritable> {

	/**
	 * This method has logic to split each word of a every record read by
	 * RecordReader and set in OutputCollector as key and 1 as default value
	 * 
	 * @param LongWritable
	 *            mapInputKey
	 * @param Text
	 *            mapInputValue
	 * @param OutputCollector
	 *            <Text, IntWritable> outputCollector. This is
	 *            collection<key,value> of mapper output
	 * @param Reporter
	 *            reporter. This interface record any exception/issue and report
	 *            to driver class
	 */
	public void map(LongWritable mapInputKey, Text mapInputValue,
			OutputCollector<Text, IntWritable> outputCollector,
			Reporter reporter) throws IOException {

		String inputString = mapInputValue.toString();

		for (String word : inputString.split(" ")) {
			outputCollector.collect(new Text(word), new IntWritable(1));
		}
	}

}

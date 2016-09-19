package com.khandelwal.hadoopV1.reducer;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

/**
 * This is Reducer class of WordCount project. This class extends MapReduceBase
 * class which implements close and configure methods of Closeable and
 * JobConfigure interfaces respectively and works as a bridge between Mapper and
 * Reducer to pass intermediate data from Mapper to Reducer. This also
 * implements Reducer interface
 * 
 * @author Abhishek
 *
 */
public class WordReducer extends MapReduceBase implements
		Reducer<Text, IntWritable, Text, IntWritable> {

	/**
	 * This method takes output of mapper in the form of key value pairs and after shuffling and sorting
	 * gives output in the form of key value output
	 * 
	 * @param Text
	 *            reducerInputKey
	 * @param Iterator<IntWritable>
	 *            reducerInputValueCollection, This is an iterator generated after shuffling and sorting steps.
	 *            For example (hi, [1,1,1,1,1,1])
	 * @param OutputCollector
	 *            <Text, IntWritable> reducerOutputColection. This is
	 *            collection<key,value> of reducer output
	 * @param Reporter
	 *            reporter. This interface record any exception/issue and report
	 *            to driver class
	 */
	public void reduce(Text reducerInputKey, Iterator<IntWritable> reducerInputValueCollection,
			OutputCollector<Text, IntWritable> reducerOutputColection, Reporter reporter)
			throws IOException {
		
		int count = 0;
		
		while (reducerInputValueCollection.hasNext()) {
			count += reducerInputValueCollection.next().get();
		}

		reducerOutputColection.collect(reducerInputKey, new IntWritable(count));
	}

}

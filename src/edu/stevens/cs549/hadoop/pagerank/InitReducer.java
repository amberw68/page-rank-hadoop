package edu.stevens.cs549.hadoop.pagerank;

import java.io.*;
import java.util.Iterator;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;

public class InitReducer extends Reducer<Text, Text, Text, Text> {

	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		
		StringBuilder sbuilder = new StringBuilder();
		for (Text val : values){
			sbuilder.append(val.toString() + " ");
		}
		context.write(new Text(key.toString() + PageRankDriver.MARKER_DELIMITER + "1.0"), new Text(sbuilder.toString().trim()));


		/* 
		 * TODO: Output key: node+rank, value: adjacency list
		 */

	}
}

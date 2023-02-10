package edu.stevens.cs549.hadoop.pagerank;

import java.io.IOException;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;

public class IterMapper extends Mapper<LongWritable, Text, Text, Text> {

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException,
			IllegalArgumentException {
		String line = value.toString(); // Converts Line to a String
		String[] sections = line.split("\t"); // Splits it into two parts. Part 1: node+rank | Part 2: adj list

		if (sections.length > 2) // Checks if the data is in the incorrect format
		{
			throw new IOException("Incorrect data format");
		}
		if (sections.length != 2) {
			return;
		}
		
		
		String[] pair = sections[0].split("\\"+PageRankDriver.MARKER_DELIMITER);
		double rank = Double.valueOf(pair[1]);
		String[] adjacentList = sections[1].split(" ");
		double weight = rank / adjacentList.length;
		
		for (int i = 0; i < adjacentList.length; i++){
			context.write(new Text(adjacentList[i]), new Text(String.valueOf(weight)));
		}
		context.write(new Text(pair[0]), new Text(PageRankDriver.MARKER_ADJ + ":" + sections[1]));
		
		/* 
		 * TODO: emit key: adj vertex, value: computed weight.
		 * 
		 * Remember to also emit the input adjacency list for this node!
		 * Put a marker on the string value to indicate it is an adjacency list.
		 */


	}

}

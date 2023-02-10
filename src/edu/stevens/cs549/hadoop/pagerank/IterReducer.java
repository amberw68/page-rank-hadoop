package edu.stevens.cs549.hadoop.pagerank;

import java.io.*;
import java.util.*;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;

public class IterReducer extends Reducer<Text, Text, Text, Text> {
	
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		double d = PageRankDriver.DECAY; // Decay factor
		
		Iterator<Text> iterator = values.iterator();
		double currentRank = 0; 
		String adjacentlist = "";
		for (Text val:values){
			String val_s = val.toString();
			if (val_s.startsWith(PageRankDriver.MARKER_ADJ))
				adjacentlist = val_s.split(":")[1];
			else currentRank += Double.valueOf(val.toString());
		}
		
		double value = (1 - d) + currentRank * d;
		context.write(new Text(key.toString() + PageRankDriver.MARKER_DELIMITER + String.valueOf(value)), new Text(adjacentlist));
	
		
		
		
		/* 
		 * TODO: emit key:node+rank, value: adjacency list
		 * Use PageRank algorithm to compute rank from weights contributed by incoming edges.
		 * Remember that one of the values will be marked as the adjacency list for the node.
		 */
	}
}

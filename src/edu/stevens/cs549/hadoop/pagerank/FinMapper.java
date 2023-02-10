package edu.stevens.cs549.hadoop.pagerank;

import java.io.IOException;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;

public class FinMapper extends Mapper<LongWritable, Text, DoubleWritable, Text> {

	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException, IllegalArgumentException {
		String line = value.toString(); // Converts Line to a String
		
		String[] sections = line.split("\t");
		String[] pairing = sections[0].split("\\" + PageRankDriver.MARKER_DELIMITER);
		if (pairing.length == 2 && pairing[1] != null && !pairing[1].equals("null"))
			context.write(new DoubleWritable(-Double.valueOf(pairing[1])), new Text(pairing[0]));
		
		/*
		 * TODO output key:-rank, value: node
		 * See IterMapper for hints on parsing the output of IterReducer.
		 */

	}

}

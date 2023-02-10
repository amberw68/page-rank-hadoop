package edu.stevens.cs549.hadoop.pagerank;

import java.io.*;
import java.util.Iterator;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;

public class DiffRed1 extends Reducer<Text, Text, Text, Text> {

	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		double[] ranks = new double[2];
		int i = 0;
		for (Text val: values){
			ranks[i++] = Double.valueOf(val.toString());
			if (i >= 2) break;
		}
		context.write(new Text(String.valueOf(Math.abs(ranks[0] - ranks[1]))), new Text());
		/* 
		 * TODO: The list of values should contain two ranks.  Compute and output their difference.
		 */

	}
}

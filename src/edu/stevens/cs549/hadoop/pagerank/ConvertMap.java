package edu.stevens.cs549.hadoop.pagerank;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ConvertMap extends Mapper<LongWritable, Text, Text, Text>{
	
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException, IllegalArgumentException {
		String line = value.toString();		
		if (line.contains(":")) {
			//value = node : NAME
			String[] sections = line.split(": ");		
			if (sections.length >= 2) {
				context.write(new Text(sections[0]), 
						new Text("NAME" + PageRankDriver.MARKER_DELIMITER + line.substring(line.indexOf(':') + 1).trim()));
			}
		} else {
			// value = node+rank | adjList
			String[] sections = line.split("\t");
			String[] pair = sections[0].trim().split("\\"+PageRankDriver.MARKER_DELIMITER);
			String rank = pair[1];
			String list = (sections.length > 1) ? sections[1].trim() : "";
			context.write(new Text(pair[0]), new Text("RANK" + PageRankDriver.MARKER_DELIMITER + rank));
			context.write(new Text(pair[0]), new Text(PageRankDriver.MARKER_ADJ + PageRankDriver.MARKER_DELIMITER + list));
		}
	}

}

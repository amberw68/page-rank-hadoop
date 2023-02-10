package edu.stevens.cs549.hadoop.pagerank;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class FindJoinReducer extends Reducer<Text, Text, Text, Text>{
	
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException, IllegalArgumentException{
		Iterator<Text> iterator = values.iterator();
		String nameOfNode = "";
		String rank = "";
		while(iterator.hasNext()) {
			String node = iterator.next().toString();
			if(node.startsWith(PageRankDriver.MARKER_NAME)) {
				nameOfNode = node.replaceAll(PageRankDriver.MARKER_NAME,"");
			}
			if(node.startsWith(PageRankDriver.MARKER_RANK)) {
				rank = node.replaceAll(PageRankDriver.MARKER_RANK, "");
			}
		}
		context.write(new Text(key + "+" + nameOfNode), new Text(rank) );
	}

}

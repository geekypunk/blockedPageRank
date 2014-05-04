package com.cs5300.proj2.simplePRv2;

import java.io.IOException;
import java.util.logging.Logger;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.cs5300.proj2.common.Constants;

public class SimpleMapper extends Mapper<LongWritable, Text, Text, Text> {

	private static Logger LOG = Logger.getLogger(SimpleMapper.class.getName());
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		Text mapperKey = null;
		Text mapperValue = null;
		try{
			String line = value.toString();
			line = line.trim();
			String[] temp = line.trim().split("\\s+");
			
			Text node = new Text(temp[0]);
			double pageRank = Double.valueOf(temp[1]);
			int degree = Integer.valueOf(temp[2]);
			String edgeList = "";
			if (temp.length == 4) {
				edgeList = temp[3];
			}
			mapperKey = new Text(node);
			mapperValue = new Text(Constants.PR_DELIMITER+Constants.TUPLE_DELIMITER + String.valueOf(pageRank) + Constants.TUPLE_DELIMITER + edgeList);
			context.write(mapperKey, mapperValue);
	
			double pageRankFactor = pageRank/(double)degree;
			if(edgeList.length()>0){
				String[] outNodes = edgeList.split(Constants.OUT_NODE_LIST_DELIMITER);
				mapperValue = new Text(String.valueOf(pageRankFactor));
				for (int i = 0; i < outNodes.length; i++) {
					mapperKey = new Text(outNodes[i]);
					context.write(mapperKey, mapperValue);
				}
			}
		}catch(Exception e){
			System.out.println("Mapper:"+value);
			System.out.println(mapperKey+":"+mapperValue);
			e.printStackTrace();
		}
		
	}
}

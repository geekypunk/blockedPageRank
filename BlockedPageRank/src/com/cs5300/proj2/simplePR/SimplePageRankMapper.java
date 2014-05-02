package com.cs5300.proj2.simplePR;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import com.cs5300.proj2.preprocess.Constants;

public class SimplePageRankMapper extends MapReduceBase implements Mapper<LongWritable, Text, IntWritable, Text> {
    	
    	//mapper gets <a b PR(a)>
    	public void map(LongWritable key, Text value, OutputCollector<IntWritable, Text> output, Reporter reporter) throws IOException {
    		System.out.println("mapper got key " + key + " and value " + value);
    		String line = value.toString();
    		line = line.trim();
    		String[] tuple = line.split("\\s+");
    		int nodeID = Integer.parseInt(tuple[0]);
    		double pageRank = Double.valueOf(tuple[1]);
    		int degree = Integer.parseInt(tuple[2]);
    		String[] outNodes = tuple[3].split(",");
    		
    		try {
    			
    			//Write page rank for node ID
    			output.collect(new IntWritable(nodeID), new Text(Constants.PR_DELIMITER + " " + pageRank));
    			
    			
    			for(String outNode : outNodes){
    				output.collect(new IntWritable(Integer.parseInt(outNode)), new Text(Constants.IN_EDGE + " " + String.valueOf(nodeID) + " " + pageRank + " " + pageRank + " " + degree));
    				output.collect(new IntWritable(nodeID), new Text(Constants.OUT_EDGE + " " + String.valueOf(outNode) + " " + pageRank + " " + pageRank + " " + degree));
    			}
    			
    			//IntWritable firstKey = new IntWritable(Integer.parseInt(inputs[0].trim()));
    			//IntWritable secondKey = new IntWritable(Integer.parseInt(inputs[1].trim()));
    			//output.collect(firstKey, value);
    			//output.collect(secondKey, value);
    		} catch (Exception e){
    			System.out.println("mapper got invalid format");
    			e.printStackTrace();
    		}
    		//reporter.incrCounter(COUNTERS.NUM_NODES, 1);
    	}
    }
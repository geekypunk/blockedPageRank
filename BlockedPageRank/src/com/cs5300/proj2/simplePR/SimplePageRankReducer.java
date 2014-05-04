package com.cs5300.proj2.simplePR;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import com.cs5300.proj2.common.Constants;
import com.cs5300.proj2.simplePR.SimplePageRank.COUNTERS;




public class SimplePageRankReducer extends MapReduceBase implements Reducer<IntWritable, Text, IntWritable, Text> {
	//reduce gets <a, a b PR(a) PR(b) deg(a)>
	public void reduce(IntWritable key, Iterator<Text> values, OutputCollector<IntWritable, Text> output, Reporter reporter) throws IOException {
		//System.out.println("reducer got key " + key);
		Set<Edge> inSet = new HashSet<Edge>();
		Set<Edge> outSet = new HashSet<Edge>();
		while (values.hasNext()){
			Text value = values.next();
			System.out.println("\t and value " + value.toString());
			String[] split = value.toString().trim().split("\\s+");
			Edge Edge = new Edge(key.get(), Integer.parseInt(split[1]), 
					Float.parseFloat(split[2]), Integer.parseInt(split[4]));
			if (split[0].equals(Constants.IN_EDGE)){
				inSet.add(Edge);
			} else {
				outSet.add(Edge);
			}
		}
		
		float oldPR = -1, newPR = 0;
		
		for (Edge entry : inSet){
			oldPR = entry.fromNodePR;
			newPR = newPR + (entry.fromNodePR / entry.fromNodeDegree);
			//System.out.println("incremented new pagerank by " + (entry.fromNodePR / entry.fromNodeDegree));
			//System.out.println("fromNodePR is " + entry.fromNodePR + " and fromNodeDegree is " + entry.fromNodeDegree);
		}
		//System.out.println("got NUM_NODES counter: " + Constants.TOTAL_NODES);
		
		//add damping factor
		newPR = (float) (((1- Constants.DAMPING_FACTOR)/ Constants.TOTAL_NODES) + (Constants.DAMPING_FACTOR * newPR));
				
		
		StringBuffer outNodes = new StringBuffer();
		for (Edge entry : outSet){
			outNodes.append(entry.toNode).append(",");
		}
				
			output.collect(null, new Text(key.get() + " " + newPR + " " +outSet.size() + " " + outNodes));
//			oldPR = entry.fromNodePR;
//		}
		//System.out.println("reducer has computed new pageRank " + newPR + " for node " + key.toString());
		float residual = Math.abs((oldPR - newPR)/newPR);
		//System.out.println("reducer has computed residual " + residual);
		reporter.incrCounter(COUNTERS.RESIDUAL_SUM, (int)(residual * Constants.RESIDUAL_OFFSET));
		reporter.incrCounter(COUNTERS.NUM_RESIDUALS, 1);
		//System.out.println("size of inSet is " + inSet.size() + " and size of outSet is " + outSet.size());
	}
}

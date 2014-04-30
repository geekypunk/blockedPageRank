package com.cs5300.proj2.blockedpr;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.cs5300.proj2.preprocess.Constants;



/**
 * Run reducer for every block
 * @author kt466
 *
 */
public class BlockPageRankReducer extends Reducer<Text, Text, Text, Text> {

	private HashMap<String, Double> newPR = new HashMap<String, Double>();
	private HashMap<String, ArrayList<String>> BE = new HashMap<String, ArrayList<String>>();
	private HashMap<String, Double> BC = new HashMap<String, Double>();
	private HashMap<String, Node> nodeDataMap = new HashMap<String, Node>();
	private ArrayList<String> vList = new ArrayList<String>();
	private double dampingFactor =  0.85;
	private double randomJumpFactor = (1 - dampingFactor) /(double) Constants.TOTAL_NODES;
	private int maxIterations = 5;
	
	protected void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		
		Iterator<Text> itr = values.iterator();
		Text input = new Text();
		String[] inputTokens = null;
		
		double pageRankOld =  0.0;
		double residualError =  0.0;
		
		String output = "";
		Integer maxNode = 0;
		
		ArrayList<String> temp = new ArrayList<String>();
		double tempBC = 0.0;
		vList.clear();
		newPR.clear();
		BE.clear();
		BC.clear();
		nodeDataMap.clear();	
		
		while (itr.hasNext()) {
			input = itr.next();
			inputTokens = input.toString().split(" ");			
			// if first element is PR, it is the node ID, previous pagerank and outgoing edgelist for this node
			if (inputTokens[0].equals("PR")) {
				String nodeID = inputTokens[1];
				
				pageRankOld = Double.valueOf(inputTokens[2]);
				newPR.put(nodeID, pageRankOld);
				Node node = new Node();
				node.setNodeID(nodeID);
				node.setPageRank(pageRankOld);
				if (inputTokens.length == 4) {
					node.setEdgeList(inputTokens[3]);
					node.setDegrees(inputTokens[3].split(",").length);
				}
				vList.add(nodeID);
				nodeDataMap.put(nodeID, node);
				// keep track of the max nodeID for this block
				if (Integer.parseInt(nodeID) > maxNode) {
					maxNode = Integer.parseInt(nodeID);
				}
				
			// if BE, it is an in-block edge
			} else if (inputTokens[0].equals("BE")) {			
				
				if (BE.containsKey(inputTokens[2])) {
					//Initialize BC for this v
					temp = BE.get(inputTokens[2]);
				} else {
					temp = new ArrayList<String>();
				}
				temp.add(inputTokens[1]);
				BE.put(inputTokens[2], temp);
				
			// if BC, it is an incoming node from outside of the block
			} else if (inputTokens[0].equals("BC")) {
				if (BC.containsKey(inputTokens[2])) {
					//Initialize BC for this v
					tempBC = BC.get(inputTokens[2]);
				} else {
					tempBC = 0.0;
				}
				tempBC += Double.valueOf(inputTokens[3]);
				BC.put(inputTokens[2], tempBC);
			}		
		}
		
		int i = 0;
		do {
			i++;
			residualError = IterateBlockOnce();
			//System.out.println("Block " + key + " pass " + i + " resError:" + residualError);
		} while (i < maxIterations && residualError > Constants.TERMINATION_RESIDUAL);

				
		// compute the ultimate residual error for each node in this block
		residualError = 0.0;
		for (String v : vList) {
			Node node = nodeDataMap.get(v);
			residualError += Math.abs(node.getPageRank() - newPR.get(v)) /(double) newPR.get(v);
		}
		residualError = residualError /(double) vList.size();
		//System.out.println("Block " + key + " overall resError for iteration: " + residualError);
		
		// add the residual error to the counter that is tracking the overall sum (must be expressed as a long value)
		long residualAsLong = (long) Math.floor(residualError * Constants.RESIDUAL_OFFSET);
		context.getCounter(Counters.RESIDUAL_ERROR).increment(residualAsLong);
		
		// output should be 
		//	key:nodeID (for this node)
		//	value:<pageRankNew> <degrees> <comma-separated outgoing edgeList>
		for (String v : vList) {
			Node node = nodeDataMap.get(v);
			output = newPR.get(v) + " " + node.getDegrees() + " " + node.getEdgeList();
			Text outputText = new Text(output);
			Text outputKey = new Text(v);
			context.write(outputKey, outputText);
			if (v.equals(maxNode.toString())) {
				System.out.println("Block:" + key + " node:" + v + " pageRank:" + newPR.get(v));
			}
		}
			
		cleanup(context);
	}
	

	// v is all nodes within this block B
	// u is all nodes pointing to this set of v
	// some u are inside the block as well, those are in BE
	// some u are outside the block, those are in BC
	// BE = the Edges from Nodes in Block B
    // BC = the Boundary Conditions
	// NPR[v] = Next PageRank value of Node v
	protected double IterateBlockOnce() {
		// used to iterate through the BE list of edges
		ArrayList<String> uList = new ArrayList<String>();
		// npr = current PageRank value of Node v
		double npr = 0.0;
		// r = sum of PR[u]/deg[u] for boundary nodes pointing to v
		double r = 0.0;
		// resErr = the avg residual error for this iteration
		double resErr = 0.0;
		
		for (String v : vList) {
			npr = 0.0;
			double prevPR = newPR.get(v);

			// calculate newPR using PR data from any BE nodes for this node
			if (BE.containsKey(v)) {
				uList = BE.get(v);
				for (String u : uList) {
					// npr += PR[u] / deg(u);
					Node uNode = nodeDataMap.get(u);
					npr += (newPR.get(u) /(double) uNode.getDegrees());
				}
			}
			
			// add on any PR from nodes outside the block (BC)
			if (BC.containsKey(v)) {
				r = BC.get(v);
				npr += r;
			}
	
	        //NPR[v] = d*NPR[v] + (1-d)/N;
			npr = (dampingFactor * npr) + randomJumpFactor;
			// update the global newPR map
			newPR.put(v, npr);
			// track the sum of the residual errors
			resErr += Math.abs(prevPR - npr) /(double) npr;
		}
		// calculate the average residual error and return it
		resErr = resErr /(double) vList.size();
		return resErr;
	}

}


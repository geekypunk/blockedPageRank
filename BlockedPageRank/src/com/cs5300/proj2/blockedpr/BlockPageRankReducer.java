package com.cs5300.proj2.blockedpr;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.cs5300.proj2.common.Constants;
import com.cs5300.proj2.common.Counters;
import com.cs5300.proj2.common.Node;



/**
 * Run reducer for every block, Receives all messages mapped to a single blockID
 * @author kt466
 *
 */
public class BlockPageRankReducer extends Reducer<Text, Text, Text, Text> {

	//New PageRank map <nodeID, pageRank>
	private HashMap<String, Double> NPR = new HashMap<String, Double>();
	private HashMap<String, Double> NPR_BACKUP = new HashMap<String, Double>();
	// <nodeID, {List of vertices to which there is an in-block edge from nodeID}>
	private HashMap<String, ArrayList<String>> BE = new HashMap<String, ArrayList<String>>();
	
	//< nodeIDOfOutsideEdge , flowOfPRFromIt>
	private HashMap<String, Double> BC = new HashMap<String, Double>();
	
	//Map<nodeID, Node> for all nodes in this graph
	private HashMap<String, Node> nodeDataMap = new HashMap<String, Node>();
	
	//List of all nodes in this graph
	private ArrayList<String> vList = new ArrayList<String>();
	
	private double randomJumpFactor = (1 - Constants.DAMPING_FACTOR) /(double) Constants.TOTAL_NODES;
	private int maxIterations = 5;
	
	
	protected void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		
		int blockId = Integer.parseInt(key.toString());
		Iterator<Text> itr = values.iterator();
		Text input = new Text();
		String[] tuple = null;
		
		double pageRankOld =  0.0;
		double residualError =  0.0;
		
		
		int maxNodeID = 0;
		
		ArrayList<String> temp = new ArrayList<String>();
		double tempBC = 0.0;
		vList.clear();
		NPR.clear();
		BE.clear();
		BC.clear();
		NPR_BACKUP.clear();
		nodeDataMap.clear();	
		
		while (itr.hasNext()) {
			
			input = itr.next();
			tuple = input.toString().split(Constants.TUPLE_DELIMITER);			
			
			// if first element is PR, it is the node ID, previous pagerank and outgoing edgelist for this node
			if (tuple[0].equals(Constants.PR_DELIMITER)) {
				String nodeID = tuple[1];
				
				pageRankOld = Double.valueOf(tuple[2]);
				NPR.put(nodeID, pageRankOld);
				Node node = new Node();
				node.setNodeID(nodeID);
				node.setPageRank(pageRankOld);
				if (tuple.length == 4) {
					node.setEdgeList(tuple[3]);
					node.setDegrees(tuple[3].split(Constants.OUT_NODE_LIST_DELIMITER).length);
				}
				vList.add(nodeID);
				nodeDataMap.put(nodeID, node);
				// keep track of the max nodeID for this block
				if (Integer.parseInt(nodeID) > maxNodeID) {
					maxNodeID = Integer.parseInt(nodeID);
				}
				
			// if BE, it is an in-block edge
			} else if (tuple[0].equals(Constants.BE_DELIMITER)) {			
				
				if (BE.containsKey(tuple[2])) {
					//Initialize BC for this v
					temp = BE.get(tuple[2]);
				} else {
					temp = new ArrayList<String>();
				}
				temp.add(tuple[1]);
				BE.put(tuple[2], temp);
				
			// if BC, it is an incoming node from outside of the block
			} else if (tuple[0].equals(Constants.BC_DELIMITER)) {
				if (BC.containsKey(tuple[2])) {
					//Initialize BC for this v
					tempBC = BC.get(tuple[2]);
				} else {
					tempBC = 0.0;
				}
				tempBC += Double.valueOf(tuple[3]);
				BC.put(tuple[2], tempBC);
			}		
		}
		
		int i = 0;
		NPR_BACKUP = new HashMap<>(NPR);
		do {
		
			i++;
			residualError = IterateBlockOnce();

		//Stop whichever happens earlier	
		} while (i < maxIterations && residualError > Constants.TERMINATION_RESIDUAL);

				
		// compute the sum residual error for all node in this block
		residualError = 0.0;
		for (String v : vList) {
			Node node = nodeDataMap.get(v);
			//residualError += Math.abs(NPR_BACKUP.get(v) - NPR.get(v)) /(double) NPR.get(v);
			residualError += Math.abs(node.getPageRank()- NPR.get(v)) /(double) NPR.get(v);
		}
		residualError = residualError /(double) vList.size();
		//System.out.println("Block " + key + " overall resError for iteration: " + residualError);
		
		// add the residual error to the counter that is tracking the overall sum (must be expressed as a long value)
		long residualAsLong = (long) Math.floor(residualError * Constants.RESIDUAL_OFFSET);
		context.getCounter(Counters.RESIDUAL_ERROR).increment(residualAsLong);
		
		// output should be 
		//	key:nodeID (for this node)
		//	value:<pageRankNew> <degrees> <comma-separated outgoing edgeList>
		String output = "";
		for (String v : vList) {
			Node node = nodeDataMap.get(v);
			
			if(node.getEdgeList()!=null){
				output = NPR.get(v) + Constants.TUPLE_DELIMITER  + node.getDegrees() 
						+ Constants.TUPLE_DELIMITER  + node.getEdgeList();
				
			}else{
				
				output = String.valueOf(NPR.get(v));
			}
			
			Text outputText = new Text(output);
			Text outputKey = new Text(v);
			context.write(outputKey, outputText);
			if (v.equals(String.valueOf(maxNodeID))) {
				System.out.println("Block:" + blockId + " node:" + v + " pageRank:" + NPR.get(v));
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
		HashMap<String, Double> NNPR = new HashMap<String, Double>();
		// npr = current PageRank value of Node v
		double npr = 0.0;
		// r = sum of PR[u]/deg[u] for boundary nodes pointing to v
		double r = 0.0;
		// resErr = the avg residual error for this iteration
		double resErr = 0.0;
		
		for (String v : vList) {
			npr = 0.0;
			double prevPR = NPR.get(v);

			// calculate newPR using PR data from any BE nodes for this node
			if (BE.containsKey(v)) {
				uList = BE.get(v);
				for (String u : uList) {
					// npr += PR[u] / deg(u);
					Node uNode = nodeDataMap.get(u);
					npr += (NPR.get(u) /(double) uNode.getDegrees());
				}
			}
			
			// add on any PR from nodes outside the block (BC)
			if (BC.containsKey(v)) {
				r = BC.get(v);
				npr += r;
			}
	
	        //NPR[v] = d*NPR[v] + (1-d)/N;
			npr = (Constants.DAMPING_FACTOR * npr) + randomJumpFactor;
			// update the global newPR map
			NNPR.put(v, npr);
			//NPR.put(v, npr);
			// track the sum of the residual errors
			resErr += Math.abs(prevPR - npr) /(double) npr;
		}
		NPR = new HashMap<>(NNPR);
		// calculate the average residual error and return it
		resErr = resErr /(double) vList.size();
		return resErr;
	}
	
	
}


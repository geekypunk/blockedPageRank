package com.cs5300.proj2.simplePR;

import java.io.IOException;
import java.util.Iterator;
import java.util.logging.Logger;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.cs5300.proj2.common.Constants;
import com.cs5300.proj2.common.Counters;

/**
 * Reducer for SimplePageRank Computation
 * @author dr472
 *
 */
public class SimplePageRankReducer extends Reducer<Text, Text, Text, Text> {

	private static final double randomJumpFactor = (1 - Constants.DAMPING_FACTOR) / (double)Constants.TOTAL_NODES;
	
	private static Logger LOG = Logger.getLogger(SimplePageRankReducer.class.getName());
	
	protected void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		
		//<key: node ID, value: page_rank out_nodes_comma_separated>
		//<key: out_node_ID, value: page_rank_factor>
		Text outputText = null;
		try{
			Iterator<Text> itr = values.iterator();
			Text input = new Text();
			String[] inputTokens = null;
			
			double pageRankIncomingSum =  0.0;
			
			double pageRankNew =  0.0;
			double pageRankOld =  0.0;
			double residualError = 0.0;
			
			String edgeList = "";
			String output = "";
	
			while (itr.hasNext()) {
				input = itr.next();
				inputTokens = input.toString().trim().split("\\s+");		
				
				//Get the old page rank, if the value has PR delimiter
				if (inputTokens[0].equals(Constants.PR_DELIMITER)) {
					pageRankOld = Float.parseFloat(inputTokens[1]);
					if (inputTokens.length == 3) {
						edgeList = inputTokens[2];
					} else {
						edgeList = "";
					}
				}
				
				//Otherwise add the pagerank factor values for out nodes
				else {
					double pageRankFactor = Double.valueOf(inputTokens[0]);
					pageRankIncomingSum += pageRankFactor;
				}
				
			}
			
			//Compute the new page rank
			pageRankNew = (Constants.DAMPING_FACTOR * pageRankIncomingSum) + randomJumpFactor;
			
			//Compute residual error
			residualError = Math.abs(pageRankOld - pageRankNew) / (double)pageRankNew;
			
			// add the residual error to the counter that is tracking the overall sum (must be expressed as a long value)
			long residualAsLong = (long) Math.floor(residualError * Constants.RESIDUAL_OFFSET);
			context.getCounter(Counters.RESIDUAL_ERROR).increment(residualAsLong);
			
			int degrees = edgeList.split(Constants.OUT_NODE_LIST_DELIMITER).length;
			
			//output format should be same as what the mapper expects
			//key:nodeID (for this node)
			//value:<pageRankNew> <degrees> <comma-separated outgoing edgeList>
			output = pageRankNew + Constants.TUPLE_DELIMITER + degrees + Constants.TUPLE_DELIMITER + edgeList;
			outputText = new Text(output);
			context.write(key, outputText);
		}catch(Exception e){
			LOG.info("Reduced for:"+values);
			LOG.info(outputText.toString());
			e.printStackTrace();
		}
	}

}

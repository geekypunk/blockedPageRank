package com.cs5300.proj2.blockedpr;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * <p><b>Map task for each block.
 * 
 * </b></p>
 * @author kt466
 *
 */
public class BlockPageRankMapper extends Mapper<LongWritable, Text, Text, Text>{
	
	
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		
		String line = value.toString();
		line = line.trim();
		String[] tuple = line.split("\\s+");
		int nodeID = Integer.parseInt(tuple[0]);
		double pageRank = Double.valueOf(tuple[1]);
		int degree = Integer.parseInt(tuple[2]);
		String[] outNodes = tuple[3].split(",");
		
		int blockID = new Integer(lookupBlockID(nodeID));
		
	
		Text mapperKey = new Text(String.valueOf(blockID));
		Text mapperValue = new Text("PR " + nodeID + " " + String.valueOf(pageRank) + " "
				+ tuple[3]);
	
		//Output tuple ( key , value )
		//= < blockID , PR nodeID pageRank {outGoingEdgeList}>
		context.write(mapperKey, mapperValue);
		
	
		for (int i = 0; i < outNodes.length; i++) {
			int out = (int) Double.valueOf(outNodes[i]).longValue();
			int blockIDOut = lookupBlockID(out);
			mapperKey = new Text(String.valueOf(blockIDOut));
			
			//Check if both the nodes are in the same block.
			if (blockIDOut == blockID) {
				
				// Output key:blockID value:BE node nodeOut
				mapperValue = new Text("BE " + String.valueOf(nodeID) + " " + outNodes[i]);
			
			} else {
				
				// the pageRankFactor is used by the reducer to calculate the new
				// pageRank for the outgoing edges
				
				double pageRankFactor = (pageRank / (double)degree);
				
				String pageRankFactorString = String.valueOf(pageRankFactor);
				// map key:blockID value:BC node nodeOut pageRankFactor 
				mapperValue = new Text("BC " +  String.valueOf(nodeID) + " " + outNodes[i] + " " + pageRankFactorString);
			}
			context.write(mapperKey, mapperValue);
		}
	
	}
	
	
		
	/**
	 * lookup the block ID in a hardcoded list based on the node ID
	 * @param nodeID
	 * @return
	 */
	public static int lookupBlockID(int nodeID) {
	
		int partitionSize = 10000;
		
		int[] blockBoundaries = { 0, 10328, 20373, 30629, 40645,
				50462, 60841, 70591, 80118, 90497, 100501, 110567, 120945,
				130999, 140574, 150953, 161332, 171154, 181514, 191625, 202004,
				212383, 222762, 232593, 242878, 252938, 263149, 273210, 283473,
				293255, 303043, 313370, 323522, 333883, 343663, 353645, 363929,
				374236, 384554, 394929, 404712, 414617, 424747, 434707, 444489,
				454285, 464398, 474196, 484050, 493968, 503752, 514131, 524510,
				534709, 545088, 555467, 565846, 576225, 586604, 596585, 606367,
				616148, 626448, 636240, 646022, 655804, 665666, 675448, 685230 };

		int blockID = (int) Math.floor(nodeID / partitionSize);
		int testBoundary = blockBoundaries[blockID];
		if (nodeID < testBoundary) {
			blockID--;
		}
		return blockID;
		
	}
}

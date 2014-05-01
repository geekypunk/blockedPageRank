package com.cs5300.proj2.preprocess;

public final class Constants {

	/**
	 * AWS secret key
	 */
	public static final String AWSSecretKey="KIYDpL6OoZ+/7KhAn+HRwL7ra7pVU098nA4gBpdq";
	/**
	 * AWS access key
	 */
	public static final String AWSAccessKeyId="AKIAJL4PGYQJWXPV6YHA";
	
	/**
	 * Reject min calculated on netID kt466 
	 */
	public static final double REJECT_MIN=0.65736;
	
	/**
	 * Reject limit calculated on netID kt466 
	 */
	public static final double REJECT_LIMIT=0.66736;
	
	/**
	 * Total number of nodes
	 */
	public static final int TOTAL_NODES = 685230;
	
	/**
	 * Total number of blocks
	 */
	public static final int TOTAL_BLOCKS = 68;

	/**
	 * Termination criteria of 0.1% residual error
	 */
	public static final double TERMINATION_RESIDUAL = 0.001;
	
	 /**
	 * Used by Hadoop counter for precision
	 */
	public static final int RESIDUAL_OFFSET = 10000000;
	
	/**
	 * Initial pagerank for all the nodes in the graph
	 */
	public static final double INIT_PR = 1/(double)TOTAL_NODES;
	
	/**
	 * Number of nodes in each block
	 */
	public static final int BLOCK_SIZE = 10000;

}

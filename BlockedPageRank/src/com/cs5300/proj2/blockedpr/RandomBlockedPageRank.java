package com.cs5300.proj2.blockedpr;

/**
 * This class sets the random blocking field to true, in order to partition nodes randomly
 * @author nsk53
 *
 */
public class RandomBlockedPageRank {
	public static void main(String args[]){
		BlockedPageRank.runJob(args[0], args[1], true);
	}
}

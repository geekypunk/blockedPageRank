package com.cs5300.proj2.blockedpr;

/**
 * This class sets the random blocking field to true, in order to partition nodes randomly
 * @author nsk53
 *
 */
public class RandomBlockedPageRank {
	public static void main(String args[]){
		/*
		 * S3 location of preprocessed input file : 
		 * 				s3n://edu-cornell-cs-cs5300s14-kt466-proj2/preprocessedInputKT466.txt
		 * */
		BlockedPageRank.runJob(args[0], args[1], true);
	}
}

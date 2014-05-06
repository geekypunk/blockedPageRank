package com.cs5300.proj2.blockedpr;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.cs5300.proj2.common.Constants;
import com.cs5300.proj2.common.Counters;



/**
 * <p>Implements the  Blocked version of PageRank algorithm</p>
 * @author kt466
 *
 */
public class BlockedPageRank {

	/*
	 * S3 location of preprocessed input file : 
	 * 				s3n://edu-cornell-cs-cs5300s14-kt466-proj2/preprocessedInputKT466.txt
	 * */
	public static String intermediateFilteredEdge="/home/kira/blockedPageRank/preprocessedEdgesKt466.txt";
	public static String preprocessedFileOut="/home/kira/blockedPageRank/preprocessedInputKT466.txt";
	public static void  main(String[] args) {
		
		
		if (args.length != 2) {
			System.err.println("Program args: s3n://<bucketname><filename> "
					+ "	s3n://<bucketname>");
			System.exit(2);
		}
	
		try{
		
			//PreprocessInput.createFilteredEdgesFileLocally(inputFile,intermediateFilteredEdge);
			//PreprocessInput.createPreprocessedInputFile(intermediateFilteredEdge, preprocessedFileOut);
		
		}catch(Exception e){
			e.printStackTrace();
			return;
		}
		
		runJob(args[0], args[1], true);
	}
	
	public static void runJob(String inputFile, String outputPath, boolean useRandomBlocking){
		boolean success = false;
		int i = 0;
		double residualErrorAvg = 0.0f;
		double residualError = 0.0f;
		do{
			try{
			
				Job job = new Job();
	            
				// Set a unique job name
	            job.setJobName("blockedPrIter_"+ i);
	            job.setJarByClass(BlockedPageRank.class);
	            
	                     
	            //True for random partitioning
	            BlockPageRankMapper.use_random_blocking = useRandomBlocking;
	            
	            // Set Mapper and Reducer class
	            job.setMapperClass(BlockPageRankMapper.class);
	            job.setReducerClass(BlockPageRankReducer.class);
	            
	           //AWS credentials, to access input and output files
		    	job.getConfiguration().set("fs.s3n.awsAccessKeyId", Constants.AWSAccessKeyId);
		    	job.getConfiguration().set("fs.s3n.awsSecretAccessKey", Constants.AWSSecretKey);
	
	            // set the classes for output key and value
	            job.setOutputKeyClass(Text.class);
	            job.setOutputValueClass(Text.class);
	            
	            // on the initial pass, use the preprocessed input file
	            // note that we use the default input format which is TextInputFormat (each record is a line of input)
	            if (i == 0) {
	                FileInputFormat.addInputPath(job, new Path(inputFile)); 	
	            // otherwise use the output of the last pass as our input
	            } else {
	            	FileInputFormat.addInputPath(job, new Path(outputPath + "/run"+i)); 
	            }
	            // set the output file path
	            FileOutputFormat.setOutputPath(job, new Path(outputPath + "/run"+(i+1)));
	            
	            // execute the job and wait for completion before starting the next pass
	           
	            try{
	            
	            	success = job.waitForCompletion(true);
	          
	            }catch(Exception e){
	            	e.printStackTrace();
	            }
	            
	            // before starting the next pass, compute the avg residual error for this pass and print it out
	            residualError = job.getCounters().findCounter(Counters.RESIDUAL_ERROR).getValue() / (double)Constants.RESIDUAL_OFFSET;
	            residualErrorAvg =   residualError /(double) Constants.TOTAL_BLOCKS;
	            System.out.println(residualErrorAvg);
	            String residualErrorString = String.format("%.4f", residualErrorAvg);
	            
	            //Print average residual error over all blocks
	            System.out.println("Average Residual Error for iteration " + i + ": " + residualErrorString);
	            
	            // reset the counter for the next round
	            job.getCounters().findCounter(Counters.RESIDUAL_ERROR).setValue(0L);
	            i++;
			}catch(Exception e){
				
				e.printStackTrace();
			}
			
		}while(residualErrorAvg > Constants.TERMINATION_RESIDUAL);
		
		System.out.println("Converged!!!");

	}

}

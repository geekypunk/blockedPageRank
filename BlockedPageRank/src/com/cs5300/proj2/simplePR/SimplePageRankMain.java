package com.cs5300.proj2.simplePR;


import java.util.logging.Logger;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.cs5300.proj2.common.Constants;
import com.cs5300.proj2.common.Counters;

/**
 * This class computes page rank on a none-by-node basis using Haddop
 * @author dr472
 *
 */
public class SimplePageRankMain {

	private static final int NUM_ITERATIONS = 25; // # of iterations to run

	private static Logger LOG = Logger.getLogger(SimplePageRankMain.class.getName());
	public static void main(String[] args) throws Exception {

		//Input file: s3n://edu-cornell-cs-cs5300s14-kt466-proj2/preprocessedInputKT466v2.txt
		//Output : s3n://edu-cornell-cs-cs5300s14-kt466-proj2/simple_page_rank/runs
		String inputFile = args[0];
		String outputPath = args[1];

		LOG.info(inputFile);
		LOG.info(outputPath);
		
		//We run for five iterations, as convergence takes longer
		for (int i = 0; i < NUM_ITERATIONS; i++) {
			
			Job job = new Job();
			
			//Setup job
			job.setJobName("simplePR_" + (i + 1));
			job.setJarByClass(SimplePageRankMain.class);
			
			//AWS credentials, to access input and output files
	    	job.getConfiguration().set("fs.s3n.awsAccessKeyId", Constants.AWSAccessKeyId);
	    	job.getConfiguration().set("fs.s3n.awsSecretAccessKey", Constants.AWSSecretKey);
			
	    	//Mapper and reducer class
			job.setMapperClass(SimplePageRankMapper.class);
			job.setReducerClass(SimplePageRankReducer.class);

			//Output format
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);

			 // on the initial pass, use the preprocessed input file
            // note that we use the default input format which is TextInputFormat (each record is a line of input)
			if (i == 0) {
				FileInputFormat.addInputPath(job, new Path(inputFile));
				
			} else {
				// otherwise use the output of the last pass as our input
				FileInputFormat.addInputPath(job, new Path(outputPath + "/run"
						+ i));
			}
			
			//Output file path
			FileOutputFormat.setOutputPath(job, new Path(outputPath + "/run"
					+ (i + 1)));

		
			//Run the job and wait for its completion
			job.waitForCompletion(true);

			//Use Hadoop Counters to calculate residual error average
			float residualErrorAvg = job.getCounters()
					.findCounter(Counters.RESIDUAL_ERROR).getValue();
			residualErrorAvg = (residualErrorAvg / Constants.RESIDUAL_OFFSET) / Constants.TOTAL_NODES;
			
			//System.out.println(residualErrorAvg);
			String residualErrorString = String
					.format("%.4f", residualErrorAvg);
			System.out.println("Average Residual error for iteration " + i + ": "
					+ residualErrorString);

			//Reset counters
			job.getCounters().findCounter(Counters.RESIDUAL_ERROR)
					.setValue(0L);
		}

	}

}

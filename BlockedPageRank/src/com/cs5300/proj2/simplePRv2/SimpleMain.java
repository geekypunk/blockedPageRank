package com.cs5300.proj2.simplePRv2;


import java.util.logging.Logger;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.cs5300.proj2.common.Constants;
import com.cs5300.proj2.common.Counters;

public class SimpleMain {

	private static final int NUM_ITERATIONS = 7; // # of iterations to run

	private static Logger LOG = Logger.getLogger(SimpleMain.class.getName());
	public static void main(String[] args) throws Exception {

		String inputFile = args[0];
		String outputPath = args[1];

		LOG.info(inputFile);
		LOG.info(outputPath);
		
		for (int i = 0; i < NUM_ITERATIONS; i++) {
			
			Job job = new Job();
			
			job.setJobName("pagerank_" + (i + 1));
			job.setJarByClass(SimpleMain.class);

			
			job.setMapperClass(SimpleMapper.class);
			job.setReducerClass(SimpleReducer.class);

			
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);

			if (i == 0) {
				FileInputFormat.addInputPath(job, new Path(inputFile));
				
			} else {
				FileInputFormat.addInputPath(job, new Path(outputPath + "/run"
						+ i));
			}
			
			FileOutputFormat.setOutputPath(job, new Path(outputPath + "/run"
					+ (i + 1)));

		
			job.waitForCompletion(true);

			
			float residualErrorAvg = job.getCounters()
					.findCounter(Counters.RESIDUAL_ERROR).getValue();
			residualErrorAvg = (residualErrorAvg / Constants.RESIDUAL_OFFSET) / Constants.TOTAL_NODES;
			String residualErrorString = String
					.format("%.4f", residualErrorAvg);
			LOG.info("Residual error for iteration " + i + ": "
					+ residualErrorString);

			job.getCounters().findCounter(Counters.RESIDUAL_ERROR)
					.setValue(0L);
		}

	}

}

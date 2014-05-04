package com.cs5300.proj2.simplePR;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

import com.cs5300.proj2.common.Constants;


public class SimplePageRank {
	
	public static int RESIDUAL_OFFSET = 1000000000;
	public static double DAMPING_FACTOR = 0.85;
	public static int NUM_NODES = 685229;
	
	public static enum COUNTERS {
		RESIDUAL_SUM,
		NUM_RESIDUALS
	};
	
    public static void main(String[] args) throws Exception {
//    	String inputFile = "s3n://edu-cornell-cs-cs5300s14-kt466-proj2/preprocessedInputKT466v2.txt";
    	String inputFile = args[0];
		String outputPath = args[1];
	
//		AWSCredentials myCredentials = new BasicAWSCredentials(
//			       String.valueOf(Constants.AWSAccessKeyId), String.valueOf(Constants.AWSSecretKey));
//		AmazonS3Client s3Client = new AmazonS3Client(myCredentials);  
//		S3Object object = s3Client.getObject(new GetObjectRequest("edu-cornell-cs-cs5300s14-project2", "edges.txt"));
//		BufferedReader reader = new BufferedReader(new InputStreamReader(
//			       object.getObjectContent()));
//		
//    	String line = reader.readLine();
//    	while(line != null){
//    		System.out.println(line);
//    		line = reader.readLine();
//    	}
//		
		for (int i = 0; i < 5; i++){
    		
    		//Create job config and set name
    		JobConf conf = new JobConf(SimplePageRank.class);
	    	conf.setJobName("simplePageRank" + i);

		       
	 	   
	    	conf.set("fs.s3n.awsAccessKeyId", Constants.AWSAccessKeyId);
            conf.set("fs.s3n.awsSecretAccessKey", Constants.AWSSecretKey);
	    	conf.setOutputKeyClass(IntWritable.class);
	    	conf.setOutputValueClass(Text.class);
	
	    	conf.setMapperClass(SimplePageRankMapper.class);
	    	//conf.setCombinerClass(Combine.class);
	    	conf.setReducerClass(SimplePageRankReducer.class);
	
	    	
	    	conf.setInputFormat(TextInputFormat.class);
	    	conf.setOutputFormat(TextOutputFormat.class);
    	  
	    	//FileInputFormat.setInputPaths(conf, new Path("/home/ben/Documents/5300/hadoop_io_3/temp/file" + i));
	    	//FileOutputFormat.setOutputPath(conf, new Path("/home/ben/Documents/5300/hadoop_io_3/temp/file" + (i+1)));
    	  
	    	// on the initial pass, use the preprocessed input file
            // note that we use the default input format which is TextInputFormat (each record is a line of input)
            if (i == 0) {
                FileInputFormat.addInputPath(conf, new Path(inputFile)); 	
            // otherwise use the output of the last pass as our input
            } else {
            	FileInputFormat.addInputPath(conf, new Path(outputPath + "/run"+i)); 
            }
            // set the output file path
            FileOutputFormat.setOutputPath(conf, new Path(outputPath + "/run"+(i+1)));
	    	
    	  
	    	RunningJob job = JobClient.runJob(conf);
	    	Counters counters = job.getCounters();
	    	float residualSum = ((float)counters.getCounter(COUNTERS.RESIDUAL_SUM))/RESIDUAL_OFFSET;
	    	float numResiduals = counters.getCounter(COUNTERS.NUM_RESIDUALS);
	    	float residualAverage = residualSum / numResiduals;
	    	System.out.println("Residual sum for this pass: " + residualSum);
	    	System.out.println("Average residual for this pass: " + residualAverage);
      }
    }
}



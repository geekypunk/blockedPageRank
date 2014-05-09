package com.cs5300.proj2.preprocess;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.AbortMultipartUploadRequest;
import com.amazonaws.services.s3.model.CompleteMultipartUploadRequest;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadResult;
import com.amazonaws.services.s3.model.PartETag;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.UploadPartRequest;
import com.cs5300.proj2.common.Constants;


/**
 * <p>This class preprocesses the edges.txt files.</p>
 * <p>Using NetID kt466</p>
 * <p>fromNetID = 0.664 (Reversed NetID)</p>
 * <p>rejectMin = 0.99 * fromNetID;</p>
 * <p>rejectLimit = rejectMin + 0.01;</p>
 * 
 * <p>Reject Min: 0.65736</p>
 * <p>Reject Limit: 0.66736</p>
 * <p>Rejects approximately 0.84706526265% of the edges</p>
 * <p>Total edges after rejection = 7524964</p>
 * 

 * @author kt466
 *
 */
public class PreprocessInput {
	
	
	public static void main(String[] args) throws IOException {
		
		try{
			
			/*
			AWSCredentials myCredentials = new BasicAWSCredentials(
				       String.valueOf(Constants.AWSAccessKeyId), String.valueOf(Constants.AWSSecretKey));
			AmazonS3Client s3Client = new AmazonS3Client(myCredentials); 
			uploadToS3(s3Client,"/home/kira/blockedPageRank/dummy.txt",
					"edu-cornell-cs-cs5300s14-kt466-proj2",
					"preprocessedInputKt466v2.txt");*/
			
			PreprocessInput.createFilteredEdgesFileLocally("/home/kira/edges.txt","/home/kira/preprocessEdges.txt");
			Map<String,Boolean> allNodes = getAllNodes("/home/kira/preprocessEdges.txt");
			System.out.println("Nodes in preprocessed:"+allNodes.size());
			PreprocessInput.createPreprocessedInputFile(allNodes,"/home/kira/preprocessEdges.txt", "/home/kira/preprocessFinal.txt");
			
			
		}catch(Exception e){
		
			e.printStackTrace();
		}
		
		
	}

	public static void createFilteredEdgesFile(String outPath) throws NumberFormatException, IOException{
		
		 
			
		AWSCredentials myCredentials = new BasicAWSCredentials(
			       String.valueOf(Constants.AWSAccessKeyId), String.valueOf(Constants.AWSSecretKey));
		AmazonS3Client s3Client = new AmazonS3Client(myCredentials);  
		S3Object object = s3Client.getObject(new GetObjectRequest("edu-cornell-cs-cs5300s14-project2", "edges.txt"));
		BufferedReader reader = new BufferedReader(new InputStreamReader(
			       object.getObjectContent()));
		File file = new File(outPath);      
		Writer writer = new OutputStreamWriter(new FileOutputStream(file));
		String line;
		String[] parts;
		double randVal;
		int edgeCount = 0;
		while ((line = reader.readLine()) != null) {          
			  parts = line.trim().split("\\s+");
		     randVal = Double.parseDouble(parts[2]);
		     if (selectInputLine(randVal)){
		          System.out.println(line); 
		          edgeCount++;
		          writer.write(line + "\n");
		     }
		}
		writer.close();
		System.out.println("Edge count:"+edgeCount);
	}
	

	public static void createFilteredEdgesFileLocally(String inPath,String outPath) throws NumberFormatException, IOException{
		
	
		BufferedReader reader = new BufferedReader(new FileReader(
				inPath));
		File file = new File(outPath);      
		Writer writer = new OutputStreamWriter(new FileOutputStream(file));
		String line;
		String[] parts;
		double randVal;
		int edgeCount = 0;
		while ((line = reader.readLine()) != null) {          
		     parts = line.trim().split("\\s+");
		     randVal = Double.parseDouble(parts[2]);
		     if (selectInputLine(randVal)){
		          System.out.println(line); 
		          edgeCount++;
		          writer.write(line + "\n");
		     }
		}
		writer.close();
		reader.close();
		System.out.println("Edge count:"+edgeCount);
	}
	private static boolean selectInputLine(double x) {
		return ( ((x >= Constants.REJECT_MIN) && (x < Constants.REJECT_LIMIT)) ? false : true );
	}
	
	public static void createPreprocessedInputFile(Map<String,Boolean> allNodes,String inPath,String outPath) throws IOException{
		
		File inFile = new File(inPath); 
		//File inFile = new File("test"); 
		File outFile = new File(outPath);      
		BufferedReader reader = new BufferedReader(new FileReader(inFile));
		FileWriter fw = new FileWriter(outFile);
		BufferedWriter bw = new BufferedWriter(fw);
		String line;
		String[] parts;
		int oldnode;
		int currentNode;
		line = reader.readLine();
		parts = line.trim().split("\\s+");
		oldnode = Integer.parseInt(parts[0]);
		allNodes.put(parts[0],true);
		List<Integer> outNodes = new ArrayList<Integer>();
		StringBuilder sb;
		while ((line = reader.readLine()) != null) {      
			  parts = line.trim().split("\\s+");
			  currentNode = Integer.parseInt(parts[0]);
			  if(oldnode != currentNode){
				  sb = new StringBuilder();
				  for(int i=0;i<outNodes.size()-1;i++){
					  sb.append(outNodes.get(i)).append(",");
				  }
				  sb.append(outNodes.get(outNodes.size()-1));
				  allNodes.put(parts[0],true);
				  
				  bw.write(oldnode+" "+Constants.INIT_PR+" "+outNodes.size()+" "+sb.toString()+"\n");
				  oldnode = currentNode;
				  outNodes = new ArrayList<Integer>();
				  outNodes.add(Integer.parseInt(parts[1]));
				  
			  }else{
				  outNodes.add(Integer.parseInt(parts[1]));
			  }
			
		}
		sb = new StringBuilder();
		for(int i=0;i<outNodes.size()-1;i++){
			sb.append(outNodes.get(i)).append(",");
		}
		sb.append(outNodes.get(outNodes.size()-1));
		bw.write(oldnode+" "+Constants.INIT_PR+" "+outNodes.size()+" "+sb.toString()+"\n");
		

	
		int i=0;
		for (Map.Entry<String, Boolean> entry : allNodes.entrySet())
		{
		    if(!entry.getValue()){
		    	i++;
		    	System.out.println("NoOut:"+entry.getKey()+" "+Constants.INIT_PR+"\n");
		    	bw.write(entry.getKey()+" "+Constants.INIT_PR+"\n");
		    }
		}

		System.out.println("Added "+i +" extra nodes");	
		bw.close();
		reader.close();
	}
	
	
	private static void uploadToS3(AmazonS3Client s3Client,String localFile, String bucketName, String keyName){
		
		//Create a list of UploadPartResponse objects. You get one of these for
		//each part upload.
		List<PartETag> partETags = new ArrayList<PartETag>();
		
		//Step 1: Initialize.
		InitiateMultipartUploadRequest initRequest = new InitiateMultipartUploadRequest(
				bucketName, keyName);
		InitiateMultipartUploadResult initResponse = 
		                          s3Client.initiateMultipartUpload(initRequest);
		
		File file = new File(localFile);
		long contentLength = file.length();
		long partSize = 5 * 1024 * 1024; // Set part size to 5 MB.
		
		try {
		// Step 2: Upload parts.
		long filePosition = 0;
		for (int i = 1; filePosition < contentLength; i++) {
		    // Last part can be less than 5 MB. Adjust part size.
		partSize = Math.min(partSize, (contentLength - filePosition));
		
		// Create request to upload a part.
		UploadPartRequest uploadRequest = new UploadPartRequest()
		    .withBucketName(bucketName).withKey(keyName)
		    .withUploadId(initResponse.getUploadId()).withPartNumber(i)
		    .withFileOffset(filePosition)
		    .withFile(file)
		    .withPartSize(partSize);
		
		// Upload part and add response to our list.
		    partETags.add(s3Client.uploadPart(uploadRequest).getPartETag());
		
		    filePosition += partSize;
		}
		
		// Step 3: Complete.
		CompleteMultipartUploadRequest compRequest = new 
		            CompleteMultipartUploadRequest(bucketName, 
		                                           keyName, 
		                                           initResponse.getUploadId(), 
		                                           partETags);
		
		s3Client.completeMultipartUpload(compRequest);
		} catch (Exception e) {
		s3Client.abortMultipartUpload(new AbortMultipartUploadRequest(
				bucketName, keyName, initResponse.getUploadId()));
		}
				
	}
	
	private static Map<String,Boolean> getAllNodes(String inPath) throws IOException{
		
		File inFile = new File(inPath); 
		BufferedReader reader = new BufferedReader(new FileReader(inFile));
		String line;
		String[] parts = null;
		Map<String,Boolean> allNodes = new HashMap<>();
		while ((line = reader.readLine()) != null) {  
			  parts = line.trim().split("\\s+");
			  allNodes.put(parts[0],false);
			  allNodes.put(parts[1],false);
			
		}   
		reader.close();
		return allNodes;
	}
}

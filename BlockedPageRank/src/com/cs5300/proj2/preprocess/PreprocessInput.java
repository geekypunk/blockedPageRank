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
import java.util.List;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;


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

 * @author kt466
 *
 */
public class PreprocessInput {
	
	
	public static void main(String[] args) throws IOException {
		
		try{
		
			//createFilteredEdgesFile();
			createPreprocessedInputFile();
			
		}catch(Exception e){
		
			e.printStackTrace();
		}
		
		
	}

	private static void createFilteredEdgesFile() throws NumberFormatException, IOException{
		
		AWSCredentials myCredentials = new BasicAWSCredentials(
			       String.valueOf(Constants.AWSAccessKeyId), String.valueOf(Constants.AWSSecretKey)); 
			
		
		AmazonS3Client s3Client = new AmazonS3Client(myCredentials);  
		S3Object object = s3Client.getObject(new GetObjectRequest("edu-cornell-cs-cs5300s14-project2", "edges.txt"));
		BufferedReader reader = new BufferedReader(new InputStreamReader(
			       object.getObjectContent()));
		File file = new File("preprocessed-edges-kt466.txt");      
		Writer writer = new OutputStreamWriter(new FileOutputStream(file));
		String line;
		String[] parts;
		double randVal;
		int edgeCount = 0;
		while ((line = reader.readLine()) != null) {          
		     parts = line.split("\\s+");
		     randVal = Double.parseDouble(parts[3]);
		     if (selectInputLine(randVal)){
		          System.out.println(line); 
		          edgeCount++;
		          writer.write(line + "\n");
		     }
		}
		writer.close();
		System.out.println("Edge count:"+edgeCount);
	}
	private static boolean selectInputLine(double x) {
		return ( ((x >= Constants.REJECT_MIN) && (x < Constants.REJECT_LIMIT)) ? false : true );
	}
	
	private static void createPreprocessedInputFile() throws IOException{
		
		File inFile = new File("/home/kira/preprocessedEdgesKt466.txt"); 
		//File inFile = new File("test"); 
		File outFile = new File("/home/kira/preprocessedInputKT466.txt");      
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
		bw.close();
	}
	
	
	
}

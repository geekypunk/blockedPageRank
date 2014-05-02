package com.cs5300.proj2.simplePR;

public class Edge {
	public int fromNode;
	public int toNode;
	public float fromNodePR;
	public int fromNodeDegree;
	
	public Edge(int fromNode, int toNode, float fromNodePR, int fromNodeDegree){
		this.fromNode = fromNode;
		this.toNode = toNode;
		this.fromNodePR = fromNodePR;
		this.fromNodeDegree = fromNodeDegree;
	}
}

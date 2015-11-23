package edu.uiuc.cs425;

public abstract class TopologyBuilder {

	abstract void setBolt(String boltName, int parallelismLevel, String parentName, String groupingType);
	abstract void setSpout(String spoutName, int parallelismLevel);
	
}

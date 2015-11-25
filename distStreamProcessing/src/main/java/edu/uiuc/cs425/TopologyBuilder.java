package edu.uiuc.cs425;

import java.util.HashMap;

/* We only support creation of a tree like topology.
 * Only one spout allowed due to this reason. Only one topology can be 
 * created by the builder
 * Usage:
 * TopologyBuilder(toponame)
 * SetSpout(compName, className, parallelism)
 * SetBolt(compName, className, parentComp, grouping, parallelism)
 * ... more spouts
 * Topology topology = GetTopology(toponame)
 */


public  class TopologyBuilder {
	
	private Topology topology;
	private boolean	 spout;
	
	public TopologyBuilder(String name)
	{
		topology = new Topology(name);
		spout = false;
	}
	
	int setBolt(String boltName, String className, String parentName, int groupingType, 
			int parallelismLevel)
	{
		//check if parent is there, else return false
		if( !topology.isAvailable(parentName)) return Commons.FAILURE;
		
		TopologyComponent component = new TopologyComponent(boltName, className, Commons.BOLT,
				topology.Get(parentName), groupingType, parallelismLevel);
		topology.Add(component);
		return Commons.SUCCESS;
	}
	int setSpout(String spoutName, String className, int parallelismLevel)
	{
		if(spout) return Commons.FAILURE;
		spout = true;
		TopologyComponent component = new TopologyComponent(spoutName, className, Commons.SPOUT,
				null, -1, parallelismLevel);
		topology.Add(component);
		return Commons.SUCCESS;
	}
	
	Topology GetTopology()
	{
		return topology;
	}
	
}

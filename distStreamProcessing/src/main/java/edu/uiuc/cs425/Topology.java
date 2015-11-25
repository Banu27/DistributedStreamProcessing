package edu.uiuc.cs425;

import java.util.HashMap;

public class Topology {
	String sTopologyName;
	private HashMap<String, TopologyComponent> topology;
	private String spoutName;
	public Topology(String name)
	{
		sTopologyName = name;
		topology = new HashMap<String, TopologyComponent>();
		spoutName = null;
	}
	
	public boolean isAvailable(String comp)
	{
		return topology.containsKey(comp);
	}
	
	public void Add(TopologyComponent comp)
	{
		topology.put(comp.getComponentName(), comp);
		if(comp.getCompType() == Commons.SPOUT) spoutName = comp.getComponentName();
	}
	
	public TopologyComponent Get(String comp)
	{
		return topology.get(comp);
	}
	
}

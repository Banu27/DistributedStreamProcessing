package edu.uiuc.cs425;

import java.util.HashMap;
import java.util.Set;

public class Topology {
	
	String sTopologyName;
	
	//Key --> Component Name
	//Value --> TopologyComponent
	private HashMap<String, TopologyComponent> topology;
	private String spoutName;
	
	
	public HashMap<String, TopologyComponent> GetTopology()
	{
		return topology;
	}
	
	public int	GetParallelismLevel(String componentName)
	{
		return topology.get(componentName).getParallelismLevel();
	}
	
	
	public Set<String> GetKeys ()
	{
		return topology.keySet();
	}
	
	public Topology(String name)
	{
		sTopologyName = name;
		topology = new HashMap<String, TopologyComponent>();
		setSpoutName(null);
	}
	
	public boolean isAvailable(String comp)
	{
		return topology.containsKey(comp);
	}
	
	public void Add(TopologyComponent comp)
	{
		topology.put(comp.getComponentName(), comp);
		if(comp.getCompType() == Commons.SPOUT) setSpoutName(comp.getComponentName());
	}
	
	public TopologyComponent Get(String comp)
	{
		return topology.get(comp);
	}

	public String getSpoutName() {
		return spoutName;
	}

	public void setSpoutName(String spoutName) {
		this.spoutName = spoutName;
	}
	
}

package edu.uiuc.cs425;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Set;

public class Topology {
	
	String sTopologyName;
	
	//Key --> Component Name
	//Value --> TopologyComponent
	private HashMap<String, TopologyComponent> topology;
	private String spoutName;
	
	// we also build a reverse lookup to get the next set of components
	private HashMap<String, ArrayList<String> > nextComponents;
	
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
		nextComponents = new HashMap<String, ArrayList<String>>();
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
		else {
			// get the parent and add the string name to 
			String sParent = comp.getParent().getComponentName();
			if(nextComponents.containsKey(sParent))
			{
				nextComponents.get(sParent).add(comp.getComponentName());
			} else
			{
				ArrayList<String> newList = new ArrayList<String>();
				newList.add(comp.getComponentName());
				nextComponents.put(sParent, newList);
			}
		}
	}
	
	public TopologyComponent Get(String comp)
	{
		return topology.get(comp);
	}
	
	public ArrayList<String> GetNextComponents(String compName)
	{
		return nextComponents.get(compName);
	}

	public String getSpoutName() {
		return spoutName;
	}

	public void setSpoutName(String spoutName) {
		this.spoutName = spoutName;
	}
	
}

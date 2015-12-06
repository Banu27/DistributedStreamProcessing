package edu.uiuc.cs425;

public class ExclamationTopology implements ITopologyDefn{

	public Topology CreateTopology() {
		TopologyBuilder builder = new TopologyBuilder("ExclamationTopology");
		builder.setSpout("ExclamationSpout", "edu.uiuc.cs425.ExclamationSpout", 6);
		builder.setBolt("ExclamationBolt1", "edu.uiuc.cs425.ExclamationBolt","ExclamationSpout",Commons.GROUPING_SHUFFLE,
				null,2);
		builder.setBolt("ExclamationBolt2", "edu.uiuc.cs425.ExclamationBolt","ExclamationBolt1",Commons.GROUPING_SHUFFLE,
				null,2);
		
		return builder.GetTopology();
	}

}



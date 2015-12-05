package edu.uiuc.cs425;

public class ExclamationTopology implements ITopologyDefn{

	public Topology CreateTopology() {
		TopologyBuilder builder = new TopologyBuilder("ExclamationTopology");
		builder.setSpout("ExclamationSpout", "ExclamationSpout", 10);
		builder.setBolt("ExclamationBolt1", "ExclamationBolt","ExclamationSpout",Commons.GROUPING_SHUFFLE,
				null,2);
		builder.setBolt("ExclamationBolt2", "ExclamationBolt","ExclamationBolt1",Commons.GROUPING_SHUFFLE,
				null,2);
		
		return builder.GetTopology();
	}

}



package edu.uiuc.cs425;

public class WordCountTopology implements ITopologyDefn {

	public Topology CreateTopology() {
		TopologyBuilder builder = new TopologyBuilder("WordCountTopology");
		builder.setSpout("SentenceGen", "edu.uiuc.cs425.SentenceGenSpout", 4);
		builder.setBolt("SplitSentence", "edu.uiuc.cs425.SplitSentenceBolt","SentenceGen",Commons.GROUPING_SHUFFLE,
				null,6);
		builder.setBolt("WordCounter", "edu.uiuc.cs425.WordCounterBolt","SplitSentence",Commons.GROUPING_FIELD,
				"word",8);
		
		return builder.GetTopology();
	}

}

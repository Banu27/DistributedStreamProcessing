package edu.uiuc.cs425;

public class WordCountTopology implements ITopologyDefn {

	public Topology CreateTopology() {
		TopologyBuilder builder = new TopologyBuilder("WordCountTopology");
		builder.setSpout("SentenceGen", "SentenceGenSpout", 4);
		builder.setBolt("SplitSentence", "SplitSentenceBolt","SentenceGen",Commons.GROUPING_SHUFFLE,
				null,6);
		builder.setBolt("WordCounter", "WordCounterBolt","SplitSentence",Commons.GROUPING_FIELD,
				"word",8);
		
		return builder.GetTopology();
	}

}

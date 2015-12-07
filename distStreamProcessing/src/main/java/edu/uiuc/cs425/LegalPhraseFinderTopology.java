package edu.uiuc.cs425;

public class LegalPhraseFinderTopology implements ITopologyDefn {

	public Topology CreateTopology() {
		TopologyBuilder builder = new TopologyBuilder("LegalPhraseFinderTopology");
		builder.setSpout("SentenceGen", "edu.uiuc.cs425.GenSentenceFileSpout", 3);
		builder.setBolt("SplitSentence", "edu.uiuc.cs425.SplitSentenceBolt","SentenceGen",Commons.GROUPING_SHUFFLE,
				null,3);
		builder.setBolt("PhraseCounter", "edu.uiuc.cs425.LegalPhraseFinderBolt","SplitSentence",Commons.GROUPING_FIELD,
				"word",3);
		
		return builder.GetTopology();
	}

}
